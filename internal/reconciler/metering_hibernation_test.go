package reconciler

import (
	"testing"
	"time"

	meteringv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/metering/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func sleepingWorkload(now time.Time, lastSampledAt, hibernatedAt time.Time, snapshotBytes int64) *runnersv1.Workload {
	workload := &runnersv1.Workload{
		Meta:           &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now.Add(-time.Hour))},
		ThreadId:       "thread-1",
		AgentId:        testAgentID,
		AgentClassId:   stringPtr(testAgentID),
		RunnerId:       "runner-1",
		OrganizationId: testOrganizationID,
		Flavor:         "cpu-1x",
		Status:         runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED,
		HibernatedAt:   timestamppb.New(hibernatedAt),
	}
	if !lastSampledAt.IsZero() {
		workload.LastMeteringSampledAt = timestamppb.New(lastSampledAt)
	}
	if snapshotBytes > 0 {
		workload.SnapshotSizeBytes = &snapshotBytes
	}
	return workload
}

// The compute interval ends at the pause: a workload sampled last before it
// went to sleep bills its flavor up to hibernated_at and no further, and the
// sample mark moves to the pause so the next tick bills storage from there.
func TestSampleWorkloadMeteringEndsComputeAtHibernation(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	hibernatedAt := now.Add(-2 * time.Minute)
	workload := sleepingWorkload(now, now.Add(-5*time.Minute), hibernatedAt, 2<<30)

	records, update, err := sampleWorkloadMetering(workload, now, labelSources{})
	if err != nil {
		t.Fatalf("sample workload metering: %v", err)
	}
	if len(records) != 1 || records[0].GetUnit() != meteringv1.Unit_UNIT_FLAVOR_SECONDS {
		t.Fatalf("expected one compute record, got %v", records)
	}
	// Three minutes of the flavor, not five.
	if records[0].GetValue() != 180*microUnitValue {
		t.Fatalf("expected 180 flavor-seconds, got %d", records[0].GetValue())
	}
	assertLabelValue(t, records[0].GetLabels(), labelResource, resourceWorkload)
	assertLabelValue(t, records[0].GetLabels(), labelIdentityType, identityTypeAgent)
	if update.GetSampledAt().AsTime().UTC() != hibernatedAt {
		t.Fatalf("expected the sample mark at the pause, got %v", update.GetSampledAt().AsTime())
	}
}

// Once the compute tail is sampled, a sleeping workload bills the storage its
// persisted state occupies and nothing else: no flavor-seconds, a snapshot
// record keyed on the interval start.
func TestSampleWorkloadMeteringBillsSnapshotStorageWhileAsleep(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	hibernatedAt := now.Add(-2 * time.Minute)
	workload := sleepingWorkload(now, hibernatedAt, hibernatedAt, 2<<30)

	records, update, err := sampleWorkloadMetering(workload, now, labelSources{})
	if err != nil {
		t.Fatalf("sample workload metering: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected one storage record, got %v", records)
	}
	record := records[0]
	if record.GetUnit() != meteringv1.Unit_UNIT_GB_SECONDS {
		t.Fatalf("expected gb-seconds, got %s", record.GetUnit())
	}
	// Two gigabytes for two minutes.
	if record.GetValue() != 240*microUnitValue {
		t.Fatalf("expected 240 gb-seconds, got %d", record.GetValue())
	}
	assertLabelValue(t, record.GetLabels(), labelResource, resourceSnapshot)
	assertLabelValue(t, record.GetLabels(), labelResourceID, "workload-1")
	assertLabelValue(t, record.GetLabels(), labelIdentityID, testAgentID)
	assertLabelValue(t, record.GetLabels(), labelIdentityType, identityTypeAgent)
	assertLabelValue(t, record.GetLabels(), labelRunnerID, "runner-1")
	assertLabelValue(t, record.GetLabels(), labelKind, kindStorage)
	if _, ok := record.GetLabels()[labelFlavor]; ok {
		t.Fatal("a sleeping workload occupies no flavor")
	}
	if record.GetIdempotencyKey() != meteringKey(resourceSnapshot, "workload-1", unitGBSecondsLabel, kindStorage, hibernatedAt) {
		t.Fatalf("unexpected idempotency key %q", record.GetIdempotencyKey())
	}
	if update.GetSampledAt().AsTime().UTC() != now {
		t.Fatalf("expected the sample mark at the tick, got %v", update.GetSampledAt().AsTime())
	}
}

// A sleeping workload whose runner reported no size bills nothing, but is
// still marked sampled so the next interval starts here.
func TestSampleWorkloadMeteringAsleepWithoutSnapshotSize(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	hibernatedAt := now.Add(-2 * time.Minute)
	workload := sleepingWorkload(now, hibernatedAt, hibernatedAt, 0)

	records, update, err := sampleWorkloadMetering(workload, now, labelSources{})
	if err != nil {
		t.Fatalf("sample workload metering: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected no record, got %v", records)
	}
	if update == nil || update.GetSampledAt().AsTime().UTC() != now {
		t.Fatalf("expected the sample mark advanced, got %v", update)
	}
}

// A hibernated workload stopped by its TTL leaves a storage tail, not a compute
// one, up to the stop.
func TestSampleWorkloadMeteringStorageTailAfterStop(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	hibernatedAt := now.Add(-3 * time.Minute)
	removedAt := now.Add(-time.Minute)
	workload := sleepingWorkload(now, hibernatedAt, hibernatedAt, 1<<30)
	workload.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
	workload.RemovedAt = timestamppb.New(removedAt)

	records, update, err := sampleWorkloadMetering(workload, now, labelSources{})
	if err != nil {
		t.Fatalf("sample workload metering: %v", err)
	}
	if len(records) != 1 || records[0].GetUnit() != meteringv1.Unit_UNIT_GB_SECONDS {
		t.Fatalf("expected one storage record, got %v", records)
	}
	if records[0].GetValue() != 120*microUnitValue {
		t.Fatalf("expected 120 gb-seconds, got %d", records[0].GetValue())
	}
	if update.GetSampledAt().AsTime().UTC() != removedAt {
		t.Fatalf("expected the sample mark at the stop, got %v", update.GetSampledAt().AsTime())
	}
}

// A sandbox's snapshot is attributed to the sandbox, as its compute is.
func TestSampleWorkloadMeteringLabelsSandboxSnapshot(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	hibernatedAt := now.Add(-time.Minute)
	workload := sleepingWorkload(now, hibernatedAt, hibernatedAt, 1<<30)
	workload.OwnerKind = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX
	workload.OwnerId = "sandbox-1"

	records, _, err := sampleWorkloadMetering(workload, now, labelSources{})
	if err != nil {
		t.Fatalf("sample workload metering: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected one storage record, got %v", records)
	}
	assertLabelValue(t, records[0].GetLabels(), labelResource, resourceSnapshot)
	assertLabelValue(t, records[0].GetLabels(), labelIdentityID, "sandbox-1")
	assertLabelValue(t, records[0].GetLabels(), labelIdentityType, identityTypeSandbox)
}
