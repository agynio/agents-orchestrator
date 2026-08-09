package reconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	meteringv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/metering/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeMeteringClient struct {
	record     func(context.Context, *meteringv1.RecordRequest, ...grpc.CallOption) (*meteringv1.RecordResponse, error)
	queryUsage func(context.Context, *meteringv1.QueryUsageRequest, ...grpc.CallOption) (*meteringv1.QueryUsageResponse, error)
}

func (f *fakeMeteringClient) Record(ctx context.Context, req *meteringv1.RecordRequest, opts ...grpc.CallOption) (*meteringv1.RecordResponse, error) {
	if f.record != nil {
		return f.record(ctx, req, opts...)
	}
	return &meteringv1.RecordResponse{}, nil
}

func (f *fakeMeteringClient) QueryUsage(ctx context.Context, req *meteringv1.QueryUsageRequest, opts ...grpc.CallOption) (*meteringv1.QueryUsageResponse, error) {
	if f.queryUsage != nil {
		return f.queryUsage(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func TestSampleMeteringEmitsRecordsAndUpdatesSampledAt(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC)
	workload1Created := now.Add(-2 * time.Minute)
	workload2Created := now.Add(-1 * time.Minute)
	workload2Sampled := now.Add(-30 * time.Second)
	volumeCreated := now.Add(-5 * time.Minute)
	volumeRemoved := now.Add(-1 * time.Minute)

	workload1 := &runnersv1.Workload{
		Meta:                   &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(workload1Created)},
		ThreadId:               "thread-1",
		AgentId:                testAgentID,
		RunnerId:               "runner-1",
		OrganizationId:         testOrganizationID,
		Flavor:                 "cpu-1x",
		AllocatedCpuMillicores: 500,
		AllocatedRamBytes:      2 * (1 << 30),
	}
	// No flavor: an agent still carrying an inline image and resources. It is
	// still marked sampled, it just bills nothing.
	workload2 := &runnersv1.Workload{
		Meta:                   &runnersv1.EntityMeta{Id: "workload-2", CreatedAt: timestamppb.New(workload2Created)},
		ThreadId:               "thread-2",
		AgentId:                testAgentIDAlt,
		RunnerId:               "runner-2",
		OrganizationId:         testOrganizationID,
		AllocatedCpuMillicores: 500,
		AllocatedRamBytes:      2 * (1 << 30),
		LastMeteringSampledAt:  timestamppb.New(workload2Sampled),
	}
	volume := &runnersv1.Volume{
		Meta:           &runnersv1.EntityMeta{Id: "volume-1", CreatedAt: timestamppb.New(volumeCreated)},
		ThreadId:       "thread-1",
		AgentId:        testAgentID,
		RunnerId:       "runner-1",
		OrganizationId: testOrganizationID,
		SizeGb:         "10",
		RemovedAt:      timestamppb.New(volumeRemoved),
	}

	var recorded []*meteringv1.UsageRecord
	recordCalled := false
	metering := &fakeMeteringClient{
		record: func(_ context.Context, req *meteringv1.RecordRequest, _ ...grpc.CallOption) (*meteringv1.RecordResponse, error) {
			recordCalled = true
			recorded = req.GetRecords()
			return &meteringv1.RecordResponse{}, nil
		},
	}

	var workloadUpdateReq *runnersv1.BatchUpdateWorkloadSampledAtRequest
	var volumeUpdateReq *runnersv1.BatchUpdateVolumeSampledAtRequest
	workloadCalls := 0
	volumeCalls := 0

	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, req *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			if !req.GetFilter().GetPendingSample() {
				return nil, errors.New("expected pending sample workload request")
			}
			if req.GetPageToken() == "" {
				return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{workload1}, NextPageToken: "next"}, nil
			}
			if req.GetPageToken() == "next" {
				return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{workload2}}, nil
			}
			return nil, errors.New("unexpected workload page token")
		},
		listVolumes: func(_ context.Context, req *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			if !req.GetFilter().GetPendingSample() {
				return nil, errors.New("expected pending sample volume request")
			}
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{volume}}, nil
		},
		batchUpdateWorkload: func(_ context.Context, req *runnersv1.BatchUpdateWorkloadSampledAtRequest, _ ...grpc.CallOption) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error) {
			workloadCalls++
			if !recordCalled {
				return nil, errors.New("record not called before workload update")
			}
			workloadUpdateReq = req
			return &runnersv1.BatchUpdateWorkloadSampledAtResponse{}, nil
		},
		batchUpdateVolume: func(_ context.Context, req *runnersv1.BatchUpdateVolumeSampledAtRequest, _ ...grpc.CallOption) (*runnersv1.BatchUpdateVolumeSampledAtResponse, error) {
			volumeCalls++
			if !recordCalled {
				return nil, errors.New("record not called before volume update")
			}
			volumeUpdateReq = req
			return &runnersv1.BatchUpdateVolumeSampledAtResponse{}, nil
		},
	}

	reconciler := New(Config{
		Runners:                runners,
		Metering:               metering,
		Agents:                 defaultAgentsClient(),
		MeteringSampleInterval: time.Minute,
	})
	if err := reconciler.sampleMetering(ctx, now); err != nil {
		t.Fatalf("sample metering: %v", err)
	}
	if workloadCalls != 1 || volumeCalls != 1 {
		t.Fatalf("expected update calls once each, got workloads=%d volumes=%d", workloadCalls, volumeCalls)
	}
	// One compute record for the flavored workload and one storage record.
	// The flavorless workload contributes neither.
	if len(recorded) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recorded))
	}

	var flavorRecord *meteringv1.UsageRecord
	var storageRecord *meteringv1.UsageRecord
	for _, record := range recorded {
		switch record.GetUnit() {
		case meteringv1.Unit_UNIT_FLAVOR_SECONDS:
			flavorRecord = record
		case meteringv1.Unit_UNIT_GB_SECONDS:
			if record.GetLabels()[labelKind] == kindStorage {
				storageRecord = record
			}
		case meteringv1.Unit_UNIT_CORE_SECONDS:
			t.Fatalf("compute must no longer emit core seconds")
		}
	}
	if flavorRecord == nil || storageRecord == nil {
		t.Fatalf("expected flavor and storage records")
	}
	if flavorRecord.GetLabels()[labelResourceID] != "workload-1" {
		t.Fatalf("flavor record is for %q, want workload-1", flavorRecord.GetLabels()[labelResourceID])
	}

	// The workload ran for the full 2-minute interval, so it occupied its
	// flavor for 120 seconds regardless of what CPU and RAM it was allocated.
	if flavorRecord.GetValue() != 120000000 {
		t.Fatalf("unexpected flavor value %d", flavorRecord.GetValue())
	}
	if storageRecord.GetValue() != 2400000000 {
		t.Fatalf("unexpected storage value %d", storageRecord.GetValue())
	}

	if flavorRecord.GetTimestamp().AsTime().UTC() != now {
		t.Fatalf("unexpected flavor timestamp %v", flavorRecord.GetTimestamp().AsTime())
	}
	if storageRecord.GetTimestamp().AsTime().UTC() != volumeRemoved {
		t.Fatalf("unexpected storage timestamp %v", storageRecord.GetTimestamp().AsTime())
	}

	if flavorRecord.GetIdempotencyKey() != meteringKey(resourceWorkload, "workload-1", unitFlavorSecondsLabel, "", now) {
		t.Fatalf("unexpected flavor idempotency key %q", flavorRecord.GetIdempotencyKey())
	}
	if storageRecord.GetIdempotencyKey() != meteringKey(resourceVolume, "volume-1", unitGBSecondsLabel, kindStorage, volumeRemoved) {
		t.Fatalf("unexpected storage idempotency key %q", storageRecord.GetIdempotencyKey())
	}

	assertLabelValue(t, flavorRecord.GetLabels(), labelResource, resourceWorkload)
	assertLabelValue(t, flavorRecord.GetLabels(), labelResourceID, "workload-1")
	assertLabelValue(t, flavorRecord.GetLabels(), labelThreadID, "thread-1")
	assertLabelValue(t, flavorRecord.GetLabels(), labelAgentID, testAgentID)
	assertLabelValue(t, flavorRecord.GetLabels(), labelRunnerID, "runner-1")
	assertLabelValue(t, flavorRecord.GetLabels(), labelIdentityID, testAgentID)
	// flavor and runner_id are the pair billing aggregates on.
	assertLabelValue(t, flavorRecord.GetLabels(), labelFlavor, "cpu-1x")
	if _, ok := flavorRecord.GetLabels()[labelKind]; ok {
		t.Fatalf("unexpected kind label on the flavor record")
	}

	assertLabelValue(t, storageRecord.GetLabels(), labelKind, kindStorage)

	if workloadUpdateReq == nil || volumeUpdateReq == nil {
		t.Fatalf("expected update requests")
	}
	if len(workloadUpdateReq.GetEntries()) != 2 {
		t.Fatalf("expected 2 workload updates, got %d", len(workloadUpdateReq.GetEntries()))
	}
	if len(volumeUpdateReq.GetEntries()) != 1 {
		t.Fatalf("expected 1 volume update, got %d", len(volumeUpdateReq.GetEntries()))
	}
	assertSampledAt(t, workloadUpdateReq.GetEntries(), "workload-1", now)
	assertSampledAt(t, workloadUpdateReq.GetEntries(), "workload-2", now)
	assertSampledAt(t, volumeUpdateReq.GetEntries(), "volume-1", volumeRemoved)
}

func assertLabelValue(t *testing.T, labels map[string]string, key, expected string) {
	t.Helper()
	value, ok := labels[key]
	if !ok {
		t.Fatalf("missing label %s", key)
	}
	if value != expected {
		t.Fatalf("expected label %s=%q, got %q", key, expected, value)
	}
}

func assertSampledAt(t *testing.T, entries []*runnersv1.SampledAtEntry, id string, expected time.Time) {
	t.Helper()
	for _, entry := range entries {
		if entry.GetId() == id {
			if entry.GetSampledAt().AsTime().UTC() != expected {
				t.Fatalf("expected sampled_at %v for %s, got %v", expected, id, entry.GetSampledAt().AsTime())
			}
			return
		}
	}
	t.Fatalf("missing sampled_at entry for %s", id)
}

func TestSampleMeteringLabelsSandboxOwner(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC)
	sandboxID := uuid.NewString()
	ownerID := uuid.NewString()

	workload := &runnersv1.Workload{
		Meta:                   &runnersv1.EntityMeta{Id: "workload-sandbox", CreatedAt: timestamppb.New(now.Add(-time.Minute))},
		RunnerId:               "runner-1",
		OrganizationId:         testOrganizationID,
		Flavor:                 "cpu-1x",
		AllocatedCpuMillicores: 500,
		OwnerKind:              runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:                sandboxID,
	}
	volume := &runnersv1.Volume{
		Meta:           &runnersv1.EntityMeta{Id: "volume-sandbox", CreatedAt: timestamppb.New(now.Add(-time.Minute))},
		RunnerId:       "runner-1",
		OrganizationId: testOrganizationID,
		SizeGb:         "10",
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:        sandboxID,
	}

	var recorded []*meteringv1.UsageRecord
	metering := &fakeMeteringClient{record: func(_ context.Context, req *meteringv1.RecordRequest, _ ...grpc.CallOption) (*meteringv1.RecordResponse, error) {
		recorded = req.GetRecords()
		return &meteringv1.RecordResponse{}, nil
	}}
	runners := &fakeRunnersClient{
		listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{workload}}, nil
		},
		listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{volume}}, nil
		},
		batchUpdateWorkload: func(context.Context, *runnersv1.BatchUpdateWorkloadSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error) {
			return &runnersv1.BatchUpdateWorkloadSampledAtResponse{}, nil
		},
	}
	getSandboxCalls := 0
	agents := &testutil.FakeAgentsClient{
		ListAgentsFunc: defaultListAgentsFunc(),
		GetSandboxFunc: func(_ context.Context, req *agentsv1.GetSandboxRequest, _ ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
			getSandboxCalls++
			if req.GetId() != sandboxID {
				return nil, errors.New("unexpected sandbox id")
			}
			return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{
				Meta:           &agentsv1.EntityMeta{Id: sandboxID},
				OrganizationId: testOrganizationID,
				OwnerId:        ownerID,
			}}, nil
		},
	}

	reconciler := New(Config{
		Runners:                runners,
		Metering:               metering,
		Agents:                 agents,
		MeteringSampleInterval: time.Minute,
	})
	if err := reconciler.sampleMetering(ctx, now); err != nil {
		t.Fatalf("sample metering: %v", err)
	}
	if getSandboxCalls != 1 {
		t.Fatalf("expected one sandbox lookup per cycle, got %d", getSandboxCalls)
	}
	if len(recorded) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recorded))
	}
	for _, record := range recorded {
		assertLabelValue(t, record.GetLabels(), labelOwnerKind, "sandbox")
		assertLabelValue(t, record.GetLabels(), labelSandboxID, sandboxID)
		assertLabelValue(t, record.GetLabels(), labelSandboxOwnerID, ownerID)
	}
}

func TestSampleMeteringKeepsSandboxRecordsWhenOwnerUnresolved(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC)
	sandboxID := uuid.NewString()

	workload := &runnersv1.Workload{
		Meta:                   &runnersv1.EntityMeta{Id: "workload-sandbox", CreatedAt: timestamppb.New(now.Add(-time.Minute))},
		RunnerId:               "runner-1",
		OrganizationId:         testOrganizationID,
		Flavor:                 "cpu-1x",
		AllocatedCpuMillicores: 500,
		OwnerKind:              runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:                sandboxID,
	}

	var recorded []*meteringv1.UsageRecord
	metering := &fakeMeteringClient{record: func(_ context.Context, req *meteringv1.RecordRequest, _ ...grpc.CallOption) (*meteringv1.RecordResponse, error) {
		recorded = req.GetRecords()
		return &meteringv1.RecordResponse{}, nil
	}}
	runners := &fakeRunnersClient{
		listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{workload}}, nil
		},
		listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{}, nil
		},
		batchUpdateWorkload: func(context.Context, *runnersv1.BatchUpdateWorkloadSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error) {
			return &runnersv1.BatchUpdateWorkloadSampledAtResponse{}, nil
		},
	}
	agents := &testutil.FakeAgentsClient{
		ListAgentsFunc: defaultListAgentsFunc(),
		GetSandboxFunc: func(context.Context, *agentsv1.GetSandboxRequest, ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
			return nil, errors.New("sandbox gone")
		},
	}

	reconciler := New(Config{
		Runners:                runners,
		Metering:               metering,
		Agents:                 agents,
		MeteringSampleInterval: time.Minute,
	})
	if err := reconciler.sampleMetering(ctx, now); err != nil {
		t.Fatalf("sample metering: %v", err)
	}
	if len(recorded) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recorded))
	}
	if _, ok := recorded[0].GetLabels()[labelSandboxOwnerID]; ok {
		t.Fatal("expected no sandbox owner label when the sandbox cannot be resolved")
	}
	assertLabelValue(t, recorded[0].GetLabels(), labelSandboxID, sandboxID)
}

// identity_id is the class in these records and the instance in the LLM
// Proxy's, so ranking a class next to an instance needs both named outright.
func TestSampleMeteringLabelsInstanceAndEnvironment(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC)
	instanceID := uuid.NewString()
	environmentID := uuid.NewString()

	workload := &runnersv1.Workload{
		Meta:           &runnersv1.EntityMeta{Id: "workload-1", CreatedAt: timestamppb.New(now.Add(-time.Minute))},
		ThreadId:       instanceID,
		AgentId:        testAgentID,
		RunnerId:       "runner-1",
		OrganizationId: testOrganizationID,
		Flavor:         "cpu-1x",
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:        instanceID,
	}

	var recorded []*meteringv1.UsageRecord
	metering := &fakeMeteringClient{record: func(_ context.Context, req *meteringv1.RecordRequest, _ ...grpc.CallOption) (*meteringv1.RecordResponse, error) {
		recorded = req.GetRecords()
		return &meteringv1.RecordResponse{}, nil
	}}
	runners := &fakeRunnersClient{
		listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{workload}}, nil
		},
		listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{}, nil
		},
		batchUpdateWorkload: func(context.Context, *runnersv1.BatchUpdateWorkloadSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error) {
			return &runnersv1.BatchUpdateWorkloadSampledAtResponse{}, nil
		},
	}
	agents := &testutil.FakeAgentsClient{
		ListAgentsFunc: func(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
			return &agentsv1.ListAgentsResponse{Agents: []*agentsv1.Agent{{
				Meta:           &agentsv1.EntityMeta{Id: testAgentID},
				OrganizationId: testOrganizationID,
				EnvironmentId:  environmentID,
			}}}, nil
		},
	}

	reconciler := New(Config{
		Runners:                runners,
		Metering:               metering,
		Agents:                 agents,
		MeteringSampleInterval: time.Minute,
	})
	if err := reconciler.sampleMetering(ctx, now); err != nil {
		t.Fatalf("sample metering: %v", err)
	}
	if len(recorded) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recorded))
	}
	labels := recorded[0].GetLabels()
	assertLabelValue(t, labels, labelAgentID, testAgentID)
	assertLabelValue(t, labels, labelAgentInstanceID, instanceID)
	assertLabelValue(t, labels, labelEnvironmentID, environmentID)
}

// A sandbox has no agent class to read an environment off, so it comes from
// the record the owner is already resolved from.
func TestSampleMeteringLabelsSandboxEnvironment(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC)
	sandboxID := uuid.NewString()
	environmentID := uuid.NewString()

	workload := &runnersv1.Workload{
		Meta:           &runnersv1.EntityMeta{Id: "workload-sandbox", CreatedAt: timestamppb.New(now.Add(-time.Minute))},
		RunnerId:       "runner-1",
		OrganizationId: testOrganizationID,
		Flavor:         "cpu-1x",
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:        sandboxID,
	}

	var recorded []*meteringv1.UsageRecord
	metering := &fakeMeteringClient{record: func(_ context.Context, req *meteringv1.RecordRequest, _ ...grpc.CallOption) (*meteringv1.RecordResponse, error) {
		recorded = req.GetRecords()
		return &meteringv1.RecordResponse{}, nil
	}}
	runners := &fakeRunnersClient{
		listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{workload}}, nil
		},
		listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{}, nil
		},
		batchUpdateWorkload: func(context.Context, *runnersv1.BatchUpdateWorkloadSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error) {
			return &runnersv1.BatchUpdateWorkloadSampledAtResponse{}, nil
		},
	}
	agents := &testutil.FakeAgentsClient{
		ListAgentsFunc: defaultListAgentsFunc(),
		GetSandboxFunc: func(context.Context, *agentsv1.GetSandboxRequest, ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
			return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{
				Meta:           &agentsv1.EntityMeta{Id: sandboxID},
				OrganizationId: testOrganizationID,
				OwnerId:        uuid.NewString(),
				EnvironmentId:  environmentID,
			}}, nil
		},
	}

	reconciler := New(Config{
		Runners:                runners,
		Metering:               metering,
		Agents:                 agents,
		MeteringSampleInterval: time.Minute,
	})
	if err := reconciler.sampleMetering(ctx, now); err != nil {
		t.Fatalf("sample metering: %v", err)
	}
	if len(recorded) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recorded))
	}
	labels := recorded[0].GetLabels()
	assertLabelValue(t, labels, labelEnvironmentID, environmentID)
	if _, ok := labels[labelAgentInstanceID]; ok {
		t.Errorf("a sandbox is not an instance, got agent_instance_id=%q", labels[labelAgentInstanceID])
	}
}
