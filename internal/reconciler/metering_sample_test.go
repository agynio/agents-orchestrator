package reconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	meteringv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/metering/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
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
		AllocatedCpuMillicores: 500,
		AllocatedRamBytes:      2 * (1 << 30),
	}
	workload2 := &runnersv1.Workload{
		Meta:                   &runnersv1.EntityMeta{Id: "workload-2", CreatedAt: timestamppb.New(workload2Created)},
		ThreadId:               "thread-2",
		AgentId:                testAgentIDAlt,
		RunnerId:               "runner-2",
		OrganizationId:         testOrganizationID,
		AllocatedCpuMillicores: 0,
		AllocatedRamBytes:      0,
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
			if !req.GetPendingSample() {
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
			if !req.GetPendingSample() {
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
		ClusterAdminIdentityID: testClusterAdminIdentityID,
	})
	if err := reconciler.sampleMetering(ctx, now); err != nil {
		t.Fatalf("sample metering: %v", err)
	}
	if workloadCalls != 1 || volumeCalls != 1 {
		t.Fatalf("expected update calls once each, got workloads=%d volumes=%d", workloadCalls, volumeCalls)
	}
	if len(recorded) != 3 {
		t.Fatalf("expected 3 records, got %d", len(recorded))
	}

	var cpuRecord *meteringv1.UsageRecord
	var ramRecord *meteringv1.UsageRecord
	var storageRecord *meteringv1.UsageRecord
	for _, record := range recorded {
		switch record.GetUnit() {
		case meteringv1.Unit_UNIT_CORE_SECONDS:
			cpuRecord = record
		case meteringv1.Unit_UNIT_GB_SECONDS:
			if record.GetLabels()[labelKind] == kindRAM {
				ramRecord = record
			} else if record.GetLabels()[labelKind] == kindStorage {
				storageRecord = record
			}
		}
	}
	if cpuRecord == nil || ramRecord == nil || storageRecord == nil {
		t.Fatalf("expected cpu, ram, and storage records")
	}

	if cpuRecord.GetValue() != 60000000 {
		t.Fatalf("unexpected cpu value %d", cpuRecord.GetValue())
	}
	if ramRecord.GetValue() != 240000000 {
		t.Fatalf("unexpected ram value %d", ramRecord.GetValue())
	}
	if storageRecord.GetValue() != 2400000000 {
		t.Fatalf("unexpected storage value %d", storageRecord.GetValue())
	}

	if cpuRecord.GetTimestamp().AsTime().UTC() != now {
		t.Fatalf("unexpected cpu timestamp %v", cpuRecord.GetTimestamp().AsTime())
	}
	if ramRecord.GetTimestamp().AsTime().UTC() != now {
		t.Fatalf("unexpected ram timestamp %v", ramRecord.GetTimestamp().AsTime())
	}
	if storageRecord.GetTimestamp().AsTime().UTC() != volumeRemoved {
		t.Fatalf("unexpected storage timestamp %v", storageRecord.GetTimestamp().AsTime())
	}

	if cpuRecord.GetIdempotencyKey() != meteringKey(resourceWorkload, "workload-1", unitCoreSecondsLabel, "", now) {
		t.Fatalf("unexpected cpu idempotency key %q", cpuRecord.GetIdempotencyKey())
	}
	if ramRecord.GetIdempotencyKey() != meteringKey(resourceWorkload, "workload-1", unitGBSecondsLabel, kindRAM, now) {
		t.Fatalf("unexpected ram idempotency key %q", ramRecord.GetIdempotencyKey())
	}
	if storageRecord.GetIdempotencyKey() != meteringKey(resourceVolume, "volume-1", unitGBSecondsLabel, kindStorage, volumeRemoved) {
		t.Fatalf("unexpected storage idempotency key %q", storageRecord.GetIdempotencyKey())
	}

	assertLabelValue(t, cpuRecord.GetLabels(), labelResource, resourceWorkload)
	assertLabelValue(t, cpuRecord.GetLabels(), labelResourceID, "workload-1")
	assertLabelValue(t, cpuRecord.GetLabels(), labelThreadID, "thread-1")
	assertLabelValue(t, cpuRecord.GetLabels(), labelAgentID, testAgentID)
	assertLabelValue(t, cpuRecord.GetLabels(), labelRunnerID, "runner-1")
	assertLabelValue(t, cpuRecord.GetLabels(), labelIdentityID, testAgentID)
	if _, ok := cpuRecord.GetLabels()[labelKind]; ok {
		t.Fatalf("unexpected cpu kind label")
	}

	assertLabelValue(t, ramRecord.GetLabels(), labelKind, kindRAM)
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
