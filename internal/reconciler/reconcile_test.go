package reconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestReconcileWorkloadsTransitionsStartingToRunning(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		listWorkloads: func(ctx context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			assertIdentityMetadata(t, ctx, testServiceIdentityID.String(), "")
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
	if updateReq.GetInstanceId() != rawInstanceID {
		t.Fatalf("unexpected instance id: %v", updateReq.GetInstanceId())
	}
}

func TestReconcileWorkloadsStopsOrphan(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID

	stopCalled := false
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
	}

	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: "orphan", InstanceId: instanceID},
			}}, nil
		},
		stopWorkload: func(ctx context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			assertIdentityMetadata(t, ctx, testServiceIdentityID.String(), "")
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			stopCalled = true
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if !stopCalled {
		t.Fatal("expected stop workload")
	}
}

func TestReconcileWorkloadsMarksMissingRunnerOnNoTerminators(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadID := "workload-1"

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return nil, errors.New("service runner-1 has no terminators")
		},
	}
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.GetId() != workloadID {
		t.Fatalf("unexpected workload id: %v", updateReq.GetId())
	}
	if updateReq.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
	if updateReq.GetRemovedAt() == nil {
		t.Fatal("expected removed_at")
	}
}

func TestReconcileWorkloadsMarksMissingRunnerOnNoTerminatorsListError(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadID := "workload-1"

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return nil, errors.New("service runner-1 has no terminators")
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.GetId() != workloadID {
		t.Fatalf("unexpected workload id: %v", updateReq.GetId())
	}
	if updateReq.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
	if updateReq.GetRemovedAt() == nil {
		t.Fatal("expected removed_at")
	}
}

func TestReconcileWorkloadsDegradesUnenrolledRunner(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	threadID := uuid.New().String()
	workloadID := "workload-1"
	secondWorkloadID := "workload-2"

	updateCount := 0
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, ThreadId: threadID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
				{Meta: &runnersv1.EntityMeta{Id: secondWorkloadID}, RunnerId: runnerID, ThreadId: threadID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			orgID := testOrganizationID
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{
				{Meta: &runnersv1.EntityMeta{Id: runnerID}, OrganizationId: &orgID, Status: runnersv1.RunnerStatus_RUNNER_STATUS_OFFLINE},
			}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateCount++
			if req.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
				return nil, errors.New("unexpected workload status")
			}
			if req.GetRemovedAt() == nil {
				return nil, errors.New("missing removed_at")
			}
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	degradeCalls := 0
	threads := &fakeThreadsClient{
		degradeThread: func(_ context.Context, req *threadsv1.DegradeThreadRequest, _ ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error) {
			degradeCalls++
			if req.GetThreadId() != threadID {
				return nil, errors.New("unexpected thread id")
			}
			if req.GetReason() != degradeReasonRunnerDeprovisioned {
				return nil, errors.New("unexpected degrade reason")
			}
			return &threadsv1.DegradeThreadResponse{}, nil
		},
	}

	runnerDialer := &fakeRunnerDialer{
		dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) {
			return nil, errors.New("unexpected dial")
		},
	}
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Threads:      threads,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateCount != 2 {
		t.Fatalf("expected 2 workload updates, got %d", updateCount)
	}
	if degradeCalls != 1 {
		t.Fatalf("expected 1 degrade call, got %d", degradeCalls)
	}
}

func TestReconcileVolumesActivatesProvisioning(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	instanceID := "volume-instance-1"
	threadID := uuid.New().String()
	volumeID := uuid.New().String()

	var updateReq *runnersv1.UpdateVolumeRequest
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				{Meta: &runnersv1.EntityMeta{Id: volumeKey}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, ThreadId: threadID, VolumeId: volumeID},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolume: func(_ context.Context, req *runnersv1.UpdateVolumeRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error) {
			updateReq = req
			return &runnersv1.UpdateVolumeResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		listVolumes: func(ctx context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			assertIdentityMetadata(t, ctx, testServiceIdentityID.String(), "")
			return &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{
				{VolumeKey: volumeKey, InstanceId: instanceID},
			}}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update volume")
	}
	if updateReq.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
	if updateReq.GetInstanceId() != instanceID {
		t.Fatalf("unexpected instance id: %v", updateReq.GetInstanceId())
	}
}

func TestReconcileVolumesMarksMissingRunnerOnNoTerminatorsListError(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	threadID := uuid.New().String()
	volumeID := uuid.New().String()

	var updateReq *runnersv1.UpdateVolumeRequest
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				{Meta: &runnersv1.EntityMeta{Id: volumeKey}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, ThreadId: threadID, VolumeId: volumeID},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolume: func(_ context.Context, req *runnersv1.UpdateVolumeRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error) {
			updateReq = req
			return &runnersv1.UpdateVolumeResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			return nil, errors.New("service runner-1 has no terminators")
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update volume")
	}
	if updateReq.GetId() != volumeKey {
		t.Fatalf("unexpected volume id: %v", updateReq.GetId())
	}
	if updateReq.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_FAILED {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
}

func TestReconcileVolumesDegradesOnMissingPVC(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	threadID := uuid.New().String()
	volumeID := uuid.New().String()

	var updateReq *runnersv1.UpdateVolumeRequest
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				{Meta: &runnersv1.EntityMeta{Id: volumeKey}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, ThreadId: threadID, VolumeId: volumeID},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolume: func(_ context.Context, req *runnersv1.UpdateVolumeRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error) {
			updateReq = req
			return &runnersv1.UpdateVolumeResponse{}, nil
		},
	}

	degradeCalls := 0
	threads := &fakeThreadsClient{
		degradeThread: func(_ context.Context, req *threadsv1.DegradeThreadRequest, _ ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error) {
			degradeCalls++
			if req.GetThreadId() != threadID {
				return nil, errors.New("unexpected thread id")
			}
			if req.GetReason() != degradeReasonVolumeLost {
				return nil, errors.New("unexpected degrade reason")
			}
			return &threadsv1.DegradeThreadResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			return &runnerv1.ListVolumesResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Threads:      threads,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update volume")
	}
	if updateReq.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_FAILED {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
	if degradeCalls != 1 {
		t.Fatalf("expected 1 degrade call, got %d", degradeCalls)
	}
}

func TestReconcileVolumesDegradesUnenrolledRunner(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	threadID := uuid.New().String()
	volumeID := uuid.New().String()

	updateCount := 0
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				{Meta: &runnersv1.EntityMeta{Id: volumeKey}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, ThreadId: threadID, VolumeId: volumeID},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			orgID := testOrganizationID
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{
				{Meta: &runnersv1.EntityMeta{Id: runnerID}, OrganizationId: &orgID, Status: runnersv1.RunnerStatus_RUNNER_STATUS_OFFLINE},
			}}, nil
		},
		updateVolume: func(_ context.Context, req *runnersv1.UpdateVolumeRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error) {
			updateCount++
			if req.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_FAILED {
				return nil, errors.New("unexpected volume status")
			}
			return &runnersv1.UpdateVolumeResponse{}, nil
		},
	}

	degradeCalls := 0
	threads := &fakeThreadsClient{
		degradeThread: func(_ context.Context, req *threadsv1.DegradeThreadRequest, _ ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error) {
			degradeCalls++
			if req.GetThreadId() != threadID {
				return nil, errors.New("unexpected thread id")
			}
			if req.GetReason() != degradeReasonRunnerDeprovisioned {
				return nil, errors.New("unexpected degrade reason")
			}
			return &threadsv1.DegradeThreadResponse{}, nil
		},
	}

	runnerDialer := &fakeRunnerDialer{
		dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) {
			return nil, errors.New("unexpected dial")
		},
	}
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Threads:      threads,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if updateCount != 1 {
		t.Fatalf("expected 1 volume update, got %d", updateCount)
	}
	if degradeCalls != 1 {
		t.Fatalf("expected 1 degrade call, got %d", degradeCalls)
	}
}

func TestReconcileVolumesTTLExpires(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	instanceID := "volume-instance-1"
	threadID := uuid.New().String()
	volumeID := uuid.New().String()

	updateStatuses := []runnersv1.VolumeStatus{}
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				{Meta: &runnersv1.EntityMeta{Id: volumeKey}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, ThreadId: threadID, VolumeId: volumeID},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolume: func(_ context.Context, req *runnersv1.UpdateVolumeRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error) {
			updateStatuses = append(updateStatuses, req.GetStatus())
			return &runnersv1.UpdateVolumeResponse{}, nil
		},
		listWorkloadsByThread: func(_ context.Context, req *runnersv1.ListWorkloadsByThreadRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
			if req.GetThreadId() != threadID {
				return nil, errors.New("unexpected thread id")
			}
			removedAt := timestamppb.New(time.Now().Add(-2 * time.Hour))
			return &runnersv1.ListWorkloadsByThreadResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, RemovedAt: removedAt},
			}}, nil
		},
	}

	removeCalled := false
	runner := &fakeRunnerClient{
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			return &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{
				{VolumeKey: volumeKey, InstanceId: instanceID},
			}}, nil
		},
		removeVolume: func(ctx context.Context, req *runnerv1.RemoveVolumeRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeResponse, error) {
			assertIdentityMetadata(t, ctx, testServiceIdentityID.String(), "")
			if req.GetVolumeName() != instanceID {
				return nil, errors.New("unexpected volume id")
			}
			removeCalled = true
			return &runnerv1.RemoveVolumeResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}

	agents := &testutil.FakeAgentsClient{
		GetVolumeFunc: func(_ context.Context, req *agentsv1.GetVolumeRequest, _ ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
			if req.GetId() != volumeID {
				return nil, errors.New("unexpected volume id")
			}
			ttl := "1h"
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Meta: &agentsv1.EntityMeta{Id: volumeID}, Persistent: true, Ttl: &ttl}}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if len(updateStatuses) == 0 {
		t.Fatal("expected update volume")
	}
	if updateStatuses[len(updateStatuses)-1] != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING {
		t.Fatalf("unexpected update status: %v", updateStatuses)
	}
	if !removeCalled {
		t.Fatal("expected remove volume")
	}
}
