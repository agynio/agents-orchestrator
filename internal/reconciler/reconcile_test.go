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
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
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
	createdAt := timestamppb.New(time.Date(2024, time.January, 1, 1, 2, 3, 0, time.UTC))
	startTime := timestamppb.New(time.Date(2024, time.January, 1, 2, 3, 4, 0, time.UTC))
	finishTime := timestamppb.New(time.Date(2024, time.January, 1, 3, 4, 5, 0, time.UTC))
	reason := "Completed"
	message := "done"
	exitCode := int32(0)

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey, CreatedAt: createdAt}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING},
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

	inspectCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			inspectCalled = true
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.InspectWorkloadResponse{Containers: []*runnerv1.WorkloadContainer{
				{
					ContainerId:  "init-id",
					Name:         "init",
					Role:         runnerv1.ContainerRole_CONTAINER_ROLE_INIT,
					Image:        "init-image",
					Status:       runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED,
					Reason:       &reason,
					Message:      &message,
					ExitCode:     &exitCode,
					RestartCount: 2,
					StartedAt:    startTime,
					FinishedAt:   finishTime,
				},
				{
					ContainerId:  "main-id",
					Name:         "main",
					Role:         runnerv1.ContainerRole_CONTAINER_ROLE_MAIN,
					Image:        "main-image",
					Status:       runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
					RestartCount: 1,
					StartedAt:    startTime,
				},
				{
					ContainerId: "sidecar-id",
					Name:        "sidecar",
					Role:        runnerv1.ContainerRole_CONTAINER_ROLE_SIDECAR,
					Image:       "sidecar-image",
					Status:      runnerv1.ContainerStatus_CONTAINER_STATUS_WAITING,
				},
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
	if !inspectCalled {
		t.Fatal("expected inspect workload")
	}
	if len(updateReq.GetContainers()) != 3 {
		t.Fatalf("expected 3 containers, got %d", len(updateReq.GetContainers()))
	}
	initContainer := updateReq.GetContainers()[0]
	if initContainer.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_INIT {
		t.Fatalf("unexpected init role: %v", initContainer.GetRole())
	}
	if initContainer.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED {
		t.Fatalf("unexpected init status: %v", initContainer.GetStatus())
	}
	if initContainer.GetReason() != reason || initContainer.Reason == nil {
		t.Fatalf("unexpected init reason: %v", initContainer.GetReason())
	}
	if initContainer.GetMessage() != message || initContainer.Message == nil {
		t.Fatalf("unexpected init message: %v", initContainer.GetMessage())
	}
	if initContainer.GetExitCode() != exitCode || initContainer.ExitCode == nil {
		t.Fatalf("unexpected init exit code: %v", initContainer.GetExitCode())
	}
	if initContainer.GetStartedAt().AsTime() != startTime.AsTime() {
		t.Fatalf("unexpected init started_at")
	}
	if initContainer.GetFinishedAt().AsTime() != finishTime.AsTime() {
		t.Fatalf("unexpected init finished_at")
	}
	mainContainer := updateReq.GetContainers()[1]
	if mainContainer.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_MAIN {
		t.Fatalf("unexpected main role: %v", mainContainer.GetRole())
	}
	if mainContainer.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING {
		t.Fatalf("unexpected main status: %v", mainContainer.GetStatus())
	}
	sidecarContainer := updateReq.GetContainers()[2]
	if sidecarContainer.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR {
		t.Fatalf("unexpected sidecar role: %v", sidecarContainer.GetRole())
	}
}

func TestReconcileWorkloadsTransitionsStartingToRunningWithoutContainers(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID
	createdAt := timestamppb.New(time.Date(2024, time.January, 1, 1, 2, 3, 0, time.UTC))

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey, CreatedAt: createdAt}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING},
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

	inspectCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			inspectCalled = true
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.InspectWorkloadResponse{StateRunning: true}, nil
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
	if len(updateReq.GetContainers()) != 0 {
		t.Fatalf("expected no containers, got %d", len(updateReq.GetContainers()))
	}
	if !inspectCalled {
		t.Fatal("expected inspect workload")
	}
}

func TestReconcileWorkloadsRefreshesContainersOnRunning(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID
	startTime := timestamppb.New(time.Date(2024, time.February, 1, 2, 3, 4, 0, time.UTC))

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, InstanceId: stringPtr(rawInstanceID)},
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

	inspectCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			inspectCalled = true
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.InspectWorkloadResponse{Containers: []*runnerv1.WorkloadContainer{
				{
					ContainerId:  "main-id",
					Name:         "main",
					Role:         runnerv1.ContainerRole_CONTAINER_ROLE_MAIN,
					Image:        "main-image",
					Status:       runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
					RestartCount: 3,
					StartedAt:    startTime,
				},
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
	if updateReq.InstanceId != nil {
		t.Fatalf("unexpected instance id update: %v", updateReq.GetInstanceId())
	}
	if updateReq.Status != nil {
		t.Fatalf("unexpected status update: %v", updateReq.GetStatus())
	}
	if !inspectCalled {
		t.Fatal("expected inspect workload")
	}
	if len(updateReq.GetContainers()) != 1 {
		t.Fatalf("expected 1 container, got %d", len(updateReq.GetContainers()))
	}
	mainContainer := updateReq.GetContainers()[0]
	if mainContainer.GetContainerId() != "main-id" {
		t.Fatalf("unexpected main container id: %s", mainContainer.GetContainerId())
	}
	if mainContainer.GetRestartCount() != 3 {
		t.Fatalf("unexpected main restart count: %v", mainContainer.GetRestartCount())
	}
	if mainContainer.GetStartedAt().AsTime() != startTime.AsTime() {
		t.Fatalf("unexpected main started_at")
	}
}

func TestReconcileWorkloadsFailsCrashloop(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID
	zitiID := "ziti-identity"
	message := "crashloop"

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, InstanceId: stringPtr(rawInstanceID), ZitiIdentityId: zitiID},
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

	stopCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.InspectWorkloadResponse{Containers: []*runnerv1.WorkloadContainer{
				{
					ContainerId:  "main-id",
					Name:         "main",
					Role:         runnerv1.ContainerRole_CONTAINER_ROLE_MAIN,
					Image:        "main-image",
					Status:       runnerv1.ContainerStatus_CONTAINER_STATUS_WAITING,
					Reason:       stringPtr(crashLoopBackoffFlag),
					Message:      stringPtr(message),
					RestartCount: crashloopThreshold,
				},
			}}, nil
		},
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			stopCalled = true
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	deleteCalled := false
	zitiMgmt := &fakeZitiMgmtClient{
		deleteIdentity: func(_ context.Context, req *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			if req.GetZitiIdentityId() != zitiID {
				return nil, errors.New("unexpected ziti identity id")
			}
			deleteCalled = true
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
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
		ZitiMgmt:     zitiMgmt,
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
	if updateReq.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP {
		t.Fatalf("unexpected failure reason: %v", updateReq.GetFailureReason())
	}
	if updateReq.GetFailureMessage() != message {
		t.Fatalf("unexpected failure message: %s", updateReq.GetFailureMessage())
	}
	if updateReq.GetInstanceId() != rawInstanceID {
		t.Fatalf("unexpected instance id: %s", updateReq.GetInstanceId())
	}
	if !stopCalled {
		t.Fatal("expected stop workload")
	}
	if !deleteCalled {
		t.Fatal("expected delete identity")
	}
}

func TestReconcileWorkloadsDoesNotPromoteStartingOnInspectError(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID
	createdAt := timestamppb.New(time.Date(2024, time.January, 1, 1, 2, 3, 0, time.UTC))

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey, CreatedAt: createdAt}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING},
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

	inspectCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			inspectCalled = true
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return nil, errors.New("inspect failed")
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
	if updateReq.Status != nil {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
	if updateReq.GetInstanceId() != rawInstanceID {
		t.Fatalf("unexpected instance id: %v", updateReq.GetInstanceId())
	}
	if len(updateReq.GetContainers()) != 0 {
		t.Fatalf("expected no containers, got %d", len(updateReq.GetContainers()))
	}
	if !inspectCalled {
		t.Fatal("expected inspect workload")
	}
}

func TestReconcileWorkloadsStopsStoppingOnInspectError(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING},
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

	inspectCalled := false
	stopCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			inspectCalled = true
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return nil, errors.New("inspect failed")
		},
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
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
	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.GetInstanceId() != rawInstanceID {
		t.Fatalf("unexpected instance id: %v", updateReq.GetInstanceId())
	}
	if updateReq.Status != nil {
		t.Fatalf("unexpected status update: %v", updateReq.GetStatus())
	}
	if !inspectCalled {
		t.Fatal("expected inspect workload")
	}
	if !stopCalled {
		t.Fatal("expected stop workload")
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
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
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

func TestReconcileWorkloadsMarksMissingRunnerOnMissingWorkload(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadID := "workload-1"
	instanceID := uuid.New().String()

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, AgentId: testAgentID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, InstanceId: stringPtr(instanceID)},
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
			return &runnerv1.ListWorkloadsResponse{}, nil
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
	if updateReq.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST {
		t.Fatalf("unexpected failure reason: %v", updateReq.GetFailureReason())
	}
	if updateReq.GetFailureMessage() != "workload missing on runner" {
		t.Fatalf("unexpected failure message: %s", updateReq.GetFailureMessage())
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
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
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
		removeVolume: func(_ context.Context, req *runnerv1.RemoveVolumeRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeResponse, error) {
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
