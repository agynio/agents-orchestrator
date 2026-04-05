package reconciler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	identityv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/identity/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testOrganizationID = "org-1"

var errNotImplemented = errors.New("not implemented")

func TestStartWorkloadCreatesIdentityAndStores(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	zitiID := "ziti-identity"
	jwt := "enrollment-jwt"
	workloadID := "workload-1"
	runnerID := "runner-1"
	mainContainerID := "container-main"
	testAssembler := newTestAssembler(agentID, true)

	var calls []string
	zitiMgmt := &fakeZitiMgmtClient{
		createAgentIdentity: func(_ context.Context, req *zitimgmtv1.CreateAgentIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
			calls = append(calls, "create")
			if req.GetAgentId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &zitimgmtv1.CreateAgentIdentityResponse{ZitiIdentityId: zitiID, EnrollmentJwt: jwt}, nil
		},
	}

	runner := &fakeRunnerClient{
		startWorkload: func(_ context.Context, req *runnerv1.StartWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error) {
			calls = append(calls, "start")
			if req.GetMain() == nil {
				return nil, errors.New("missing main container")
			}
			zitiContainer := testutil.FindInitContainer(req.GetInitContainers(), assembler.ZitiSidecarInitContainerName)
			if zitiContainer == nil {
				return nil, errors.New("missing ziti sidecar init container")
			}
			envs := envMap(zitiContainer.GetEnv())
			if envs["ZITI_ENROLL_TOKEN"] != jwt {
				return nil, errors.New("missing ZITI_ENROLL_TOKEN")
			}
			return &runnerv1.StartWorkloadResponse{
				Id:     workloadID,
				Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
				Containers: &runnerv1.WorkloadContainers{
					Main: mainContainerID,
				},
			}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			calls = append(calls, "dial")
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}

	runners := &fakeRunnersClient{
		createWorkload: func(_ context.Context, req *runnersv1.CreateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
			calls = append(calls, "create-workload")
			if req.GetId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			if req.GetRunnerId() != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			if req.GetAgentId() != agentID.String() || req.GetThreadId() != threadID.String() {
				return nil, errors.New("unexpected identifiers")
			}
			if req.GetOrganizationId() != testOrganizationID {
				return nil, errors.New("unexpected organization id")
			}
			if req.GetZitiIdentityId() != zitiID {
				return nil, errors.New("unexpected ziti identity id")
			}
			if req.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
				return nil, errors.New("unexpected workload status")
			}
			if len(req.GetContainers()) != 1 {
				return nil, errors.New("unexpected containers")
			}
			container := req.GetContainers()[0]
			expectedName := fmt.Sprintf("agent-%s-%s", agentID.String()[:8], threadID.String()[:8])
			if container.GetContainerId() != mainContainerID || container.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_MAIN {
				return nil, errors.New("unexpected main container")
			}
			if container.GetName() != expectedName {
				return nil, errors.New("unexpected main container name")
			}
			if container.GetImage() != "agent-image" {
				return nil, errors.New("unexpected main container image")
			}
			return &runnersv1.CreateWorkloadResponse{}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		ZitiMgmt:     zitiMgmt,
		Assembler:    testAssembler,
	})
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID})

	if !reflect.DeepEqual(calls, []string{"dial", "create", "start", "create-workload"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStartWorkloadSkipsIdentityWhenZitiMgmtNil(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	workloadID := "workload-1"
	runnerID := "runner-1"
	mainContainerID := "container-main"
	testAssembler := newTestAssembler(agentID, false)

	var calls []string
	runner := &fakeRunnerClient{
		startWorkload: func(_ context.Context, req *runnerv1.StartWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error) {
			calls = append(calls, "start")
			if req.GetMain() == nil {
				return nil, errors.New("missing main container")
			}
			zitiContainer := testutil.FindInitContainer(req.GetInitContainers(), assembler.ZitiSidecarInitContainerName)
			if zitiContainer != nil {
				envs := envMap(zitiContainer.GetEnv())
				if _, ok := envs["ZITI_ENROLL_TOKEN"]; ok {
					return nil, errors.New("unexpected ZITI_ENROLL_TOKEN")
				}
			}
			return &runnerv1.StartWorkloadResponse{
				Id:     workloadID,
				Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
				Containers: &runnerv1.WorkloadContainers{
					Main: mainContainerID,
				},
			}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			calls = append(calls, "dial")
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}

	runners := &fakeRunnersClient{
		createWorkload: func(_ context.Context, req *runnersv1.CreateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
			calls = append(calls, "create-workload")
			if req.GetId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			if req.GetRunnerId() != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			if req.GetAgentId() != agentID.String() || req.GetThreadId() != threadID.String() {
				return nil, errors.New("unexpected identifiers")
			}
			if req.GetZitiIdentityId() != "" {
				return nil, errors.New("unexpected ziti identity id")
			}
			if len(req.GetContainers()) != 1 || req.GetContainers()[0].GetContainerId() != mainContainerID {
				return nil, errors.New("unexpected containers")
			}
			return &runnersv1.CreateWorkloadResponse{}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID})

	if !reflect.DeepEqual(calls, []string{"dial", "start", "create-workload"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStartWorkloadDeletesIdentityOnRunnerError(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	zitiID := "ziti-identity"
	runnerID := "runner-1"
	testAssembler := newTestAssembler(agentID, true)

	var calls []string
	zitiMgmt := &fakeZitiMgmtClient{
		createAgentIdentity: func(_ context.Context, _ *zitimgmtv1.CreateAgentIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
			calls = append(calls, "create")
			return &zitimgmtv1.CreateAgentIdentityResponse{ZitiIdentityId: zitiID, EnrollmentJwt: "jwt"}, nil
		},
		deleteIdentity: func(_ context.Context, req *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			calls = append(calls, "delete")
			if req.GetZitiIdentityId() != zitiID {
				return nil, errors.New("unexpected ziti identity id")
			}
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		startWorkload: func(_ context.Context, _ *runnerv1.StartWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error) {
			calls = append(calls, "start")
			return nil, errors.New("runner error")
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			calls = append(calls, "dial")
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}

	runners := &fakeRunnersClient{
		createWorkload: func(context.Context, *runnersv1.CreateWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
			calls = append(calls, "create-workload")
			return nil, errors.New("unexpected create workload")
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		ZitiMgmt:     zitiMgmt,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID})

	if !reflect.DeepEqual(calls, []string{"dial", "create", "start", "delete"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStartWorkloadStopsAndDeletesIdentityOnStoreFailure(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	zitiID := "ziti-identity"
	workloadID := "workload-1"
	runnerID := "runner-1"
	mainContainerID := "container-main"
	testAssembler := newTestAssembler(agentID, true)

	var calls []string
	zitiMgmt := &fakeZitiMgmtClient{
		createAgentIdentity: func(_ context.Context, _ *zitimgmtv1.CreateAgentIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
			calls = append(calls, "create")
			return &zitimgmtv1.CreateAgentIdentityResponse{ZitiIdentityId: zitiID, EnrollmentJwt: "jwt"}, nil
		},
		deleteIdentity: func(_ context.Context, req *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			calls = append(calls, "delete")
			if req.GetZitiIdentityId() != zitiID {
				return nil, errors.New("unexpected ziti identity id")
			}
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		startWorkload: func(_ context.Context, _ *runnerv1.StartWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error) {
			calls = append(calls, "start")
			return &runnerv1.StartWorkloadResponse{
				Id:     workloadID,
				Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
				Containers: &runnerv1.WorkloadContainers{
					Main: mainContainerID,
				},
			}, nil
		},
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			if req.GetWorkloadId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			calls = append(calls, "dial")
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}

	runners := &fakeRunnersClient{
		createWorkload: func(_ context.Context, _ *runnersv1.CreateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
			calls = append(calls, "create-workload")
			return nil, errors.New("create error")
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		ZitiMgmt:     zitiMgmt,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID})

	if !reflect.DeepEqual(calls, []string{"dial", "create", "start", "create-workload", "stop", "delete"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStopWorkloadDeletesIdentityAfterStop(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"
	zitiID := "ziti-identity"

	var calls []string
	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, _ *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			calls = append(calls, "dial")
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}

	zitiMgmt := &fakeZitiMgmtClient{
		deleteIdentity: func(_ context.Context, _ *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			calls = append(calls, "delete")
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}

	runners := &fakeRunnersClient{
		deleteWorkload: func(_ context.Context, _ *runnersv1.DeleteWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error) {
			calls = append(calls, "delete-workload")
			return &runnersv1.DeleteWorkloadResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		ZitiMgmt:     zitiMgmt,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, ZitiIdentityId: zitiID})

	if !reflect.DeepEqual(calls, []string{"dial", "stop", "delete", "delete-workload"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStopWorkloadSkipsIdentityWhenNil(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"

	deleteCalled := false
	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, _ *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
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

	zitiMgmt := &fakeZitiMgmtClient{
		deleteIdentity: func(_ context.Context, _ *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			deleteCalled = true
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}

	runners := &fakeRunnersClient{
		deleteWorkload: func(_ context.Context, _ *runnersv1.DeleteWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error) {
			return &runnersv1.DeleteWorkloadResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		ZitiMgmt:     zitiMgmt,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID})

	if deleteCalled {
		t.Fatal("expected no delete identity call")
	}
}

func TestStopWorkloadSkipsIdentityWhenZitiMgmtNil(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, false)
	zitiID := "ziti-identity"
	runnerID := "runner-1"

	var calls []string
	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, _ *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			calls = append(calls, "dial")
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}

	runners := &fakeRunnersClient{
		deleteWorkload: func(_ context.Context, _ *runnersv1.DeleteWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error) {
			calls = append(calls, "delete-workload")
			return &runnersv1.DeleteWorkloadResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, ZitiIdentityId: zitiID})

	if !reflect.DeepEqual(calls, []string{"dial", "stop", "delete-workload"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestReconcileOrphanIdentitiesDeletesOrphans(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	activeID := "active-id"
	orphanID := "orphan-id"

	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, req *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			if len(req.GetStatuses()) == 0 {
				return nil, errors.New("missing statuses")
			}
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, ZitiIdentityId: activeID},
			}}, nil
		},
	}

	deleteCalls := []string{}
	zitiMgmt := &fakeZitiMgmtClient{
		listManagedIdentities: func(_ context.Context, req *zitimgmtv1.ListManagedIdentitiesRequest, _ ...grpc.CallOption) (*zitimgmtv1.ListManagedIdentitiesResponse, error) {
			if req.GetIdentityType() != identityv1.IdentityType_IDENTITY_TYPE_AGENT {
				return nil, errors.New("unexpected identity type")
			}
			return &zitimgmtv1.ListManagedIdentitiesResponse{
				Identities: []*zitimgmtv1.ManagedIdentity{
					{ZitiIdentityId: activeID},
					{ZitiIdentityId: orphanID},
				},
			}, nil
		},
		deleteIdentity: func(_ context.Context, req *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			deleteCalls = append(deleteCalls, req.GetZitiIdentityId())
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		ZitiMgmt:     zitiMgmt,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	if err := reconciler.reconcileOrphanIdentities(ctx); err != nil {
		t.Fatalf("reconcile orphan identities: %v", err)
	}

	if !reflect.DeepEqual(deleteCalls, []string{orphanID}) {
		t.Fatalf("unexpected delete calls: %v", deleteCalls)
	}
}

func TestFetchActualDeletesIdentityForStaleWorkload(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	zitiID := "ziti-id"
	runnerID := "runner-1"

	deleteCalled := false
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, ZitiIdentityId: zitiID},
			}}, nil
		},
		deleteWorkload: func(_ context.Context, req *runnersv1.DeleteWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error) {
			if req.GetId() != "workload-1" {
				return nil, errors.New("unexpected workload id")
			}
			deleteCalled = true
			return &runnersv1.DeleteWorkloadResponse{}, nil
		},
	}
	zitiMgmt := &fakeZitiMgmtClient{
		deleteIdentity: func(_ context.Context, req *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			if req.GetZitiIdentityId() != zitiID {
				return nil, errors.New("unexpected ziti identity id")
			}
			deleteCalled = true
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		findWorkloadsByLabels: func(_ context.Context, _ *runnerv1.FindWorkloadsByLabelsRequest, _ ...grpc.CallOption) (*runnerv1.FindWorkloadsByLabelsResponse, error) {
			return &runnerv1.FindWorkloadsByLabelsResponse{TargetIds: []string{}}, nil
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

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		ZitiMgmt:     zitiMgmt,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	if _, err := reconciler.fetchActual(ctx); err != nil {
		t.Fatalf("fetch actual: %v", err)
	}
	if !deleteCalled {
		t.Fatal("expected delete identity call")
	}
}

func TestFetchActualSkipsIdentityCleanupWhenZitiMgmtNil(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, false)
	zitiID := "ziti-id"
	runnerID := "runner-1"

	deleted := false
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, ZitiIdentityId: zitiID},
			}}, nil
		},
		deleteWorkload: func(_ context.Context, _ *runnersv1.DeleteWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error) {
			deleted = true
			return &runnersv1.DeleteWorkloadResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		findWorkloadsByLabels: func(_ context.Context, _ *runnerv1.FindWorkloadsByLabelsRequest, _ ...grpc.CallOption) (*runnerv1.FindWorkloadsByLabelsResponse, error) {
			return &runnerv1.FindWorkloadsByLabelsResponse{TargetIds: []string{}}, nil
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

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	if _, err := reconciler.fetchActual(ctx); err != nil {
		t.Fatalf("fetch actual: %v", err)
	}
	if !deleted {
		t.Fatal("expected stale workload delete")
	}
}

func TestFetchActualKeepsTerminalWorkloadWhenMissingOnRunner(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, false)
	runnerID := "runner-1"
	workloadID := "workload-1"

	deleted := false
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED},
			}}, nil
		},
		deleteWorkload: func(_ context.Context, req *runnersv1.DeleteWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error) {
			deleted = true
			return &runnersv1.DeleteWorkloadResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		findWorkloadsByLabels: func(_ context.Context, _ *runnerv1.FindWorkloadsByLabelsRequest, _ ...grpc.CallOption) (*runnerv1.FindWorkloadsByLabelsResponse, error) {
			return &runnerv1.FindWorkloadsByLabelsResponse{TargetIds: []string{}}, nil
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

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	actual, err := reconciler.fetchActual(ctx)
	if err != nil {
		t.Fatalf("fetch actual: %v", err)
	}
	if deleted {
		t.Fatal("expected terminal workload to be retained")
	}
	if len(actual) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(actual))
	}
	if actual[0].GetMeta().GetId() != workloadID {
		t.Fatalf("unexpected workload id: %s", actual[0].GetMeta().GetId())
	}
}

func TestFetchActualRemovesRunningWorkloadWhenMissingOnRunner(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, false)
	runnerID := "runner-1"
	workloadID := "workload-1"

	deleted := []string{}
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
			}}, nil
		},
		deleteWorkload: func(_ context.Context, req *runnersv1.DeleteWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error) {
			deleted = append(deleted, req.GetId())
			return &runnersv1.DeleteWorkloadResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		findWorkloadsByLabels: func(_ context.Context, _ *runnerv1.FindWorkloadsByLabelsRequest, _ ...grpc.CallOption) (*runnerv1.FindWorkloadsByLabelsResponse, error) {
			return &runnerv1.FindWorkloadsByLabelsResponse{TargetIds: []string{}}, nil
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

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	actual, err := reconciler.fetchActual(ctx)
	if err != nil {
		t.Fatalf("fetch actual: %v", err)
	}
	if len(actual) != 0 {
		t.Fatalf("expected 0 workloads, got %d", len(actual))
	}
	if !reflect.DeepEqual(deleted, []string{workloadID}) {
		t.Fatalf("unexpected delete ids: %v", deleted)
	}
}

func newTestReconciler(cfg Config) *Reconciler {
	if cfg.Poll == 0 {
		cfg.Poll = time.Second
	}
	if cfg.Idle == 0 {
		cfg.Idle = time.Minute
	}
	if cfg.StopSec == 0 {
		cfg.StopSec = 30
	}
	if cfg.RunnerDialer == nil {
		cfg.RunnerDialer = &fakeRunnerDialer{}
	}
	return New(cfg)
}

func newTestAssembler(agentID uuid.UUID, zitiEnabled bool) *assembler.Assembler {
	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: testOrganizationID, Image: "agent-image"}}, nil
		},
		ListSkillsFunc: func(context.Context, *agentsv1.ListSkillsRequest, ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(context.Context, *agentsv1.ListInitScriptsRequest, ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(context.Context, *agentsv1.ListVolumeAttachmentsRequest, ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(context.Context, *agentsv1.ListImagePullSecretAttachmentsRequest, ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListMcpsFunc: func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		ListHooksFunc: func(context.Context, *agentsv1.ListHooksRequest, ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	cfg := &config.Config{
		DefaultInitImage:    "default-init-image",
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
		ZitiEnabled:         zitiEnabled,
		ZitiSidecarImage:    "ziti-sidecar-image",
		ClusterDNS:          "10.43.0.10",
	}
	return assembler.New(agentsClient, &testutil.FakeSecretsClient{}, cfg)
}

func envMap(envs []*runnerv1.EnvVar) map[string]string {
	result := make(map[string]string, len(envs))
	for _, env := range envs {
		result[env.GetName()] = env.GetValue()
	}
	return result
}

func buildRunner(id string) *runnersv1.Runner {
	orgID := testOrganizationID
	return &runnersv1.Runner{
		Meta:           &runnersv1.EntityMeta{Id: id},
		OrganizationId: &orgID,
		Status:         runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED,
	}
}

type fakeRunnerDialer struct {
	dial func(context.Context, string) (runnerv1.RunnerServiceClient, error)
}

func (f *fakeRunnerDialer) Dial(ctx context.Context, runnerID string) (runnerv1.RunnerServiceClient, error) {
	if f.dial != nil {
		return f.dial(ctx, runnerID)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerDialer) Close() {}

type fakeRunnersClient struct {
	createWorkload       func(context.Context, *runnersv1.CreateWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error)
	deleteWorkload       func(context.Context, *runnersv1.DeleteWorkloadRequest, ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error)
	listWorkloads        func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error)
	listRunners          func(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error)
	updateWorkloadStatus func(context.Context, *runnersv1.UpdateWorkloadStatusRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadStatusResponse, error)
}

func (f *fakeRunnersClient) RegisterRunner(context.Context, *runnersv1.RegisterRunnerRequest, ...grpc.CallOption) (*runnersv1.RegisterRunnerResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) GetRunner(context.Context, *runnersv1.GetRunnerRequest, ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListRunners(ctx context.Context, req *runnersv1.ListRunnersRequest, opts ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
	if f.listRunners != nil {
		return f.listRunners(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) UpdateRunner(context.Context, *runnersv1.UpdateRunnerRequest, ...grpc.CallOption) (*runnersv1.UpdateRunnerResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) EnrollRunner(ctx context.Context, in *runnersv1.EnrollRunnerRequest, opts ...grpc.CallOption) (*runnersv1.EnrollRunnerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f *fakeRunnersClient) DeleteRunner(context.Context, *runnersv1.DeleteRunnerRequest, ...grpc.CallOption) (*runnersv1.DeleteRunnerResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ValidateServiceToken(context.Context, *runnersv1.ValidateServiceTokenRequest, ...grpc.CallOption) (*runnersv1.ValidateServiceTokenResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) CreateWorkload(ctx context.Context, req *runnersv1.CreateWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
	if f.createWorkload != nil {
		return f.createWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) UpdateWorkloadStatus(ctx context.Context, req *runnersv1.UpdateWorkloadStatusRequest, opts ...grpc.CallOption) (*runnersv1.UpdateWorkloadStatusResponse, error) {
	if f.updateWorkloadStatus != nil {
		return f.updateWorkloadStatus(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) DeleteWorkload(ctx context.Context, req *runnersv1.DeleteWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error) {
	if f.deleteWorkload != nil {
		return f.deleteWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) GetWorkload(context.Context, *runnersv1.GetWorkloadRequest, ...grpc.CallOption) (*runnersv1.GetWorkloadResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListWorkloadsByThread(context.Context, *runnersv1.ListWorkloadsByThreadRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListWorkloads(ctx context.Context, req *runnersv1.ListWorkloadsRequest, opts ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
	if f.listWorkloads != nil {
		return f.listWorkloads(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

type fakeRunnerClient struct {
	startWorkload         func(context.Context, *runnerv1.StartWorkloadRequest, ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error)
	stopWorkload          func(context.Context, *runnerv1.StopWorkloadRequest, ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error)
	inspectWorkload       func(context.Context, *runnerv1.InspectWorkloadRequest, ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error)
	findWorkloadsByLabels func(context.Context, *runnerv1.FindWorkloadsByLabelsRequest, ...grpc.CallOption) (*runnerv1.FindWorkloadsByLabelsResponse, error)
}

func (f *fakeRunnerClient) Ready(context.Context, *runnerv1.ReadyRequest, ...grpc.CallOption) (*runnerv1.ReadyResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) StartWorkload(ctx context.Context, req *runnerv1.StartWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error) {
	if f.startWorkload != nil {
		return f.startWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) StopWorkload(ctx context.Context, req *runnerv1.StopWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
	if f.stopWorkload != nil {
		return f.stopWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) RemoveWorkload(context.Context, *runnerv1.RemoveWorkloadRequest, ...grpc.CallOption) (*runnerv1.RemoveWorkloadResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) InspectWorkload(ctx context.Context, req *runnerv1.InspectWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
	if f.inspectWorkload != nil {
		return f.inspectWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) GetWorkloadLabels(context.Context, *runnerv1.GetWorkloadLabelsRequest, ...grpc.CallOption) (*runnerv1.GetWorkloadLabelsResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) FindWorkloadsByLabels(ctx context.Context, req *runnerv1.FindWorkloadsByLabelsRequest, opts ...grpc.CallOption) (*runnerv1.FindWorkloadsByLabelsResponse, error) {
	if f.findWorkloadsByLabels != nil {
		return f.findWorkloadsByLabels(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) ListWorkloadsByVolume(context.Context, *runnerv1.ListWorkloadsByVolumeRequest, ...grpc.CallOption) (*runnerv1.ListWorkloadsByVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) RemoveVolume(context.Context, *runnerv1.RemoveVolumeRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) TouchWorkload(context.Context, *runnerv1.TouchWorkloadRequest, ...grpc.CallOption) (*runnerv1.TouchWorkloadResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) PutArchive(context.Context, *runnerv1.PutArchiveRequest, ...grpc.CallOption) (*runnerv1.PutArchiveResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) StreamWorkloadLogs(context.Context, *runnerv1.StreamWorkloadLogsRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[runnerv1.StreamWorkloadLogsResponse], error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) StreamEvents(context.Context, *runnerv1.StreamEventsRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[runnerv1.StreamEventsResponse], error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) Exec(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[runnerv1.ExecRequest, runnerv1.ExecResponse], error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) CancelExecution(context.Context, *runnerv1.CancelExecutionRequest, ...grpc.CallOption) (*runnerv1.CancelExecutionResponse, error) {
	return nil, errNotImplemented
}

type fakeZitiMgmtClient struct {
	createAgentIdentity    func(context.Context, *zitimgmtv1.CreateAgentIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error)
	createAppIdentity      func(context.Context, *zitimgmtv1.CreateAppIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateAppIdentityResponse, error)
	createRunnerIdentity   func(context.Context, *zitimgmtv1.CreateRunnerIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateRunnerIdentityResponse, error)
	deleteAppIdentity      func(context.Context, *zitimgmtv1.DeleteAppIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteAppIdentityResponse, error)
	deleteIdentity         func(context.Context, *zitimgmtv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error)
	deleteRunnerIdentity   func(context.Context, *zitimgmtv1.DeleteRunnerIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteRunnerIdentityResponse, error)
	listManagedIdentities  func(context.Context, *zitimgmtv1.ListManagedIdentitiesRequest, ...grpc.CallOption) (*zitimgmtv1.ListManagedIdentitiesResponse, error)
	requestServiceIdentity func(context.Context, *zitimgmtv1.RequestServiceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error)
	extendIdentityLease    func(context.Context, *zitimgmtv1.ExtendIdentityLeaseRequest, ...grpc.CallOption) (*zitimgmtv1.ExtendIdentityLeaseResponse, error)
}

func (f *fakeZitiMgmtClient) CreateAgentIdentity(ctx context.Context, req *zitimgmtv1.CreateAgentIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
	if f.createAgentIdentity != nil {
		return f.createAgentIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) CreateAppIdentity(ctx context.Context, req *zitimgmtv1.CreateAppIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateAppIdentityResponse, error) {
	if f.createAppIdentity != nil {
		return f.createAppIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) CreateRunnerIdentity(ctx context.Context, req *zitimgmtv1.CreateRunnerIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateRunnerIdentityResponse, error) {
	if f.createRunnerIdentity != nil {
		return f.createRunnerIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteAppIdentity(ctx context.Context, req *zitimgmtv1.DeleteAppIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteAppIdentityResponse, error) {
	if f.deleteAppIdentity != nil {
		return f.deleteAppIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteIdentity(ctx context.Context, req *zitimgmtv1.DeleteIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
	if f.deleteIdentity != nil {
		return f.deleteIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteRunnerIdentity(ctx context.Context, req *zitimgmtv1.DeleteRunnerIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteRunnerIdentityResponse, error) {
	if f.deleteRunnerIdentity != nil {
		return f.deleteRunnerIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ListManagedIdentities(ctx context.Context, req *zitimgmtv1.ListManagedIdentitiesRequest, opts ...grpc.CallOption) (*zitimgmtv1.ListManagedIdentitiesResponse, error) {
	if f.listManagedIdentities != nil {
		return f.listManagedIdentities(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ResolveIdentity(context.Context, *zitimgmtv1.ResolveIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.ResolveIdentityResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) RequestServiceIdentity(ctx context.Context, req *zitimgmtv1.RequestServiceIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
	if f.requestServiceIdentity != nil {
		return f.requestServiceIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ExtendIdentityLease(ctx context.Context, req *zitimgmtv1.ExtendIdentityLeaseRequest, opts ...grpc.CallOption) (*zitimgmtv1.ExtendIdentityLeaseResponse, error) {
	if f.extendIdentityLease != nil {
		return f.extendIdentityLease(ctx, req, opts...)
	}
	return nil, errNotImplemented
}
