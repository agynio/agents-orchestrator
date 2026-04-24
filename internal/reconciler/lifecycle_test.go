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
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testOrganizationID         = "11111111-1111-1111-1111-111111111111"
	testAgentID                = "22222222-2222-2222-2222-222222222222"
	testAgentIDAlt             = "33333333-3333-3333-3333-333333333333"
	testAllocatedCPUMillicores = int32(500)
	testAllocatedRAMBytes      = int64(1 << 30)
)

var errNotImplemented = errors.New("not implemented")

func TestStartWorkloadCreatesIdentityAndStores(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	zitiID := "ziti-identity"
	jwt := "enrollment-jwt"
	runnerID := "runner-1"
	mainContainerID := "container-main"
	testAssembler := newTestAssembler(agentID, true)

	var calls []string
	var workloadID string
	zitiMgmt := &fakeZitiMgmtClient{
		createAgentIdentity: func(_ context.Context, req *zitimgmtv1.CreateAgentIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
			calls = append(calls, "create")
			if req.GetAgentId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			workloadID = req.GetWorkloadId()
			if workloadID == "" {
				return nil, errors.New("missing workload id")
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
			if req.GetWorkloadId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			labelKey := assembler.LabelKeyPrefix + assembler.LabelWorkloadKey
			if req.GetAdditionalProperties()[labelKey] != workloadID {
				return nil, errors.New("unexpected workload key label")
			}
			mainEnvs := envMap(req.GetMain().GetEnv())
			if mainEnvs["WORKLOAD_ID"] != workloadID {
				return nil, errors.New("missing WORKLOAD_ID")
			}
			zitiContainer := testutil.FindInitContainer(req.GetInitContainers(), assembler.ZitiSidecarContainerName)
			if zitiContainer == nil {
				return nil, errors.New("missing ziti sidecar container")
			}
			envs := envMap(zitiContainer.GetEnv())
			if envs[assembler.ZitiEnrollmentTokenEnvVar] != jwt {
				return nil, errors.New("missing ZITI_ENROLL_TOKEN")
			}
			if envs[assembler.ZitiIdentityBasenameEnvVar] != assembler.ZitiIdentityBasename {
				return nil, errors.New("missing ZITI_IDENTITY_BASENAME")
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
			if req.GetId() == "" {
				return nil, errors.New("missing workload id")
			}
			if workloadID == "" {
				return nil, errors.New("missing workload id")
			}
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
			if req.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING {
				return nil, errors.New("unexpected workload status")
			}
			if req.GetAllocatedCpuMillicores() != testAllocatedCPUMillicores {
				return nil, errors.New("unexpected allocated cpu")
			}
			if req.GetAllocatedRamBytes() != testAllocatedRAMBytes {
				return nil, errors.New("unexpected allocated ram")
			}
			return &runnersv1.CreateWorkloadResponse{}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			calls = append(calls, "update-workload")
			if req.GetId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			if req.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
				return nil, errors.New("unexpected workload status")
			}
			if req.GetInstanceId() != workloadID {
				return nil, errors.New("unexpected instance id")
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
			return &runnersv1.UpdateWorkloadResponse{}, nil
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
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, newDegradeTracker())

	if !reflect.DeepEqual(calls, []string{"dial", "create", "create-workload", "start", "update-workload"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStartWorkloadSkipsIdentityWhenZitiMgmtNil(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	runnerID := "runner-1"
	mainContainerID := "container-main"
	testAssembler := newTestAssembler(agentID, false)

	var calls []string
	var workloadID string
	runner := &fakeRunnerClient{
		startWorkload: func(_ context.Context, req *runnerv1.StartWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error) {
			calls = append(calls, "start")
			if req.GetMain() == nil {
				return nil, errors.New("missing main container")
			}
			if req.GetWorkloadId() == "" {
				return nil, errors.New("missing workload id")
			}
			if req.GetWorkloadId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			labelKey := assembler.LabelKeyPrefix + assembler.LabelWorkloadKey
			if req.GetAdditionalProperties()[labelKey] != workloadID {
				return nil, errors.New("unexpected workload key label")
			}
			mainEnvs := envMap(req.GetMain().GetEnv())
			if mainEnvs["WORKLOAD_ID"] != workloadID {
				return nil, errors.New("missing WORKLOAD_ID")
			}
			zitiContainer := testutil.FindInitContainer(req.GetInitContainers(), assembler.ZitiSidecarContainerName)
			if zitiContainer != nil {
				envs := envMap(zitiContainer.GetEnv())
				if _, ok := envs[assembler.ZitiEnrollmentTokenEnvVar]; ok {
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
			if req.GetId() == "" {
				return nil, errors.New("missing workload id")
			}
			workloadID = req.GetId()
			if req.GetRunnerId() != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			if req.GetAgentId() != agentID.String() || req.GetThreadId() != threadID.String() {
				return nil, errors.New("unexpected identifiers")
			}
			if req.GetZitiIdentityId() != "" {
				return nil, errors.New("unexpected ziti identity id")
			}
			if req.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING {
				return nil, errors.New("unexpected workload status")
			}
			return &runnersv1.CreateWorkloadResponse{}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			calls = append(calls, "update-workload")
			if req.GetId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			if req.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
				return nil, errors.New("unexpected workload status")
			}
			if req.GetInstanceId() != workloadID {
				return nil, errors.New("unexpected instance id")
			}
			if len(req.GetContainers()) != 1 || req.GetContainers()[0].GetContainerId() != mainContainerID {
				return nil, errors.New("unexpected containers")
			}
			return &runnersv1.UpdateWorkloadResponse{}, nil
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
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, newDegradeTracker())

	if !reflect.DeepEqual(calls, []string{"dial", "create-workload", "start", "update-workload"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStartWorkloadPinsRunnerFromVolumes(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	mainContainerID := "container-main"
	testAssembler := newTestAssembler(agentID, false)

	var calls []string
	var workloadID string
	runner := &fakeRunnerClient{
		startWorkload: func(_ context.Context, req *runnerv1.StartWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error) {
			calls = append(calls, "start")
			workloadID = req.GetWorkloadId()
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
		listVolumesByThread: func(_ context.Context, req *runnersv1.ListVolumesByThreadRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesByThreadResponse, error) {
			calls = append(calls, "list-volumes")
			if req.GetThreadId() != threadID.String() {
				return nil, errors.New("unexpected thread id")
			}
			return &runnersv1.ListVolumesByThreadResponse{Volumes: []*runnersv1.Volume{
				{
					Meta:     &runnersv1.EntityMeta{Id: volumeKey},
					RunnerId: runnerID,
					Status:   runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
					ThreadId: threadID.String(),
					VolumeId: "volume-id",
				},
			}}, nil
		},
		getRunner: func(_ context.Context, req *runnersv1.GetRunnerRequest, _ ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
			calls = append(calls, "get-runner")
			if req.GetId() != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return &runnersv1.GetRunnerResponse{Runner: buildRunner(runnerID)}, nil
		},
		createWorkload: func(_ context.Context, req *runnersv1.CreateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
			calls = append(calls, "create-workload")
			if req.GetRunnerId() != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return &runnersv1.CreateWorkloadResponse{}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			calls = append(calls, "update-workload")
			if req.GetInstanceId() == "" {
				return nil, errors.New("missing instance id")
			}
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
		listRunners: func(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return nil, errors.New("unexpected list runners")
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, newDegradeTracker())

	if !reflect.DeepEqual(calls, []string{"list-volumes", "get-runner", "dial", "create-workload", "start", "update-workload"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStartWorkloadDegradesWhenPinnedRunnerNotEnrolled(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	testAssembler := newTestAssembler(agentID, false)

	var calls []string
	threads := &fakeThreadsClient{
		degradeThread: func(_ context.Context, req *threadsv1.DegradeThreadRequest, _ ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error) {
			calls = append(calls, "degrade")
			if req.GetThreadId() != threadID.String() {
				return nil, errors.New("unexpected thread id")
			}
			if req.GetReason() != degradeReasonRunnerDeprovisioned {
				return nil, errors.New("unexpected degrade reason")
			}
			return &threadsv1.DegradeThreadResponse{}, nil
		},
	}

	runners := &fakeRunnersClient{
		listVolumesByThread: func(_ context.Context, req *runnersv1.ListVolumesByThreadRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesByThreadResponse, error) {
			calls = append(calls, "list-volumes")
			if req.GetThreadId() != threadID.String() {
				return nil, errors.New("unexpected thread id")
			}
			return &runnersv1.ListVolumesByThreadResponse{Volumes: []*runnersv1.Volume{
				{
					Meta:     &runnersv1.EntityMeta{Id: volumeKey},
					RunnerId: runnerID,
					Status:   runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
					ThreadId: threadID.String(),
					VolumeId: "volume-id",
				},
			}}, nil
		},
		getRunner: func(_ context.Context, req *runnersv1.GetRunnerRequest, _ ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
			calls = append(calls, "get-runner")
			if req.GetId() != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return &runnersv1.GetRunnerResponse{Runner: &runnersv1.Runner{
				Meta:   &runnersv1.EntityMeta{Id: runnerID},
				Status: runnersv1.RunnerStatus_RUNNER_STATUS_OFFLINE,
			}}, nil
		},
		createWorkload: func(context.Context, *runnersv1.CreateWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
			return nil, errors.New("unexpected create workload")
		},
	}

	runnerDialer := &fakeRunnerDialer{
		dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) {
			return nil, errors.New("unexpected dial")
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Threads:      threads,
		Assembler:    testAssembler,
	})
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, newDegradeTracker())

	if !reflect.DeepEqual(calls, []string{"list-volumes", "get-runner", "degrade"}) {
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
		createAgentIdentity: func(_ context.Context, req *zitimgmtv1.CreateAgentIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
			calls = append(calls, "create")
			if req.GetWorkloadId() == "" {
				return nil, errors.New("missing workload id")
			}
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
		createWorkload: func(_ context.Context, req *runnersv1.CreateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
			calls = append(calls, "create-workload")
			if req.GetId() == "" {
				return nil, errors.New("missing workload id")
			}
			return &runnersv1.CreateWorkloadResponse{}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			calls = append(calls, "update-workload")
			if req.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
				return nil, errors.New("unexpected workload status")
			}
			if req.GetRemovedAt() == nil {
				return nil, errors.New("missing removed_at")
			}
			return &runnersv1.UpdateWorkloadResponse{}, nil
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
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, newDegradeTracker())

	if !reflect.DeepEqual(calls, []string{"dial", "create", "create-workload", "start", "update-workload", "delete"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStartWorkloadRollsBackOnWorkloadIDMismatch(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	zitiID := "ziti-identity"
	runnerID := "runner-1"
	instanceID := "runner-workload-1"
	mainContainerID := "container-main"
	testAssembler := newTestAssembler(agentID, true)

	var calls []string
	var workloadID string
	zitiMgmt := &fakeZitiMgmtClient{
		createAgentIdentity: func(_ context.Context, req *zitimgmtv1.CreateAgentIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
			calls = append(calls, "create")
			workloadID = req.GetWorkloadId()
			if workloadID == "" {
				return nil, errors.New("missing workload id")
			}
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
		startWorkload: func(_ context.Context, req *runnerv1.StartWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error) {
			calls = append(calls, "start")
			if req.GetWorkloadId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.StartWorkloadResponse{
				Id:     instanceID,
				Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
				Containers: &runnerv1.WorkloadContainers{
					Main: mainContainerID,
				},
			}, nil
		},
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			if req.GetWorkloadId() != instanceID {
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
		createWorkload: func(_ context.Context, req *runnersv1.CreateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
			calls = append(calls, "create-workload")
			if req.GetId() == "" {
				return nil, errors.New("missing workload id")
			}
			if workloadID == "" {
				return nil, errors.New("missing workload id")
			}
			if req.GetId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			if req.GetRunnerId() != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return &runnersv1.CreateWorkloadResponse{}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			calls = append(calls, "update-workload")
			if req.GetId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			if req.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
				return nil, errors.New("unexpected workload status")
			}
			if req.GetInstanceId() != instanceID {
				return nil, errors.New("unexpected instance id")
			}
			if req.GetRemovedAt() == nil {
				return nil, errors.New("missing removed_at")
			}
			return &runnersv1.UpdateWorkloadResponse{}, nil
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
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, newDegradeTracker())

	if !reflect.DeepEqual(calls, []string{"dial", "create", "create-workload", "start", "stop", "update-workload", "delete"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStartWorkloadStopsAndDeletesIdentityOnStoreFailure(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	zitiID := "ziti-identity"
	runnerID := "runner-1"
	testAssembler := newTestAssembler(agentID, true)

	var calls []string
	zitiMgmt := &fakeZitiMgmtClient{
		createAgentIdentity: func(_ context.Context, req *zitimgmtv1.CreateAgentIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
			calls = append(calls, "create")
			if req.GetWorkloadId() == "" {
				return nil, errors.New("missing workload id")
			}
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
			return nil, errors.New("unexpected start")
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
			if req.GetId() == "" {
				return nil, errors.New("missing workload id")
			}
			return nil, errors.New("create error")
		},
		updateWorkload: func(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			calls = append(calls, "update-workload")
			return nil, errors.New("unexpected update")
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
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, newDegradeTracker())

	if !reflect.DeepEqual(calls, []string{"dial", "create", "create-workload", "delete"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStopWorkloadDeletesIdentityAfterStop(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"
	zitiID := "ziti-identity"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID

	var calls []string
	var updateStatuses []runnersv1.WorkloadStatus
	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			if req.GetWorkloadId() != rawInstanceID {
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

	zitiMgmt := &fakeZitiMgmtClient{
		deleteIdentity: func(_ context.Context, _ *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			calls = append(calls, "delete")
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}

	runners := &fakeRunnersClient{
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			calls = append(calls, "update-workload")
			updateStatuses = append(updateStatuses, req.GetStatus())
			if req.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED && req.GetRemovedAt() == nil {
				return nil, errors.New("missing removed_at")
			}
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		ZitiMgmt:     zitiMgmt,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, AgentId: agentID.String(), ZitiIdentityId: zitiID, InstanceId: stringPtr(instanceID)})

	if !reflect.DeepEqual(calls, []string{"dial", "update-workload", "stop", "update-workload", "delete"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
	if !reflect.DeepEqual(updateStatuses, []runnersv1.WorkloadStatus{runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING, runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED}) {
		t.Fatalf("unexpected update statuses: %v", updateStatuses)
	}
}

func TestStopWorkloadMarksMissingRunnerOnNoTerminators(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"
	instanceID := "workload-" + uuid.New().String()

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
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

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{
		Meta:       &runnersv1.EntityMeta{Id: "workload-1"},
		RunnerId:   runnerID,
		AgentId:    agentID.String(),
		InstanceId: stringPtr(instanceID),
		Status:     runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
	})

	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.GetId() != "workload-1" {
		t.Fatalf("unexpected workload id: %s", updateReq.GetId())
	}
	if updateReq.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		t.Fatalf("unexpected workload status: %v", updateReq.GetStatus())
	}
	if updateReq.GetRemovedAt() == nil {
		t.Fatal("expected removed_at")
	}
}

func TestStopWorkloadMarksMissingRunnerOnNoTerminatorsStopError(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID

	var updateStatuses []runnersv1.WorkloadStatus
	runners := &fakeRunnersClient{
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateStatuses = append(updateStatuses, req.GetStatus())
			if req.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED && req.GetRemovedAt() == nil {
				return nil, errors.New("missing removed_at")
			}
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	stopCalled := false
	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			stopCalled = true
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

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{
		Meta:       &runnersv1.EntityMeta{Id: "workload-1"},
		RunnerId:   runnerID,
		AgentId:    agentID.String(),
		InstanceId: stringPtr(instanceID),
		Status:     runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
	})

	if !stopCalled {
		t.Fatal("expected stop workload")
	}
	if !reflect.DeepEqual(updateStatuses, []runnersv1.WorkloadStatus{runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING, runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED}) {
		t.Fatalf("unexpected update statuses: %v", updateStatuses)
	}
}

func TestStopWorkloadMarksFailedWhenInstanceMissing(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"

	updateCalled := false
	var updateRequest *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateCalled = true
			updateRequest = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	dialCalled := false
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, _ string) (runnerv1.RunnerServiceClient, error) {
			dialCalled = true
			return nil, errors.New("unexpected dial")
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, AgentId: agentID.String()})

	if dialCalled {
		t.Fatal("expected no dial call")
	}
	if !updateCalled {
		t.Fatal("expected update workload call")
	}
	if updateRequest.GetId() != "workload-1" {
		t.Fatalf("unexpected workload id: %s", updateRequest.GetId())
	}
	if updateRequest.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		t.Fatalf("unexpected workload status: %v", updateRequest.GetStatus())
	}
	if updateRequest.GetRemovedAt() == nil {
		t.Fatal("expected removed_at")
	}
	if updateRequest.GetInstanceId() != "" {
		t.Fatalf("expected empty instance id, got %q", updateRequest.GetInstanceId())
	}
}

func TestStopWorkloadSkipsIdentityWhenNil(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"
	instanceID := "runner-workload-1"

	deleteCalled := false
	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			if req.GetWorkloadId() != instanceID {
				return nil, errors.New("unexpected workload id")
			}
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
		updateWorkload: func(_ context.Context, _ *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		ZitiMgmt:     zitiMgmt,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, AgentId: agentID.String(), InstanceId: stringPtr(instanceID)})

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
	instanceID := "runner-workload-1"

	var calls []string
	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			if req.GetWorkloadId() != instanceID {
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
		updateWorkload: func(_ context.Context, _ *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			calls = append(calls, "update-workload")
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, AgentId: agentID.String(), ZitiIdentityId: zitiID, InstanceId: stringPtr(instanceID)})

	if !reflect.DeepEqual(calls, []string{"dial", "update-workload", "stop", "update-workload"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStopRunnerWorkloadIgnoresNotFoundForUUID(t *testing.T) {
	ctx := context.Background()
	instanceID := uuid.New().String()
	calls := []string{}

	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, req.GetWorkloadId())
			if req.GetTimeoutSec() != 30 {
				return nil, errors.New("unexpected timeout")
			}
			return nil, status.Error(codes.NotFound, "not found")
		},
	}

	reconciler := newTestReconciler(Config{})
	if err := reconciler.stopRunnerWorkload(ctx, runner, instanceID); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	expected := []string{instanceID, "workload-" + instanceID}
	if !reflect.DeepEqual(calls, expected) {
		t.Fatalf("expected calls %v, got %v", expected, calls)
	}
}

func TestStopRunnerWorkloadRetriesWithPrefixedID(t *testing.T) {
	ctx := context.Background()
	instanceID := uuid.New().String()
	calls := []string{}

	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, req.GetWorkloadId())
			if req.GetTimeoutSec() != 30 {
				return nil, errors.New("unexpected timeout")
			}
			switch req.GetWorkloadId() {
			case instanceID:
				return nil, status.Error(codes.NotFound, "not found")
			case "workload-" + instanceID:
				return &runnerv1.StopWorkloadResponse{}, nil
			default:
				return nil, errors.New("unexpected workload id")
			}
		},
	}

	reconciler := newTestReconciler(Config{})
	if err := reconciler.stopRunnerWorkload(ctx, runner, instanceID); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	expected := []string{instanceID, "workload-" + instanceID}
	if !reflect.DeepEqual(calls, expected) {
		t.Fatalf("expected calls %v, got %v", expected, calls)
	}
}

func TestStopRunnerWorkloadReturnsNotFoundForInvalidID(t *testing.T) {
	ctx := context.Background()
	instanceID := "workload-" + uuid.New().String()
	called := false

	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			called = true
			if req.GetWorkloadId() != instanceID {
				return nil, errors.New("unexpected workload id")
			}
			return nil, status.Error(codes.NotFound, "not found")
		},
	}

	reconciler := newTestReconciler(Config{})
	err := reconciler.stopRunnerWorkload(ctx, runner, instanceID)
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found error, got %v", err)
	}
	if !called {
		t.Fatal("expected stop workload call")
	}
}

func TestStopRunnerWorkloadReturnsErrorOnFailure(t *testing.T) {
	ctx := context.Background()
	instanceID := "runner-workload-1"

	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			if req.GetWorkloadId() != instanceID {
				return nil, errors.New("unexpected workload id")
			}
			return nil, status.Error(codes.Internal, "stop failed")
		},
	}

	reconciler := newTestReconciler(Config{})
	err := reconciler.stopRunnerWorkload(ctx, runner, instanceID)
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected internal error, got %v", err)
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

func TestFetchActualReturnsTrackedWorkloads(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"

	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID},
			}}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		Runners:   runners,
		Assembler: testAssembler,
	})
	actual, err := reconciler.fetchActual(ctx)
	if err != nil {
		t.Fatalf("fetch actual: %v", err)
	}
	if len(actual) != 1 {
		t.Fatalf("expected workload, got %d", len(actual))
	}
}

func TestFetchActualSkipsMissingRunnerID(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, false)

	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: "workload-1"}},
			}}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		Runners:   runners,
		Assembler: testAssembler,
	})
	actual, err := reconciler.fetchActual(ctx)
	if err != nil {
		t.Fatalf("fetch actual: %v", err)
	}
	if len(actual) != 0 {
		t.Fatalf("expected no workloads, got %d", len(actual))
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
	if cfg.MeteringSampleInterval == 0 {
		cfg.MeteringSampleInterval = time.Minute
	}
	if cfg.RunnerDialer == nil {
		cfg.RunnerDialer = &fakeRunnerDialer{}
	}
	if cfg.Metering == nil {
		cfg.Metering = &fakeMeteringClient{}
	}
	if cfg.Agents == nil {
		cfg.Agents = defaultAgentsClient()
	} else if agentsClient, ok := cfg.Agents.(*testutil.FakeAgentsClient); ok && agentsClient.ListAgentsFunc == nil {
		agentsClient.ListAgentsFunc = defaultListAgentsFunc()
	}
	return New(cfg)
}

func defaultAgentsClient() *testutil.FakeAgentsClient {
	return &testutil.FakeAgentsClient{ListAgentsFunc: defaultListAgentsFunc()}
}

func defaultListAgentsFunc() func(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
	return func(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
		return &agentsv1.ListAgentsResponse{Agents: []*agentsv1.Agent{
			{
				Meta:           &agentsv1.EntityMeta{Id: testAgentID},
				OrganizationId: testOrganizationID,
			},
		}}, nil
	}
}

func newTestAssembler(agentID uuid.UUID, zitiEnabled bool) *assembler.Assembler {
	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{
				Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
				OrganizationId: testOrganizationID,
				Image:          "agent-image",
				InitImage:      "agent-init-image",
				Resources: &agentsv1.ComputeResources{
					RequestsCpu:    "500m",
					RequestsMemory: "1Gi",
				},
			}}, nil
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
	createWorkload        func(context.Context, *runnersv1.CreateWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error)
	createVolume          func(context.Context, *runnersv1.CreateVolumeRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeResponse, error)
	deleteWorkload        func(context.Context, *runnersv1.DeleteWorkloadRequest, ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error)
	getRunner             func(context.Context, *runnersv1.GetRunnerRequest, ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error)
	listWorkloads         func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error)
	listWorkloadsByThread func(context.Context, *runnersv1.ListWorkloadsByThreadRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error)
	batchUpdateWorkload   func(context.Context, *runnersv1.BatchUpdateWorkloadSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error)
	listVolumes           func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error)
	listVolumesByThread   func(context.Context, *runnersv1.ListVolumesByThreadRequest, ...grpc.CallOption) (*runnersv1.ListVolumesByThreadResponse, error)
	listRunners           func(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error)
	updateWorkload        func(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error)
	updateWorkloadStatus  func(context.Context, *runnersv1.UpdateWorkloadStatusRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadStatusResponse, error)
	updateVolume          func(context.Context, *runnersv1.UpdateVolumeRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error)
	batchUpdateVolume     func(context.Context, *runnersv1.BatchUpdateVolumeSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateVolumeSampledAtResponse, error)
	streamWorkloadLogs    func(context.Context, *runnerv1.StreamWorkloadLogsRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[runnerv1.StreamWorkloadLogsResponse], error)
}

func (f *fakeRunnersClient) RegisterRunner(context.Context, *runnersv1.RegisterRunnerRequest, ...grpc.CallOption) (*runnersv1.RegisterRunnerResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) GetRunner(ctx context.Context, req *runnersv1.GetRunnerRequest, opts ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
	if f.getRunner != nil {
		return f.getRunner(ctx, req, opts...)
	}
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

func (f *fakeRunnersClient) CreateVolume(ctx context.Context, req *runnersv1.CreateVolumeRequest, opts ...grpc.CallOption) (*runnersv1.CreateVolumeResponse, error) {
	if f.createVolume != nil {
		return f.createVolume(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) UpdateWorkload(ctx context.Context, req *runnersv1.UpdateWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
	if f.updateWorkload != nil {
		return f.updateWorkload(ctx, req, opts...)
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

func (f *fakeRunnersClient) GetVolume(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListWorkloadsByThread(ctx context.Context, req *runnersv1.ListWorkloadsByThreadRequest, opts ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
	if f.listWorkloadsByThread != nil {
		return f.listWorkloadsByThread(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListWorkloads(ctx context.Context, req *runnersv1.ListWorkloadsRequest, opts ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
	if f.listWorkloads != nil {
		return f.listWorkloads(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) BatchUpdateWorkloadSampledAt(ctx context.Context, req *runnersv1.BatchUpdateWorkloadSampledAtRequest, opts ...grpc.CallOption) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error) {
	if f.batchUpdateWorkload != nil {
		return f.batchUpdateWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListVolumes(ctx context.Context, req *runnersv1.ListVolumesRequest, opts ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
	if f.listVolumes != nil {
		return f.listVolumes(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListVolumesByThread(ctx context.Context, req *runnersv1.ListVolumesByThreadRequest, opts ...grpc.CallOption) (*runnersv1.ListVolumesByThreadResponse, error) {
	if f.listVolumesByThread != nil {
		return f.listVolumesByThread(ctx, req, opts...)
	}
	return &runnersv1.ListVolumesByThreadResponse{}, nil
}

func (f *fakeRunnersClient) BatchUpdateVolumeSampledAt(ctx context.Context, req *runnersv1.BatchUpdateVolumeSampledAtRequest, opts ...grpc.CallOption) (*runnersv1.BatchUpdateVolumeSampledAtResponse, error) {
	if f.batchUpdateVolume != nil {
		return f.batchUpdateVolume(ctx, req, opts...)
	}
	return &runnersv1.BatchUpdateVolumeSampledAtResponse{}, nil
}

func (f *fakeRunnersClient) UpdateVolume(ctx context.Context, req *runnersv1.UpdateVolumeRequest, opts ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error) {
	if f.updateVolume != nil {
		return f.updateVolume(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) TouchWorkload(context.Context, *runnersv1.TouchWorkloadRequest, ...grpc.CallOption) (*runnersv1.TouchWorkloadResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) StreamWorkloadLogs(ctx context.Context, req *runnerv1.StreamWorkloadLogsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[runnerv1.StreamWorkloadLogsResponse], error) {
	if f.streamWorkloadLogs != nil {
		return f.streamWorkloadLogs(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

type fakeRunnerClient struct {
	startWorkload         func(context.Context, *runnerv1.StartWorkloadRequest, ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error)
	stopWorkload          func(context.Context, *runnerv1.StopWorkloadRequest, ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error)
	listWorkloads         func(context.Context, *runnerv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error)
	listVolumes           func(context.Context, *runnerv1.ListVolumesRequest, ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error)
	removeVolume          func(context.Context, *runnerv1.RemoveVolumeRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeResponse, error)
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

func (f *fakeRunnerClient) ListWorkloads(ctx context.Context, req *runnerv1.ListWorkloadsRequest, opts ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
	if f.listWorkloads != nil {
		return f.listWorkloads(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) ListVolumes(ctx context.Context, req *runnerv1.ListVolumesRequest, opts ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
	if f.listVolumes != nil {
		return f.listVolumes(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) ListWorkloadsByVolume(context.Context, *runnerv1.ListWorkloadsByVolumeRequest, ...grpc.CallOption) (*runnerv1.ListWorkloadsByVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) RemoveVolume(ctx context.Context, req *runnerv1.RemoveVolumeRequest, opts ...grpc.CallOption) (*runnerv1.RemoveVolumeResponse, error) {
	if f.removeVolume != nil {
		return f.removeVolume(ctx, req, opts...)
	}
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
	createService          func(context.Context, *zitimgmtv1.CreateServiceRequest, ...grpc.CallOption) (*zitimgmtv1.CreateServiceResponse, error)
	createRunnerIdentity   func(context.Context, *zitimgmtv1.CreateRunnerIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateRunnerIdentityResponse, error)
	deleteAppIdentity      func(context.Context, *zitimgmtv1.DeleteAppIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteAppIdentityResponse, error)
	deleteIdentity         func(context.Context, *zitimgmtv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error)
	deleteRunnerIdentity   func(context.Context, *zitimgmtv1.DeleteRunnerIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteRunnerIdentityResponse, error)
	listManagedIdentities  func(context.Context, *zitimgmtv1.ListManagedIdentitiesRequest, ...grpc.CallOption) (*zitimgmtv1.ListManagedIdentitiesResponse, error)
	requestServiceIdentity func(context.Context, *zitimgmtv1.RequestServiceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error)
	extendIdentityLease    func(context.Context, *zitimgmtv1.ExtendIdentityLeaseRequest, ...grpc.CallOption) (*zitimgmtv1.ExtendIdentityLeaseResponse, error)
	createServicePolicy    func(context.Context, *zitimgmtv1.CreateServicePolicyRequest, ...grpc.CallOption) (*zitimgmtv1.CreateServicePolicyResponse, error)
	deleteServicePolicy    func(context.Context, *zitimgmtv1.DeleteServicePolicyRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteServicePolicyResponse, error)
	deleteService          func(context.Context, *zitimgmtv1.DeleteServiceRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteServiceResponse, error)
	createDeviceIdentity   func(context.Context, *zitimgmtv1.CreateDeviceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateDeviceIdentityResponse, error)
	deleteDeviceIdentity   func(context.Context, *zitimgmtv1.DeleteDeviceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteDeviceIdentityResponse, error)
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

func (f *fakeZitiMgmtClient) CreateService(ctx context.Context, req *zitimgmtv1.CreateServiceRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateServiceResponse, error) {
	if f.createService != nil {
		return f.createService(ctx, req, opts...)
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

func (f *fakeZitiMgmtClient) CreateServicePolicy(ctx context.Context, req *zitimgmtv1.CreateServicePolicyRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateServicePolicyResponse, error) {
	if f.createServicePolicy != nil {
		return f.createServicePolicy(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteServicePolicy(ctx context.Context, req *zitimgmtv1.DeleteServicePolicyRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteServicePolicyResponse, error) {
	if f.deleteServicePolicy != nil {
		return f.deleteServicePolicy(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteService(ctx context.Context, req *zitimgmtv1.DeleteServiceRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteServiceResponse, error) {
	if f.deleteService != nil {
		return f.deleteService(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) CreateDeviceIdentity(ctx context.Context, req *zitimgmtv1.CreateDeviceIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateDeviceIdentityResponse, error) {
	if f.createDeviceIdentity != nil {
		return f.createDeviceIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteDeviceIdentity(ctx context.Context, req *zitimgmtv1.DeleteDeviceIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteDeviceIdentityResponse, error) {
	if f.deleteDeviceIdentity != nil {
		return f.deleteDeviceIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}
