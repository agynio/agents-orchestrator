package reconciler

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	identityv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/identity/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

func TestStartWorkloadCreatesIdentityAndStores(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	zitiID := "ziti-identity"
	jwt := "enrollment-jwt"
	workloadID := "workload-1"
	assembler := newTestAssembler(agentID)

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
			envs := envMap(req.GetMain().GetEnv())
			if envs["ZITI_ENROLLMENT_JWT"] != jwt {
				return nil, errors.New("missing ZITI_ENROLLMENT_JWT")
			}
			return &runnerv1.StartWorkloadResponse{Id: workloadID, Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING}, nil
		},
	}

	workloadStore := &fakeWorkloadStore{
		insert: func(_ context.Context, workloadIDArg string, agentIDArg, threadIDArg uuid.UUID, zitiIdentityID *string) (*store.Workload, error) {
			calls = append(calls, "insert")
			if workloadIDArg != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			if agentIDArg != agentID || threadIDArg != threadID {
				return nil, errors.New("unexpected identifiers")
			}
			if zitiIdentityID == nil || *zitiIdentityID != zitiID {
				return nil, errors.New("unexpected ziti identity id")
			}
			return &store.Workload{WorkloadID: workloadIDArg, AgentID: agentIDArg, ThreadID: threadIDArg, ZitiIdentityID: zitiIdentityID}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		Runner:    runner,
		ZitiMgmt:  zitiMgmt,
		Store:     workloadStore,
		Assembler: assembler,
	})
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID})

	if !reflect.DeepEqual(calls, []string{"create", "start", "insert"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStartWorkloadSkipsIdentityWhenZitiMgmtNil(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	workloadID := "workload-1"
	assembler := newTestAssembler(agentID)

	var calls []string
	runner := &fakeRunnerClient{
		startWorkload: func(_ context.Context, req *runnerv1.StartWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error) {
			calls = append(calls, "start")
			if req.GetMain() == nil {
				return nil, errors.New("missing main container")
			}
			envs := envMap(req.GetMain().GetEnv())
			if _, ok := envs["ZITI_ENROLLMENT_JWT"]; ok {
				return nil, errors.New("unexpected ZITI_ENROLLMENT_JWT")
			}
			return &runnerv1.StartWorkloadResponse{Id: workloadID, Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING}, nil
		},
	}

	workloadStore := &fakeWorkloadStore{
		insert: func(_ context.Context, workloadIDArg string, agentIDArg, threadIDArg uuid.UUID, zitiIdentityID *string) (*store.Workload, error) {
			calls = append(calls, "insert")
			if workloadIDArg != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			if agentIDArg != agentID || threadIDArg != threadID {
				return nil, errors.New("unexpected identifiers")
			}
			if zitiIdentityID != nil {
				return nil, errors.New("unexpected ziti identity id")
			}
			return &store.Workload{WorkloadID: workloadIDArg, AgentID: agentIDArg, ThreadID: threadIDArg}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		Runner:    runner,
		Store:     workloadStore,
		Assembler: assembler,
	})
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID})

	if !reflect.DeepEqual(calls, []string{"start", "insert"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStartWorkloadDeletesIdentityOnRunnerError(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	zitiID := "ziti-identity"
	assembler := newTestAssembler(agentID)

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

	workloadStore := &fakeWorkloadStore{
		insert: func(context.Context, string, uuid.UUID, uuid.UUID, *string) (*store.Workload, error) {
			return nil, errors.New("unexpected insert")
		},
	}

	reconciler := newTestReconciler(Config{
		Runner:    runner,
		ZitiMgmt:  zitiMgmt,
		Store:     workloadStore,
		Assembler: assembler,
	})
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID})

	if !reflect.DeepEqual(calls, []string{"create", "start", "delete"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStartWorkloadStopsAndDeletesIdentityOnStoreFailure(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	zitiID := "ziti-identity"
	workloadID := "workload-1"
	assembler := newTestAssembler(agentID)

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
			return &runnerv1.StartWorkloadResponse{Id: workloadID, Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING}, nil
		},
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			if req.GetWorkloadId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}

	workloadStore := &fakeWorkloadStore{
		insert: func(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ *string) (*store.Workload, error) {
			calls = append(calls, "insert")
			return nil, errors.New("store error")
		},
	}

	reconciler := newTestReconciler(Config{
		Runner:    runner,
		ZitiMgmt:  zitiMgmt,
		Store:     workloadStore,
		Assembler: assembler,
	})
	reconciler.startWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID})

	if !reflect.DeepEqual(calls, []string{"create", "start", "insert", "stop", "delete"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStopWorkloadDeletesIdentityAfterStop(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	assembler := newTestAssembler(agentID)
	zitiID := "ziti-identity"

	var calls []string
	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, _ *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}

	zitiMgmt := &fakeZitiMgmtClient{
		deleteIdentity: func(_ context.Context, _ *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			calls = append(calls, "delete")
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}

	workloadStore := &fakeWorkloadStore{
		delete: func(_ context.Context, _ string) (*store.Workload, error) {
			calls = append(calls, "store-delete")
			return &store.Workload{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		Runner:    runner,
		ZitiMgmt:  zitiMgmt,
		Store:     workloadStore,
		Assembler: assembler,
	})
	reconciler.stopWorkload(ctx, store.Workload{WorkloadID: "workload-1", ZitiIdentityID: &zitiID})

	if len(calls) < 2 || calls[0] != "stop" || calls[1] != "delete" {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStopWorkloadSkipsIdentityWhenNil(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	assembler := newTestAssembler(agentID)

	deleteCalled := false
	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, _ *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}

	zitiMgmt := &fakeZitiMgmtClient{
		deleteIdentity: func(_ context.Context, _ *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			deleteCalled = true
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}

	workloadStore := &fakeWorkloadStore{
		delete: func(_ context.Context, _ string) (*store.Workload, error) {
			return &store.Workload{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		Runner:    runner,
		ZitiMgmt:  zitiMgmt,
		Store:     workloadStore,
		Assembler: assembler,
	})
	reconciler.stopWorkload(ctx, store.Workload{WorkloadID: "workload-1"})

	if deleteCalled {
		t.Fatal("expected no delete identity call")
	}
}

func TestStopWorkloadSkipsIdentityWhenZitiMgmtNil(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	assembler := newTestAssembler(agentID)
	zitiID := "ziti-identity"

	var calls []string
	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, _ *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}

	workloadStore := &fakeWorkloadStore{
		delete: func(_ context.Context, _ string) (*store.Workload, error) {
			calls = append(calls, "store-delete")
			return &store.Workload{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		Runner:    runner,
		Store:     workloadStore,
		Assembler: assembler,
	})
	reconciler.stopWorkload(ctx, store.Workload{WorkloadID: "workload-1", ZitiIdentityID: &zitiID})

	if !reflect.DeepEqual(calls, []string{"stop", "store-delete"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestReconcileOrphanIdentitiesDeletesOrphans(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	assembler := newTestAssembler(agentID)
	activeID := "active-id"
	orphanID := "orphan-id"

	workloadStore := &fakeWorkloadStore{
		listAll: func(context.Context) ([]store.Workload, error) {
			return []store.Workload{{WorkloadID: "workload-1", ZitiIdentityID: &activeID}}, nil
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

	reconciler := newTestReconciler(Config{
		Runner:    &fakeRunnerClient{},
		ZitiMgmt:  zitiMgmt,
		Store:     workloadStore,
		Assembler: assembler,
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
	assembler := newTestAssembler(agentID)
	zitiID := "ziti-id"

	workloadStore := &fakeWorkloadStore{
		listAll: func(context.Context) ([]store.Workload, error) {
			return []store.Workload{{WorkloadID: "workload-1", ZitiIdentityID: &zitiID}}, nil
		},
		delete: func(_ context.Context, _ string) (*store.Workload, error) {
			return &store.Workload{WorkloadID: "workload-1", ZitiIdentityID: &zitiID}, nil
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

	runner := &fakeRunnerClient{
		findWorkloadsByLabels: func(_ context.Context, _ *runnerv1.FindWorkloadsByLabelsRequest, _ ...grpc.CallOption) (*runnerv1.FindWorkloadsByLabelsResponse, error) {
			return &runnerv1.FindWorkloadsByLabelsResponse{TargetIds: []string{}}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		Runner:    runner,
		ZitiMgmt:  zitiMgmt,
		Store:     workloadStore,
		Assembler: assembler,
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
	assembler := newTestAssembler(agentID)
	zitiID := "ziti-id"

	deleted := false
	workloadStore := &fakeWorkloadStore{
		listAll: func(context.Context) ([]store.Workload, error) {
			return []store.Workload{{WorkloadID: "workload-1", ZitiIdentityID: &zitiID}}, nil
		},
		delete: func(_ context.Context, _ string) (*store.Workload, error) {
			deleted = true
			return &store.Workload{WorkloadID: "workload-1", ZitiIdentityID: &zitiID}, nil
		},
	}

	runner := &fakeRunnerClient{
		findWorkloadsByLabels: func(_ context.Context, _ *runnerv1.FindWorkloadsByLabelsRequest, _ ...grpc.CallOption) (*runnerv1.FindWorkloadsByLabelsResponse, error) {
			return &runnerv1.FindWorkloadsByLabelsResponse{TargetIds: []string{}}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		Runner:    runner,
		Store:     workloadStore,
		Assembler: assembler,
	})
	if _, err := reconciler.fetchActual(ctx); err != nil {
		t.Fatalf("fetch actual: %v", err)
	}
	if !deleted {
		t.Fatal("expected stale workload delete")
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
	return New(cfg)
}

func newTestAssembler(agentID uuid.UUID) *assembler.Assembler {
	agentsClient := &fakeAgentsClient{
		getAgent: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, Image: "agent-image"}}, nil
		},
		listSkills: func(context.Context, *agentsv1.ListSkillsRequest, ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		listEnvs: func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		listInitScripts: func(context.Context, *agentsv1.ListInitScriptsRequest, ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		listVolumeAttachments: func(context.Context, *agentsv1.ListVolumeAttachmentsRequest, ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		listMcps: func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		listHooks: func(context.Context, *agentsv1.ListHooksRequest, ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	cfg := &config.Config{
		DefaultInitImage:    "default-init-image",
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}
	return assembler.New(agentsClient, &fakeSecretsClient{}, cfg)
}

func envMap(envs []*runnerv1.EnvVar) map[string]string {
	result := make(map[string]string, len(envs))
	for _, env := range envs {
		result[env.GetName()] = env.GetValue()
	}
	return result
}

type fakeWorkloadStore struct {
	insert           func(context.Context, string, uuid.UUID, uuid.UUID, *string) (*store.Workload, error)
	delete           func(context.Context, string) (*store.Workload, error)
	findByWorkloadID func(context.Context, string) (*store.Workload, error)
	listAll          func(context.Context) ([]store.Workload, error)
}

func (f *fakeWorkloadStore) Insert(ctx context.Context, workloadID string, agentID, threadID uuid.UUID, zitiIdentityID *string) (*store.Workload, error) {
	if f.insert != nil {
		return f.insert(ctx, workloadID, agentID, threadID, zitiIdentityID)
	}
	return nil, errNotImplemented
}

func (f *fakeWorkloadStore) Delete(ctx context.Context, workloadID string) (*store.Workload, error) {
	if f.delete != nil {
		return f.delete(ctx, workloadID)
	}
	return nil, errNotImplemented
}

func (f *fakeWorkloadStore) FindByWorkloadID(ctx context.Context, workloadID string) (*store.Workload, error) {
	if f.findByWorkloadID != nil {
		return f.findByWorkloadID(ctx, workloadID)
	}
	return nil, errNotImplemented
}

func (f *fakeWorkloadStore) ListAll(ctx context.Context) ([]store.Workload, error) {
	if f.listAll != nil {
		return f.listAll(ctx)
	}
	return nil, errNotImplemented
}

type fakeRunnerClient struct {
	startWorkload         func(context.Context, *runnerv1.StartWorkloadRequest, ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error)
	stopWorkload          func(context.Context, *runnerv1.StopWorkloadRequest, ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error)
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

func (f *fakeRunnerClient) InspectWorkload(context.Context, *runnerv1.InspectWorkloadRequest, ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
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
	deleteAppIdentity      func(context.Context, *zitimgmtv1.DeleteAppIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteAppIdentityResponse, error)
	deleteIdentity         func(context.Context, *zitimgmtv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error)
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

type fakeAgentsClient struct {
	getAgent              func(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error)
	listSkills            func(context.Context, *agentsv1.ListSkillsRequest, ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error)
	listEnvs              func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error)
	listInitScripts       func(context.Context, *agentsv1.ListInitScriptsRequest, ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error)
	listVolumeAttachments func(context.Context, *agentsv1.ListVolumeAttachmentsRequest, ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error)
	listMcps              func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error)
	listHooks             func(context.Context, *agentsv1.ListHooksRequest, ...grpc.CallOption) (*agentsv1.ListHooksResponse, error)
	getVolume             func(context.Context, *agentsv1.GetVolumeRequest, ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error)
}

var errNotImplemented = errors.New("not implemented")

func (f *fakeAgentsClient) CreateAgent(context.Context, *agentsv1.CreateAgentRequest, ...grpc.CallOption) (*agentsv1.CreateAgentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetAgent(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
	if f.getAgent != nil {
		return f.getAgent(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateAgent(context.Context, *agentsv1.UpdateAgentRequest, ...grpc.CallOption) (*agentsv1.UpdateAgentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteAgent(context.Context, *agentsv1.DeleteAgentRequest, ...grpc.CallOption) (*agentsv1.DeleteAgentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListAgents(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateVolume(context.Context, *agentsv1.CreateVolumeRequest, ...grpc.CallOption) (*agentsv1.CreateVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetVolume(ctx context.Context, req *agentsv1.GetVolumeRequest, opts ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
	if f.getVolume != nil {
		return f.getVolume(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateVolume(context.Context, *agentsv1.UpdateVolumeRequest, ...grpc.CallOption) (*agentsv1.UpdateVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteVolume(context.Context, *agentsv1.DeleteVolumeRequest, ...grpc.CallOption) (*agentsv1.DeleteVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListVolumes(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateVolumeAttachment(context.Context, *agentsv1.CreateVolumeAttachmentRequest, ...grpc.CallOption) (*agentsv1.CreateVolumeAttachmentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetVolumeAttachment(context.Context, *agentsv1.GetVolumeAttachmentRequest, ...grpc.CallOption) (*agentsv1.GetVolumeAttachmentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteVolumeAttachment(context.Context, *agentsv1.DeleteVolumeAttachmentRequest, ...grpc.CallOption) (*agentsv1.DeleteVolumeAttachmentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListVolumeAttachments(ctx context.Context, req *agentsv1.ListVolumeAttachmentsRequest, opts ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
	if f.listVolumeAttachments != nil {
		return f.listVolumeAttachments(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateMcp(context.Context, *agentsv1.CreateMcpRequest, ...grpc.CallOption) (*agentsv1.CreateMcpResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetMcp(context.Context, *agentsv1.GetMcpRequest, ...grpc.CallOption) (*agentsv1.GetMcpResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateMcp(context.Context, *agentsv1.UpdateMcpRequest, ...grpc.CallOption) (*agentsv1.UpdateMcpResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteMcp(context.Context, *agentsv1.DeleteMcpRequest, ...grpc.CallOption) (*agentsv1.DeleteMcpResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListMcps(ctx context.Context, req *agentsv1.ListMcpsRequest, opts ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
	if f.listMcps != nil {
		return f.listMcps(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateSkill(context.Context, *agentsv1.CreateSkillRequest, ...grpc.CallOption) (*agentsv1.CreateSkillResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetSkill(context.Context, *agentsv1.GetSkillRequest, ...grpc.CallOption) (*agentsv1.GetSkillResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateSkill(context.Context, *agentsv1.UpdateSkillRequest, ...grpc.CallOption) (*agentsv1.UpdateSkillResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteSkill(context.Context, *agentsv1.DeleteSkillRequest, ...grpc.CallOption) (*agentsv1.DeleteSkillResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListSkills(ctx context.Context, req *agentsv1.ListSkillsRequest, opts ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
	if f.listSkills != nil {
		return f.listSkills(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateHook(context.Context, *agentsv1.CreateHookRequest, ...grpc.CallOption) (*agentsv1.CreateHookResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetHook(context.Context, *agentsv1.GetHookRequest, ...grpc.CallOption) (*agentsv1.GetHookResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateHook(context.Context, *agentsv1.UpdateHookRequest, ...grpc.CallOption) (*agentsv1.UpdateHookResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteHook(context.Context, *agentsv1.DeleteHookRequest, ...grpc.CallOption) (*agentsv1.DeleteHookResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListHooks(ctx context.Context, req *agentsv1.ListHooksRequest, opts ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
	if f.listHooks != nil {
		return f.listHooks(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateEnv(context.Context, *agentsv1.CreateEnvRequest, ...grpc.CallOption) (*agentsv1.CreateEnvResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetEnv(context.Context, *agentsv1.GetEnvRequest, ...grpc.CallOption) (*agentsv1.GetEnvResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateEnv(context.Context, *agentsv1.UpdateEnvRequest, ...grpc.CallOption) (*agentsv1.UpdateEnvResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteEnv(context.Context, *agentsv1.DeleteEnvRequest, ...grpc.CallOption) (*agentsv1.DeleteEnvResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListEnvs(ctx context.Context, req *agentsv1.ListEnvsRequest, opts ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
	if f.listEnvs != nil {
		return f.listEnvs(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateInitScript(context.Context, *agentsv1.CreateInitScriptRequest, ...grpc.CallOption) (*agentsv1.CreateInitScriptResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetInitScript(context.Context, *agentsv1.GetInitScriptRequest, ...grpc.CallOption) (*agentsv1.GetInitScriptResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateInitScript(context.Context, *agentsv1.UpdateInitScriptRequest, ...grpc.CallOption) (*agentsv1.UpdateInitScriptResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteInitScript(context.Context, *agentsv1.DeleteInitScriptRequest, ...grpc.CallOption) (*agentsv1.DeleteInitScriptResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListInitScripts(ctx context.Context, req *agentsv1.ListInitScriptsRequest, opts ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
	if f.listInitScripts != nil {
		return f.listInitScripts(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

type fakeSecretsClient struct {
	resolveSecret func(context.Context, *secretsv1.ResolveSecretRequest, ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error)
}

func (f *fakeSecretsClient) CreateSecretProvider(context.Context, *secretsv1.CreateSecretProviderRequest, ...grpc.CallOption) (*secretsv1.CreateSecretProviderResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) GetSecretProvider(context.Context, *secretsv1.GetSecretProviderRequest, ...grpc.CallOption) (*secretsv1.GetSecretProviderResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) UpdateSecretProvider(context.Context, *secretsv1.UpdateSecretProviderRequest, ...grpc.CallOption) (*secretsv1.UpdateSecretProviderResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) DeleteSecretProvider(context.Context, *secretsv1.DeleteSecretProviderRequest, ...grpc.CallOption) (*secretsv1.DeleteSecretProviderResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) ListSecretProviders(context.Context, *secretsv1.ListSecretProvidersRequest, ...grpc.CallOption) (*secretsv1.ListSecretProvidersResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) CreateSecret(context.Context, *secretsv1.CreateSecretRequest, ...grpc.CallOption) (*secretsv1.CreateSecretResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) GetSecret(context.Context, *secretsv1.GetSecretRequest, ...grpc.CallOption) (*secretsv1.GetSecretResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) UpdateSecret(context.Context, *secretsv1.UpdateSecretRequest, ...grpc.CallOption) (*secretsv1.UpdateSecretResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) DeleteSecret(context.Context, *secretsv1.DeleteSecretRequest, ...grpc.CallOption) (*secretsv1.DeleteSecretResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) ListSecrets(context.Context, *secretsv1.ListSecretsRequest, ...grpc.CallOption) (*secretsv1.ListSecretsResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) ResolveSecret(ctx context.Context, req *secretsv1.ResolveSecretRequest, opts ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error) {
	if f.resolveSecret != nil {
		return f.resolveSecret(ctx, req, opts...)
	}
	return nil, errNotImplemented
}
