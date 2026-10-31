package reconciler

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	exposev1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/expose/v1"
	imageproxyv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/image_proxy/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeExposeClient struct {
	listExposures func(context.Context, *exposev1.ListExposuresRequest, ...grpc.CallOption) (*exposev1.ListExposuresResponse, error)
}

func (f *fakeExposeClient) ListExposures(ctx context.Context, req *exposev1.ListExposuresRequest, opts ...grpc.CallOption) (*exposev1.ListExposuresResponse, error) {
	if f.listExposures != nil {
		return f.listExposures(ctx, req, opts...)
	}
	return &exposev1.ListExposuresResponse{}, nil
}

type fakeImageProxyClient struct {
	mint   func(context.Context, *imageproxyv1.MintPullCredentialRequest, ...grpc.CallOption) (*imageproxyv1.MintPullCredentialResponse, error)
	revoke func(context.Context, *imageproxyv1.RevokePullCredentialRequest, ...grpc.CallOption) (*imageproxyv1.RevokePullCredentialResponse, error)
}

func (f *fakeImageProxyClient) MintPullCredential(ctx context.Context, req *imageproxyv1.MintPullCredentialRequest, opts ...grpc.CallOption) (*imageproxyv1.MintPullCredentialResponse, error) {
	if f.mint != nil {
		return f.mint(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeImageProxyClient) RevokePullCredential(ctx context.Context, req *imageproxyv1.RevokePullCredentialRequest, opts ...grpc.CallOption) (*imageproxyv1.RevokePullCredentialResponse, error) {
	if f.revoke != nil {
		return f.revoke(ctx, req, opts...)
	}
	return &imageproxyv1.RevokePullCredentialResponse{}, nil
}

// hibernatingRunner stands for a runner whose catalog report advertises the
// capability -- the k8s-microvm-runner, once there is one.
func hibernatingRunner(id string) *runnersv1.Runner {
	runner := buildRunner(id)
	runner.Capabilities = []string{hibernateCapability}
	return runner
}

// idleAgentWorkload is a running agent workload whose agent reported its turn
// over at activityAt.
func idleAgentWorkload(agentID, instanceID uuid.UUID, runnerID string, activityAt time.Time) *runnersv1.Workload {
	id := uuid.NewString()
	return &runnersv1.Workload{
		Meta:            &runnersv1.EntityMeta{Id: id, CreatedAt: timestamppb.New(activityAt.Add(-time.Hour))},
		RunnerId:        runnerID,
		OrganizationId:  testOrganizationID,
		AgentId:         agentID.String(),
		AgentClassId:    stringPtr(agentID.String()),
		AgentInstanceId: stringPtr(instanceID.String()),
		OwnerKind:       runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:         instanceID.String(),
		Status:          runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		AgentState:      runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_IDLE,
		LastActivityAt:  timestamppb.New(activityAt),
		InstanceId:      stringPtr(id),
		ZitiIdentityId:  "ziti-" + id,
	}
}

// hibernatedAgentWorkload is one asleep since hibernatedAt.
func hibernatedAgentWorkload(agentID, instanceID uuid.UUID, runnerID string, hibernatedAt time.Time) *runnersv1.Workload {
	workload := idleAgentWorkload(agentID, instanceID, runnerID, hibernatedAt.Add(-time.Minute))
	workload.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED
	workload.HibernatedAt = timestamppb.New(hibernatedAt)
	size := int64(400 << 20)
	workload.SnapshotSizeBytes = &size
	return workload
}

func cloneWorkload(workload *runnersv1.Workload) *runnersv1.Workload {
	return proto.Clone(workload).(*runnersv1.Workload)
}

// The record alone decides most of it: the agent's state, the class's policy,
// the clock. What the record cannot say -- the runner's capability, an exposed
// port, the inbox -- is checked in the loop, so none of it is here.
func TestComputeActionsHibernation(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	agentID := uuid.New()
	noSleepAgent := uuid.New()
	instanceID := uuid.New()
	policies := map[uuid.UUID]agentPolicy{
		agentID:      {idleTimeout: 10 * time.Minute, hibernate: true, hibernateAfter: time.Minute, hibernationTTL: time.Hour},
		noSleepAgent: {idleTimeout: 10 * time.Minute, hibernate: false},
	}
	fallback := agentPolicy{idleTimeout: 5 * time.Minute}
	desired := []AgentInstanceTarget{{AgentID: agentID, AgentInstanceID: instanceID}}

	idle := idleAgentWorkload(agentID, instanceID, "runner-1", now.Add(-2*time.Minute))
	processing := cloneWorkload(idle)
	processing.AgentState = runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_PROCESSING
	fresh := cloneWorkload(idle)
	fresh.LastActivityAt = timestamppb.New(now.Add(-30 * time.Second))
	pastIdleTimeout := cloneWorkload(idle)
	pastIdleTimeout.LastActivityAt = timestamppb.New(now.Add(-11 * time.Minute))
	noSleep := idleAgentWorkload(noSleepAgent, uuid.New(), "runner-1", now.Add(-2*time.Minute))
	unknownClass := idleAgentWorkload(uuid.New(), uuid.New(), "runner-1", now.Add(-2*time.Minute))
	asleep := hibernatedAgentWorkload(agentID, instanceID, "runner-1", now.Add(-30*time.Minute))
	overslept := hibernatedAgentWorkload(agentID, instanceID, "runner-1", now.Add(-2*time.Hour))
	unstamped := cloneWorkload(asleep)
	unstamped.HibernatedAt = nil
	hibernating := cloneWorkload(idle)
	hibernating.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING
	waking := cloneWorkload(asleep)
	waking.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING

	cases := []struct {
		name     string
		desired  []AgentInstanceTarget
		actual   []*runnersv1.Workload
		expected Actions
	}{
		{name: "turn over, hibernate_after elapsed", actual: []*runnersv1.Workload{idle}, expected: Actions{ToHibernate: []*runnersv1.Workload{idle}}},
		{name: "still processing", actual: []*runnersv1.Workload{processing}},
		{name: "turn over, hibernate_after not yet elapsed", actual: []*runnersv1.Workload{fresh}},
		{name: "past idle_timeout: the stop of last resort", actual: []*runnersv1.Workload{pastIdleTimeout}, expected: Actions{ToStop: []*runnersv1.Workload{pastIdleTimeout}}},
		{name: "class keeps its workloads awake", actual: []*runnersv1.Workload{noSleep}},
		{name: "class unreadable: the fallback hibernates nothing", actual: []*runnersv1.Workload{unknownClass}},
		{name: "work waiting for a sleeping workload: wake it", desired: desired, actual: []*runnersv1.Workload{asleep}, expected: Actions{ToWake: []*runnersv1.Workload{asleep}}},
		{name: "asleep within the TTL", actual: []*runnersv1.Workload{asleep}, expected: Actions{Hibernated: []*runnersv1.Workload{asleep}}},
		{name: "asleep past the TTL", actual: []*runnersv1.Workload{overslept}, expected: Actions{ToStop: []*runnersv1.Workload{overslept}}},
		{name: "asleep with no hibernated_at yet", actual: []*runnersv1.Workload{unstamped}, expected: Actions{Hibernated: []*runnersv1.Workload{unstamped}}},
		{name: "hibernating is not interrupted", actual: []*runnersv1.Workload{hibernating}},
		{name: "waking is already on its way", desired: desired, actual: []*runnersv1.Workload{waking}},
		{name: "work waiting for a running workload", desired: desired, actual: []*runnersv1.Workload{idle}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := ComputeActions(testCase.desired, testCase.actual, policies, fallback, now)
			if err != nil {
				t.Fatalf("compute actions: %v", err)
			}
			if !reflect.DeepEqual(result, testCase.expected) {
				t.Fatalf("expected %+v, got %+v", testCase.expected, result)
			}
		})
	}
}

// Nothing advertises the capability yet, so on every runner there is this is
// dormant: no runner call, no inbox read, and the runner is asked about once
// per cycle however many idle workloads it hosts.
func TestHibernateIdleWorkloadRequiresCapability(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	getRunnerCalls := 0
	runners := &fakeRunnersClient{
		getRunner: func(_ context.Context, req *runnersv1.GetRunnerRequest, _ ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
			getRunnerCalls++
			return &runnersv1.GetRunnerResponse{Runner: buildRunner(req.GetId())}, nil
		},
		updateWorkload: func(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			t.Fatal("expected no record write on a runner that cannot hibernate")
			return nil, nil
		},
	}
	runner := &fakeRunnerClient{hibernateWorkload: func(context.Context, *runnerv1.HibernateWorkloadRequest, ...grpc.CallOption) (*runnerv1.HibernateWorkloadResponse, error) {
		t.Fatal("expected no hibernate call on a runner that cannot hibernate")
		return nil, nil
	}}
	agents := &testutil.FakeAgentsClient{GetUnackedInboxCountFunc: func(context.Context, *agentsv1.GetUnackedInboxCountRequest, ...grpc.CallOption) (*agentsv1.GetUnackedInboxCountResponse, error) {
		t.Fatal("expected no inbox read on a runner that cannot hibernate")
		return nil, nil
	}}
	reconciler := newTestReconciler(Config{
		Runners:      runners,
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Agents:       agents,
	})
	capable := map[string]bool{}
	for i := 0; i < 2; i++ {
		hibernated, err := reconciler.hibernateIdleWorkload(ctx, idleAgentWorkload(uuid.New(), uuid.New(), "runner-1", now.Add(-time.Minute)), capable, now)
		if err != nil {
			t.Fatalf("hibernate idle workload: %v", err)
		}
		if hibernated {
			t.Fatal("expected no hibernation on a runner that cannot")
		}
	}
	if getRunnerCalls != 1 {
		t.Fatalf("expected the runner to be asked once per cycle, got %d", getRunnerCalls)
	}
}

// The flow, in order: the exposure check, the inbox check immediately before
// the runner call, UpdateWorkload(hibernating), HibernateWorkload, and the
// pull credential revoked once the state is durable.
func TestHibernateWorkloadFlow(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := uuid.New()
	instanceID := uuid.New()
	workload := idleAgentWorkload(agentID, instanceID, "runner-1", now.Add(-time.Minute))
	workloadID := workload.GetMeta().GetId()

	var calls []string
	runners := &fakeRunnersClient{
		getRunner: func(_ context.Context, req *runnersv1.GetRunnerRequest, _ ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
			return &runnersv1.GetRunnerResponse{Runner: hibernatingRunner(req.GetId())}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			if req.GetId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			calls = append(calls, "update:"+req.GetStatus().String())
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}
	runner := &fakeRunnerClient{hibernateWorkload: func(_ context.Context, req *runnerv1.HibernateWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.HibernateWorkloadResponse, error) {
		if req.GetWorkloadId() != workloadID {
			return nil, errors.New("unexpected runner workload id")
		}
		calls = append(calls, "hibernate")
		return &runnerv1.HibernateWorkloadResponse{}, nil
	}}
	agents := &testutil.FakeAgentsClient{GetUnackedInboxCountFunc: func(ctx context.Context, req *agentsv1.GetUnackedInboxCountRequest, _ ...grpc.CallOption) (*agentsv1.GetUnackedInboxCountResponse, error) {
		if req.GetAgentInstanceId() != instanceID.String() {
			return nil, errors.New("unexpected agent instance id")
		}
		// Read as the platform: Agents admits cluster admin on the inbox reads
		// for exactly this.
		metadataValues, _ := metadata.FromOutgoingContext(ctx)
		if identities := metadataValues.Get(identityMetadataKey); len(identities) != 1 || identities[0] != testPlatformIdentityID.String() {
			return nil, errors.New("expected the platform identity on the inbox read")
		}
		calls = append(calls, "inbox")
		return &agentsv1.GetUnackedInboxCountResponse{Count: 0}, nil
	}}
	expose := &fakeExposeClient{listExposures: func(_ context.Context, req *exposev1.ListExposuresRequest, _ ...grpc.CallOption) (*exposev1.ListExposuresResponse, error) {
		if req.GetWorkloadId() != workloadID {
			return nil, errors.New("unexpected workload id on the exposure listing")
		}
		calls = append(calls, "exposures")
		return &exposev1.ListExposuresResponse{}, nil
	}}
	proxy := &fakeImageProxyClient{revoke: func(_ context.Context, req *imageproxyv1.RevokePullCredentialRequest, _ ...grpc.CallOption) (*imageproxyv1.RevokePullCredentialResponse, error) {
		if req.GetWorkloadId() != workloadID {
			return nil, errors.New("unexpected workload id on the revocation")
		}
		calls = append(calls, "revoke")
		return &imageproxyv1.RevokePullCredentialResponse{}, nil
	}}
	reconciler := newTestReconciler(Config{
		Runners:      runners,
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Agents:       agents,
		Expose:       expose,
	}).WithImageProxy(proxy, "registry.agyn.test")

	hibernated, err := reconciler.hibernateIdleWorkload(ctx, workload, map[string]bool{}, now)
	if err != nil {
		t.Fatalf("hibernate idle workload: %v", err)
	}
	if !hibernated {
		t.Fatal("expected the workload to be hibernated")
	}
	if !reflect.DeepEqual(calls, []string{"exposures", "inbox", "update:WORKLOAD_STATUS_HIBERNATING", "hibernate", "revoke"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
	if workload.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING {
		t.Fatalf("expected the record to read hibernating, got %s", workload.GetStatus())
	}
}

// An item landed since the record was read: the workload stays awake and
// nothing is written. The next cycle finds the inbox non-empty and leaves it.
func TestHibernateWorkloadKeptAwakeByInbox(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	workload := idleAgentWorkload(uuid.New(), uuid.New(), "runner-1", now.Add(-time.Minute))
	runners := &fakeRunnersClient{
		getRunner: func(_ context.Context, req *runnersv1.GetRunnerRequest, _ ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
			return &runnersv1.GetRunnerResponse{Runner: hibernatingRunner(req.GetId())}, nil
		},
		updateWorkload: func(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			t.Fatal("expected no record write with work waiting")
			return nil, nil
		},
	}
	runner := &fakeRunnerClient{hibernateWorkload: func(context.Context, *runnerv1.HibernateWorkloadRequest, ...grpc.CallOption) (*runnerv1.HibernateWorkloadResponse, error) {
		t.Fatal("expected no hibernate call with work waiting")
		return nil, nil
	}}
	agents := &testutil.FakeAgentsClient{GetUnackedInboxCountFunc: func(context.Context, *agentsv1.GetUnackedInboxCountRequest, ...grpc.CallOption) (*agentsv1.GetUnackedInboxCountResponse, error) {
		return &agentsv1.GetUnackedInboxCountResponse{Count: 1}, nil
	}}
	reconciler := newTestReconciler(Config{
		Runners:      runners,
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Agents:       agents,
	})
	outcome, err := reconciler.hibernateWorkload(ctx, workload, now)
	if err != nil {
		t.Fatalf("hibernate workload: %v", err)
	}
	if outcome != hibernateKeptAwakeByInbox {
		t.Fatalf("expected the inbox to keep the workload awake, got %v", outcome)
	}
	if workload.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		t.Fatalf("expected the record to stay running, got %s", workload.GetStatus())
	}
}

// An exposed port is a standing request to be reachable, and a frozen guest is
// not. A provisioning exposure is the same request not yet served; a removing
// one is no longer standing.
func TestHibernateWorkloadKeptAwakeByExposure(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	for _, testCase := range []struct {
		name     string
		status   exposev1.ExposureStatus
		expected hibernateOutcome
	}{
		{name: "active", status: exposev1.ExposureStatus_EXPOSURE_STATUS_ACTIVE, expected: hibernateKeptAwakeByExposure},
		{name: "provisioning", status: exposev1.ExposureStatus_EXPOSURE_STATUS_PROVISIONING, expected: hibernateKeptAwakeByExposure},
		{name: "removing", status: exposev1.ExposureStatus_EXPOSURE_STATUS_REMOVING, expected: hibernatedWorkload},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			workload := idleAgentWorkload(uuid.New(), uuid.New(), "runner-1", now.Add(-time.Minute))
			inboxRead := false
			runners := &fakeRunnersClient{updateWorkload: func(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
				return &runnersv1.UpdateWorkloadResponse{}, nil
			}}
			runner := &fakeRunnerClient{hibernateWorkload: func(context.Context, *runnerv1.HibernateWorkloadRequest, ...grpc.CallOption) (*runnerv1.HibernateWorkloadResponse, error) {
				return &runnerv1.HibernateWorkloadResponse{}, nil
			}}
			agents := &testutil.FakeAgentsClient{GetUnackedInboxCountFunc: func(context.Context, *agentsv1.GetUnackedInboxCountRequest, ...grpc.CallOption) (*agentsv1.GetUnackedInboxCountResponse, error) {
				inboxRead = true
				return &agentsv1.GetUnackedInboxCountResponse{}, nil
			}}
			expose := &fakeExposeClient{listExposures: func(context.Context, *exposev1.ListExposuresRequest, ...grpc.CallOption) (*exposev1.ListExposuresResponse, error) {
				return &exposev1.ListExposuresResponse{Exposures: []*exposev1.Exposure{{Status: testCase.status}}}, nil
			}}
			reconciler := newTestReconciler(Config{
				Runners:      runners,
				RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
				Agents:       agents,
				Expose:       expose,
			})
			outcome, err := reconciler.hibernateWorkload(ctx, workload, now)
			if err != nil {
				t.Fatalf("hibernate workload: %v", err)
			}
			if outcome != testCase.expected {
				t.Fatalf("expected outcome %v, got %v", testCase.expected, outcome)
			}
			if kept := outcome == hibernateKeptAwakeByExposure; kept == inboxRead {
				t.Fatalf("expected the inbox read only once the exposure check cleared; kept awake=%v, inbox read=%v", kept, inboxRead)
			}
		})
	}
}

// The runner could not persist it: the record reads running again, nothing
// terminal is written, the credential stays, and the next attempt waits
// hibernateRetry. The idle timeout remains the stop of last resort meanwhile.
func TestHibernateWorkloadFailureLeavesRunningAndWaits(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	workload := idleAgentWorkload(uuid.New(), uuid.New(), "runner-1", now.Add(-time.Minute))

	var calls []string
	runners := &fakeRunnersClient{
		getRunner: func(_ context.Context, req *runnersv1.GetRunnerRequest, _ ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
			return &runnersv1.GetRunnerResponse{Runner: hibernatingRunner(req.GetId())}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			if req.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED || req.GetRemovedAt() != nil {
				return nil, errors.New("a failed hibernation must record nothing terminal")
			}
			calls = append(calls, "update:"+req.GetStatus().String())
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}
	runner := &fakeRunnerClient{hibernateWorkload: func(context.Context, *runnerv1.HibernateWorkloadRequest, ...grpc.CallOption) (*runnerv1.HibernateWorkloadResponse, error) {
		calls = append(calls, "hibernate")
		return nil, status.Error(codes.Internal, "snapshot store unavailable")
	}}
	proxy := &fakeImageProxyClient{revoke: func(context.Context, *imageproxyv1.RevokePullCredentialRequest, ...grpc.CallOption) (*imageproxyv1.RevokePullCredentialResponse, error) {
		t.Fatal("the credential of a workload still running must not be revoked")
		return nil, nil
	}}
	reconciler := newTestReconciler(Config{
		Runners:      runners,
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Agents:       &testutil.FakeAgentsClient{},
	}).WithImageProxy(proxy, "registry.agyn.test")

	hibernated, err := reconciler.hibernateIdleWorkload(ctx, workload, map[string]bool{}, now)
	if err == nil || hibernated {
		t.Fatalf("expected the failure to surface, got hibernated=%v err=%v", hibernated, err)
	}
	if !reflect.DeepEqual(calls, []string{"update:WORKLOAD_STATUS_HIBERNATING", "hibernate", "update:WORKLOAD_STATUS_RUNNING"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
	if workload.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		t.Fatalf("expected the record to stay running, got %s", workload.GetStatus())
	}

	// Within the window nothing is tried; after it the attempt is made again.
	calls = nil
	outcome, err := reconciler.hibernateWorkload(ctx, workload, now.Add(hibernateRetry-time.Second))
	if err != nil || outcome != hibernateDeferred {
		t.Fatalf("expected a deferred retry within the window, got outcome=%v err=%v", outcome, err)
	}
	if len(calls) != 0 {
		t.Fatalf("expected no calls within the retry window, got %v", calls)
	}
	if _, err := reconciler.hibernateWorkload(ctx, workload, now.Add(hibernateRetry+time.Second)); err == nil {
		t.Fatal("expected the retried hibernation to fail the same way")
	}
	if len(calls) == 0 || calls[0] != "update:WORKLOAD_STATUS_HIBERNATING" {
		t.Fatalf("expected the attempt to be made again after the window, got %v", calls)
	}
}

// A sandbox has no inbox, and its layout snapshot is taken before the pause,
// for the stop that may follow: a hibernated sandbox that reaches its TTL is
// stopped without ever being thawed, and a frozen container cannot be read.
func TestHibernateWorkloadSnapshotsSandboxShellsBeforeThePause(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	workloadID := uuid.NewString()
	workload := &runnersv1.Workload{
		Meta:             &runnersv1.EntityMeta{Id: workloadID, CreatedAt: timestamppb.New(now.Add(-time.Hour))},
		RunnerId:         "runner-1",
		OrganizationId:   testOrganizationID,
		OwnerKind:        runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:          uuid.NewString(),
		Status:           runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		InstanceId:       stringPtr(workloadID),
		PersistentShells: true,
		LastActivityAt:   timestamppb.New(now.Add(-2 * time.Hour)),
	}
	var calls []string
	runners := &fakeRunnersClient{updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
		calls = append(calls, "update:"+req.GetStatus().String())
		return &runnersv1.UpdateWorkloadResponse{}, nil
	}}
	runner := &fakeRunnerClient{
		exec: func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[runnerv1.ExecRequest, runnerv1.ExecResponse], error) {
			calls = append(calls, "exec")
			// Best-effort: a snapshot that cannot be read never blocks the pause.
			return nil, errors.New("exec unavailable")
		},
		hibernateWorkload: func(context.Context, *runnerv1.HibernateWorkloadRequest, ...grpc.CallOption) (*runnerv1.HibernateWorkloadResponse, error) {
			calls = append(calls, "hibernate")
			return &runnerv1.HibernateWorkloadResponse{}, nil
		},
	}
	agents := &testutil.FakeAgentsClient{GetUnackedInboxCountFunc: func(context.Context, *agentsv1.GetUnackedInboxCountRequest, ...grpc.CallOption) (*agentsv1.GetUnackedInboxCountResponse, error) {
		t.Fatal("a sandbox has no inbox to check")
		return nil, nil
	}}
	reconciler := newTestReconciler(Config{
		Runners:      runners,
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Agents:       agents,
	})
	outcome, err := reconciler.hibernateWorkload(ctx, workload, now)
	if err != nil || outcome != hibernatedWorkload {
		t.Fatalf("expected the sandbox to be hibernated, got outcome=%v err=%v", outcome, err)
	}
	if !reflect.DeepEqual(calls, []string{"update:WORKLOAD_STATUS_HIBERNATING", "exec", "hibernate"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

// The wake: a fresh credential for the same images the start resolved,
// UpdateWorkload(waking), then WakeWorkload carrying it. The record is left for
// the runner's report, or reconciliation, to move to running.
func TestWakeWorkloadFlow(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := uuid.New()
	workload := hibernatedAgentWorkload(agentID, uuid.New(), "runner-1", now.Add(-time.Hour))
	workloadID := workload.GetMeta().GetId()

	var calls []string
	proxy := &fakeImageProxyClient{mint: func(_ context.Context, req *imageproxyv1.MintPullCredentialRequest, _ ...grpc.CallOption) (*imageproxyv1.MintPullCredentialResponse, error) {
		if req.GetWorkloadId() != workloadID {
			return nil, errors.New("unexpected workload id on the mint")
		}
		if req.GetOrganizationId() != testOrganizationID {
			return nil, errors.New("unexpected organization on the mint")
		}
		if !reflect.DeepEqual(req.GetImageIds(), []string{testAssemblerRuntimeImageID}) {
			return nil, errors.New("expected the credential scoped to the images the start resolved")
		}
		calls = append(calls, "mint")
		return &imageproxyv1.MintPullCredentialResponse{Username: "user", Password: "pass"}, nil
	}}
	runners := &fakeRunnersClient{updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
		if req.GetId() != workloadID {
			return nil, errors.New("unexpected workload id")
		}
		calls = append(calls, "update:"+req.GetStatus().String())
		return &runnersv1.UpdateWorkloadResponse{}, nil
	}}
	runner := &fakeRunnerClient{wakeWorkload: func(_ context.Context, req *runnerv1.WakeWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.WakeWorkloadResponse, error) {
		if req.GetWorkloadId() != workloadID {
			return nil, errors.New("unexpected runner workload id")
		}
		credentials := req.GetImagePullCredentials()
		if len(credentials) != 1 || credentials[0].GetRegistry() != testAssemblerProxyHost || credentials[0].GetUsername() != "user" || credentials[0].GetPassword() != "pass" {
			return nil, errors.New("expected the wake to carry the fresh credential")
		}
		calls = append(calls, "wake")
		return &runnerv1.WakeWorkloadResponse{}, nil
	}}
	reconciler := newTestReconciler(Config{
		Runners:      runners,
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Assembler:    newTestAssembler(agentID, false),
	}).WithImageProxy(proxy, testAssemblerProxyHost)

	if err := reconciler.wakeWorkload(ctx, workload); err != nil {
		t.Fatalf("wake workload: %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"mint", "update:WORKLOAD_STATUS_WAKING", "wake"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
	if workload.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING {
		t.Fatalf("expected the record to read waking, got %s", workload.GetStatus())
	}
}

// WakeWorkload failing marks the workload failed with wake_failed, discards
// the persisted state, and releases the identity and the credential just
// minted; the next tick cold-starts the instance on its volumes.
func TestWakeWorkloadFailureMarksWakeFailed(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	workload := hibernatedAgentWorkload(uuid.New(), uuid.New(), "runner-1", now.Add(-time.Hour))
	workloadID := workload.GetMeta().GetId()

	var calls []string
	var failed *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
		calls = append(calls, "update:"+req.GetStatus().String())
		if req.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
			failed = req
		}
		return &runnersv1.UpdateWorkloadResponse{}, nil
	}}
	runner := &fakeRunnerClient{
		wakeWorkload: func(context.Context, *runnerv1.WakeWorkloadRequest, ...grpc.CallOption) (*runnerv1.WakeWorkloadResponse, error) {
			calls = append(calls, "wake")
			return nil, status.Error(codes.Internal, "restore failed")
		},
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			if req.GetWorkloadId() != workloadID {
				return nil, errors.New("unexpected workload id on the discard")
			}
			calls = append(calls, "stop")
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	zitiMgmt := &fakeZitiMgmtClient{deleteIdentity: func(_ context.Context, req *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
		if req.GetZitiIdentityId() != workload.GetZitiIdentityId() {
			return nil, errors.New("unexpected identity")
		}
		calls = append(calls, "delete-identity")
		return &zitimgmtv1.DeleteIdentityResponse{}, nil
	}}
	proxy := &fakeImageProxyClient{
		mint: func(context.Context, *imageproxyv1.MintPullCredentialRequest, ...grpc.CallOption) (*imageproxyv1.MintPullCredentialResponse, error) {
			calls = append(calls, "mint")
			return &imageproxyv1.MintPullCredentialResponse{Username: "user", Password: "pass"}, nil
		},
		revoke: func(context.Context, *imageproxyv1.RevokePullCredentialRequest, ...grpc.CallOption) (*imageproxyv1.RevokePullCredentialResponse, error) {
			calls = append(calls, "revoke")
			return &imageproxyv1.RevokePullCredentialResponse{}, nil
		},
	}
	agentID := uuid.MustParse(workload.GetAgentClassId())
	reconciler := newTestReconciler(Config{
		Runners:      runners,
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		ZitiMgmt:     zitiMgmt,
		Assembler:    newTestAssembler(agentID, false),
	}).WithImageProxy(proxy, testAssemblerProxyHost)

	if err := reconciler.wakeWorkload(ctx, workload); err == nil {
		t.Fatal("expected the failed wake to surface")
	}
	if !reflect.DeepEqual(calls, []string{"mint", "update:WORKLOAD_STATUS_WAKING", "wake", "update:WORKLOAD_STATUS_FAILED", "stop", "revoke", "delete-identity"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
	if failed.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_WAKE_FAILED {
		t.Fatalf("expected wake_failed, got %s", failed.GetFailureReason())
	}
	if failed.GetRemovedAt() == nil {
		t.Fatal("expected removed_at on the failed record")
	}
}

// An instance with unacked items whose live workload is asleep gets a wake,
// not a start.
func TestShouldStartWorkloadWakesHibernatedWorkload(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := uuid.New()
	instanceID := uuid.New()
	asleep := hibernatedAgentWorkload(agentID, instanceID, "runner-1", now.Add(-time.Hour))
	fixture := startDecisionFixture{t: t, agentInstanceID: instanceID, active: []*runnersv1.Workload{asleep}}

	woken := false
	runners := &fakeRunnersClient{
		listWorkloadsByAgentInstance: fixture.list,
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			if req.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING {
				return nil, errors.New("expected only the waking write")
			}
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}
	runner := &fakeRunnerClient{wakeWorkload: func(context.Context, *runnerv1.WakeWorkloadRequest, ...grpc.CallOption) (*runnerv1.WakeWorkloadResponse, error) {
		woken = true
		return &runnerv1.WakeWorkloadResponse{}, nil
	}}
	reconciler := newTestReconciler(Config{
		Runners:      runners,
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
	})
	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: instanceID}, now, map[uuid.UUID]time.Time{})
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if shouldStart {
		t.Fatal("expected no start while a workload is asleep")
	}
	if !woken {
		t.Fatal("expected the sleeping workload to be woken")
	}
}

// A hibernated workload is stopped whenever its instance is paused or
// terminated, since nothing will ever wake it; one whose instance is active
// sleeps on.
func TestStopHibernatedIfInstanceGone(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	for _, testCase := range []struct {
		name     string
		state    agentsv1.AgentInstanceState
		err      error
		expected bool
	}{
		{name: "active", state: agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_ACTIVE, expected: false},
		{name: "paused", state: agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_PAUSED, expected: true},
		{name: "terminated", state: agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_TERMINATED, expected: true},
		{name: "gone", err: status.Error(codes.NotFound, "no such instance"), expected: true},
		{name: "unreadable", err: errors.New("agents is down"), expected: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			instanceID := uuid.New()
			workload := hibernatedAgentWorkload(uuid.New(), instanceID, "runner-1", now.Add(-time.Hour))
			stopped := false
			runners := &fakeRunnersClient{updateWorkload: func(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
				return &runnersv1.UpdateWorkloadResponse{}, nil
			}}
			runner := &fakeRunnerClient{stopWorkload: func(context.Context, *runnerv1.StopWorkloadRequest, ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
				stopped = true
				return &runnerv1.StopWorkloadResponse{}, nil
			}}
			agents := &testutil.FakeAgentsClient{GetInstanceFunc: func(_ context.Context, req *agentsv1.GetInstanceRequest, _ ...grpc.CallOption) (*agentsv1.GetInstanceResponse, error) {
				if req.GetId() != instanceID.String() {
					return nil, errors.New("unexpected instance id")
				}
				if testCase.err != nil {
					return nil, testCase.err
				}
				return &agentsv1.GetInstanceResponse{Instance: &agentsv1.AgentInstance{Meta: &agentsv1.EntityMeta{Id: instanceID.String()}, State: testCase.state}}, nil
			}}
			reconciler := newTestReconciler(Config{
				Runners:      runners,
				RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
				Agents:       agents,
			})
			if got := reconciler.stopHibernatedIfInstanceGone(ctx, workload); got != testCase.expected || stopped != testCase.expected {
				t.Fatalf("expected stopped=%v, got decision=%v stop call=%v", testCase.expected, got, stopped)
			}
		})
	}
}

// A hibernated workload stops through the same sequence as a running one, with
// two things left out: the layout snapshot, because a frozen container cannot
// be read, and the credential revocation, because the credential went when the
// workload fell asleep. The identity goes as it does for any stop.
func TestStopHibernatedWorkloadSkipsSnapshotAndRevocation(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	workloadID := uuid.NewString()
	workload := &runnersv1.Workload{
		Meta:             &runnersv1.EntityMeta{Id: workloadID, CreatedAt: timestamppb.New(now.Add(-time.Hour))},
		RunnerId:         "runner-1",
		OrganizationId:   testOrganizationID,
		OwnerKind:        runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:          uuid.NewString(),
		Status:           runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED,
		HibernatedAt:     timestamppb.New(now.Add(-30 * time.Minute)),
		InstanceId:       stringPtr(workloadID),
		PersistentShells: true,
		ZitiIdentityId:   "ziti-sandbox",
	}
	var calls []string
	runners := &fakeRunnersClient{updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
		calls = append(calls, "update:"+req.GetStatus().String())
		return &runnersv1.UpdateWorkloadResponse{}, nil
	}}
	runner := &fakeRunnerClient{
		exec: func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[runnerv1.ExecRequest, runnerv1.ExecResponse], error) {
			t.Fatal("a frozen container must not be read")
			return nil, nil
		},
		stopWorkload: func(context.Context, *runnerv1.StopWorkloadRequest, ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	zitiMgmt := &fakeZitiMgmtClient{deleteIdentity: func(context.Context, *zitimgmtv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
		calls = append(calls, "delete-identity")
		return &zitimgmtv1.DeleteIdentityResponse{}, nil
	}}
	proxy := &fakeImageProxyClient{revoke: func(context.Context, *imageproxyv1.RevokePullCredentialRequest, ...grpc.CallOption) (*imageproxyv1.RevokePullCredentialResponse, error) {
		t.Fatal("a hibernated workload has no credential left to revoke")
		return nil, nil
	}}
	reconciler := newTestReconciler(Config{
		Runners:      runners,
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		ZitiMgmt:     zitiMgmt,
	}).WithImageProxy(proxy, "registry.agyn.test")

	if err := reconciler.stopSandboxWorkload(ctx, workload); err != nil {
		t.Fatalf("stop sandbox workload: %v", err)
	}
	// The sandbox stop deletes the identity once more after the stop flow has;
	// that is how it was, and only the flow's own sequence is asserted here.
	if len(calls) < 4 || !reflect.DeepEqual(calls[:4], []string{"update:WORKLOAD_STATUS_STOPPING", "stop", "update:WORKLOAD_STATUS_STOPPED", "delete-identity"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

// A hibernated workload's identity is in use the moment it wakes, so the sweep
// counts it as live and leaves it.
func TestReconcileOrphanIdentitiesKeepsHibernatedWorkloads(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	asleep := hibernatedAgentWorkload(uuid.New(), uuid.New(), "runner-1", now.Add(-time.Hour))
	runners := &fakeRunnersClient{listWorkloads: func(_ context.Context, req *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
		if !matchStatuses(req.GetFilter().GetStatusIn(), liveWorkloadStatuses) {
			return nil, errors.New("expected every live status, the sleeping ones included")
		}
		return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{asleep}}, nil
	}}
	var deleted []string
	zitiMgmt := &fakeZitiMgmtClient{
		listManagedIdentities: func(context.Context, *zitimgmtv1.ListManagedIdentitiesRequest, ...grpc.CallOption) (*zitimgmtv1.ListManagedIdentitiesResponse, error) {
			return &zitimgmtv1.ListManagedIdentitiesResponse{Identities: []*zitimgmtv1.ManagedIdentity{
				{ZitiIdentityId: asleep.GetZitiIdentityId()},
				{ZitiIdentityId: "orphan"},
			}}, nil
		},
		deleteIdentity: func(_ context.Context, req *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			deleted = append(deleted, req.GetZitiIdentityId())
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}
	reconciler := newTestReconciler(Config{Runners: runners, ZitiMgmt: zitiMgmt})
	if err := reconciler.reconcileOrphanIdentities(ctx); err != nil {
		t.Fatalf("reconcile orphan identities: %v", err)
	}
	// The agent sweep and the sandbox sweep each see the orphan once.
	if !reflect.DeepEqual(deleted, []string{"orphan", "orphan"}) {
		t.Fatalf("expected only the orphan deleted, got %v", deleted)
	}
}

// The owner's volume TTL clock stays stopped while its workload sleeps.
func TestAgentInstanceActivityCountsHibernatedAsLive(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	instanceID := uuid.New()
	asleep := hibernatedAgentWorkload(uuid.New(), instanceID, "runner-1", now.Add(-time.Hour))
	runners := &fakeRunnersClient{listWorkloadsByAgentInstance: func(context.Context, *runnersv1.ListWorkloadsByAgentInstanceRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByAgentInstanceResponse, error) {
		return &runnersv1.ListWorkloadsByAgentInstanceResponse{Workloads: []*runnersv1.Workload{asleep}}, nil
	}}
	reconciler := newTestReconciler(Config{Runners: runners})
	activity, err := reconciler.agentInstanceActivity(ctx, instanceID.String(), map[string]instanceActivity{})
	if err != nil {
		t.Fatalf("agent instance activity: %v", err)
	}
	if !activity.hasActive {
		t.Fatal("expected a hibernated workload to count as live")
	}
}
