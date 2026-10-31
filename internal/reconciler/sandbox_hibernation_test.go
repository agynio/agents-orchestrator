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
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// sandboxHarness is one sandbox with one workload on one runner, and what each
// side was told.
type sandboxHarness struct {
	sandbox    *agentsv1.Sandbox
	workload   *runnersv1.Workload
	runner     *fakeRunnerClient
	runners    *fakeRunnersClient
	calls      []string
	runtime    []*agentsv1.UpdateSandboxRuntimeStateRequest
	reconciler *Reconciler
}

func newSandboxHarness(t *testing.T, sandboxStatus agentsv1.SandboxStatus, workloadStatus runnersv1.WorkloadStatus, hibernates bool) *sandboxHarness {
	t.Helper()
	now := time.Now().UTC()
	runnerID := "runner-1"
	sandboxID := uuid.NewString()
	workloadID := uuid.NewString()
	h := &sandboxHarness{}
	h.workload = &runnersv1.Workload{
		Meta:             &runnersv1.EntityMeta{Id: workloadID, CreatedAt: timestamppb.New(now.Add(-3 * time.Hour))},
		RunnerId:         runnerID,
		OrganizationId:   testOrganizationID,
		Status:           workloadStatus,
		InstanceId:       stringPtr(workloadID),
		LastActivityAt:   timestamppb.New(now.Add(-2 * time.Hour)),
		OwnerKind:        runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:          sandboxID,
		PersistentShells: true,
		ZitiIdentityId:   "ziti-sandbox",
	}
	if isFrozenWorkloadStatus(workloadStatus) {
		h.workload.HibernatedAt = timestamppb.New(now.Add(-time.Hour))
	}
	h.sandbox = &agentsv1.Sandbox{
		Meta:           &agentsv1.EntityMeta{Id: sandboxID, CreatedAt: timestamppb.New(now.Add(-3 * time.Hour))},
		OrganizationId: testOrganizationID,
		OwnerId:        uuid.NewString(),
		EnvironmentId:  testAssemblerEnvironmentID,
		Status:         sandboxStatus,
		IdleTimeout:    "1h",
		Ttl:            "72h",
		WorkloadId:     stringPtr(workloadID),
	}
	runner := buildRunner(runnerID)
	if hibernates {
		runner = hibernatingRunner(runnerID)
	}
	h.runners = &fakeRunnersClient{
		getRunner: func(context.Context, *runnersv1.GetRunnerRequest, ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
			return &runnersv1.GetRunnerResponse{Runner: runner}, nil
		},
		listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			if h.workload == nil {
				return &runnersv1.ListWorkloadsResponse{}, nil
			}
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{h.workload}}, nil
		},
		listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			if req.Status != nil {
				h.calls = append(h.calls, "update:"+req.GetStatus().String())
			}
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}
	h.runner = &fakeRunnerClient{
		inspectWorkload: func(context.Context, *runnerv1.InspectWorkloadRequest, ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			h.calls = append(h.calls, "inspect")
			return &runnerv1.InspectWorkloadResponse{
				StateRunning: true,
				Status:       runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
				Containers:   []*runnerv1.WorkloadContainer{{Name: "sandbox", Role: runnerv1.ContainerRole_CONTAINER_ROLE_MAIN, Status: runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING}},
			}, nil
		},
		exec: func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[runnerv1.ExecRequest, runnerv1.ExecResponse], error) {
			h.calls = append(h.calls, "exec")
			return nil, errors.New("exec unavailable")
		},
		hibernateWorkload: func(context.Context, *runnerv1.HibernateWorkloadRequest, ...grpc.CallOption) (*runnerv1.HibernateWorkloadResponse, error) {
			h.calls = append(h.calls, "hibernate")
			return &runnerv1.HibernateWorkloadResponse{}, nil
		},
		wakeWorkload: func(context.Context, *runnerv1.WakeWorkloadRequest, ...grpc.CallOption) (*runnerv1.WakeWorkloadResponse, error) {
			h.calls = append(h.calls, "wake")
			return &runnerv1.WakeWorkloadResponse{}, nil
		},
		stopWorkload: func(context.Context, *runnerv1.StopWorkloadRequest, ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			h.calls = append(h.calls, "stop")
			return &runnerv1.StopWorkloadResponse{}, nil
		},
		startWorkload: func(context.Context, *runnerv1.StartWorkloadRequest, ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error) {
			h.calls = append(h.calls, "start")
			return nil, errors.New("no start expected")
		},
	}
	agents := &testutil.FakeAgentsClient{UpdateSandboxRuntimeStateFunc: func(_ context.Context, req *agentsv1.UpdateSandboxRuntimeStateRequest, _ ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
		h.runtime = append(h.runtime, req)
		return &agentsv1.UpdateSandboxRuntimeStateResponse{}, nil
	}}
	h.reconciler = newTestReconciler(Config{
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return h.runner, nil }},
		Runners:      h.runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	return h
}

func (h *sandboxHarness) reconcile(t *testing.T) {
	t.Helper()
	if err := h.reconciler.reconcileSandbox(context.Background(), h.sandbox, time.Now().UTC()); err != nil {
		t.Fatalf("reconcile sandbox: %v", err)
	}
}

func (h *sandboxHarness) lastRuntime(t *testing.T) *agentsv1.UpdateSandboxRuntimeStateRequest {
	t.Helper()
	if len(h.runtime) == 0 {
		t.Fatal("expected a sandbox runtime update")
	}
	return h.runtime[len(h.runtime)-1]
}

// Where the k8s-runner path would stop an idle sandbox, a runner that can
// hibernate puts it to sleep instead, after the layout snapshot; the sandbox
// shows hibernated and keeps the workload the next connect wakes.
func TestReconcileSandboxIdleHibernatesOnCapableRunner(t *testing.T) {
	h := newSandboxHarness(t, agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING, runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, true)
	h.reconcile(t)
	if !reflect.DeepEqual(h.calls, []string{"inspect", "update:WORKLOAD_STATUS_HIBERNATING", "exec", "hibernate"}) {
		t.Fatalf("unexpected call order: %v", h.calls)
	}
	runtime := h.lastRuntime(t)
	if runtime.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_HIBERNATED {
		t.Fatalf("expected the sandbox hibernated, got %s", runtime.GetStatus())
	}
	if runtime.GetWorkloadId() != h.workload.GetMeta().GetId() || runtime.GetClearWorkloadId() {
		t.Fatalf("expected the workload kept on the sandbox, got %v", runtime)
	}
}

// An exposed port keeps a sandbox awake, and the idle timeout then governs it
// as on a runner that cannot hibernate: it is stopped.
func TestReconcileSandboxIdleStopsWhenExposed(t *testing.T) {
	h := newSandboxHarness(t, agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING, runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, true)
	h.reconciler.expose = &fakeExposeClient{listExposures: func(context.Context, *exposev1.ListExposuresRequest, ...grpc.CallOption) (*exposev1.ListExposuresResponse, error) {
		return &exposev1.ListExposuresResponse{Exposures: []*exposev1.Exposure{{Status: exposev1.ExposureStatus_EXPOSURE_STATUS_ACTIVE}}}, nil
	}}
	h.reconcile(t)
	// The stop path as it was: the layout snapshot, then the stop flow.
	if !reflect.DeepEqual(h.calls, []string{"inspect", "exec", "update:WORKLOAD_STATUS_STOPPING", "stop", "update:WORKLOAD_STATUS_STOPPED"}) {
		t.Fatalf("unexpected call order: %v", h.calls)
	}
	runtime := h.lastRuntime(t)
	if runtime.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED || !runtime.GetClearWorkloadId() {
		t.Fatalf("expected the sandbox stopped with its workload cleared, got %v", runtime)
	}
}

// A hibernation that failed recently leaves the sandbox running until the
// retry window passes; it is not stopped in its place.
func TestReconcileSandboxIdleWaitsAfterFailedHibernation(t *testing.T) {
	h := newSandboxHarness(t, agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING, runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, true)
	h.reconciler.hibernation.hibernateFailed(h.workload.GetMeta().GetId(), time.Now().UTC())
	h.reconcile(t)
	if !reflect.DeepEqual(h.calls, []string{"inspect"}) {
		t.Fatalf("expected the sandbox left running, got %v", h.calls)
	}
}

// EnsureSandboxRunning asks for a start; for a hibernated workload the start is
// a wake, never a second workload.
func TestReconcileSandboxStartingWakesHibernatedWorkload(t *testing.T) {
	h := newSandboxHarness(t, agentsv1.SandboxStatus_SANDBOX_STATUS_STARTING, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, true)
	h.reconcile(t)
	if !reflect.DeepEqual(h.calls, []string{"update:WORKLOAD_STATUS_WAKING", "wake"}) {
		t.Fatalf("unexpected call order: %v", h.calls)
	}
}

// A hibernated sandbox sleeps on: no inspection of a paused guest, no stop, no
// second hibernation, however long it has been idle.
func TestReconcileSandboxHibernatedSleepsOn(t *testing.T) {
	h := newSandboxHarness(t, agentsv1.SandboxStatus_SANDBOX_STATUS_HIBERNATED, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, true)
	h.reconcile(t)
	if len(h.calls) != 0 {
		t.Fatalf("expected no runner call, got %v", h.calls)
	}
	if len(h.runtime) != 0 {
		t.Fatalf("expected the sandbox record left as it is, got %v", h.runtime)
	}
}

// A running sandbox whose workload the runner reported asleep follows it.
func TestReconcileSandboxFollowsHibernatedWorkload(t *testing.T) {
	h := newSandboxHarness(t, agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, true)
	h.reconcile(t)
	runtime := h.lastRuntime(t)
	if runtime.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_HIBERNATED || runtime.GetWorkloadId() != h.workload.GetMeta().GetId() {
		t.Fatalf("expected the sandbox hibernated with its workload, got %v", runtime)
	}
}

// A hibernated sandbox whose persisted state is gone is stopped, so the next
// connect restarts it on its volumes.
func TestReconcileSandboxHibernatedWithoutWorkloadStops(t *testing.T) {
	h := newSandboxHarness(t, agentsv1.SandboxStatus_SANDBOX_STATUS_HIBERNATED, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, true)
	h.workload = nil
	h.sandbox.WorkloadId = nil
	h.reconcile(t)
	runtime := h.lastRuntime(t)
	if runtime.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED || !runtime.GetClearWorkloadId() {
		t.Fatalf("expected the sandbox stopped, got %v", runtime)
	}
	if len(h.calls) != 0 {
		t.Fatalf("expected no runner call, got %v", h.calls)
	}
}

// A stopped sandbox with a hibernated workload discards it through the stop
// flow: no layout snapshot of a frozen container, no credential to revoke.
func TestReconcileSandboxStoppedDiscardsHibernatedWorkload(t *testing.T) {
	h := newSandboxHarness(t, agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, true)
	h.reconciler.WithImageProxy(&fakeImageProxyClient{revoke: func(context.Context, *imageproxyv1.RevokePullCredentialRequest, ...grpc.CallOption) (*imageproxyv1.RevokePullCredentialResponse, error) {
		t.Fatal("a hibernated workload has no credential left to revoke")
		return nil, nil
	}}, "registry.agyn.test")
	h.reconcile(t)
	if !reflect.DeepEqual(h.calls, []string{"update:WORKLOAD_STATUS_STOPPING", "stop", "update:WORKLOAD_STATUS_STOPPED"}) {
		t.Fatalf("unexpected call order: %v", h.calls)
	}
	runtime := h.lastRuntime(t)
	if runtime.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED || !runtime.GetClearWorkloadId() {
		t.Fatalf("expected the sandbox stopped with its workload cleared, got %v", runtime)
	}
}
