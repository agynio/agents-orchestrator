package reconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errNoTerminators = errors.New("service runner-1 has no terminators")

func TestRunnerAbsencesLostOnlyAfterStayingAway(t *testing.T) {
	absences := newRunnerAbsences()
	start := time.Now().UTC()
	if absences.lost("runner-1", start) {
		t.Fatal("expected the first sighting to open the absence, not end it")
	}
	if absences.lost("runner-1", start.Add(runnerLostAfter-time.Second)) {
		t.Fatal("expected no verdict inside runnerLostAfter")
	}
	if !absences.lost("runner-1", start.Add(runnerLostAfter)) {
		t.Fatal("expected a verdict once runnerLostAfter has passed")
	}
	if absences.lost("runner-2", start.Add(runnerLostAfter)) {
		t.Fatal("expected each runner to have its own absence")
	}
	absences.answered("runner-1")
	if absences.lost("runner-1", start.Add(2*runnerLostAfter)) {
		t.Fatal("expected an answer to end the absence")
	}
}

func TestRunnerAbsencesNilReceiverDecides(t *testing.T) {
	var absences *runnerAbsences
	absences.answered("runner-1")
	if absences.lost("runner-1", time.Now().UTC()) {
		t.Fatal("expected no verdict without a memory of the absence")
	}
}

// Whatever a workload is doing, a runner with no terminator has said nothing
// about it: the record, the identity and what the runner holds all stay.
func TestReconcileWorkloadsKeepsWorkloadsOfRunnerWithoutTerminators(t *testing.T) {
	for _, recorded := range []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
	} {
		t.Run(recorded.String(), func(t *testing.T) {
			h := newReconcileRowHarness(t, recorded, true, runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, nil)
			h.runner.listWorkloads = func(context.Context, *runnerv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
				return nil, errNoTerminators
			}
			h.reconcile(t)
			// A later tick inside the grace is no different from the first.
			h.reconcile(t)
			if len(h.updates) != 0 {
				t.Fatalf("expected the record left alone, got %v", h.updates)
			}
			if len(h.deleted) != 0 {
				t.Fatalf("expected the identity kept, got %v", h.deleted)
			}
			if h.stops != 0 || h.wakes != 0 {
				t.Fatalf("expected no runner call, got %d stops and %d wakes", h.stops, h.wakes)
			}
		})
	}
}

func TestReconcileWorkloadsFailsHibernatedOnLostRunnerAsSnapshotLost(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, true, runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, nil)
	h.runner.listWorkloads = func(context.Context, *runnerv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
		return nil, errNoTerminators
	}
	h.reconciler.absences.since[h.workload.GetRunnerId()] = time.Now().UTC().Add(-runnerLostAfter)
	h.reconcile(t)
	update := h.singleUpdate(t)
	if update.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED || update.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_SNAPSHOT_LOST {
		t.Fatalf("expected failed with snapshot_lost, got %s %s", update.GetStatus(), update.GetFailureReason())
	}
	if len(h.deleted) != 1 {
		t.Fatalf("expected the identity deleted, got %v", h.deleted)
	}
}

// A runner that answers has ended its absence, so a later loss of its
// terminator starts the wait over rather than finishing the old one.
func TestReconcileWorkloadsAnswerEndsTheAbsence(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, true, runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, nil)
	runnerID := h.workload.GetRunnerId()
	h.reconciler.absences.since[runnerID] = time.Now().UTC().Add(-runnerLostAfter)
	h.reconcile(t)
	if _, ok := h.reconciler.absences.since[runnerID]; ok {
		t.Fatal("expected the answer to end the absence")
	}
	h.runner.listWorkloads = func(context.Context, *runnerv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
		return nil, errNoTerminators
	}
	h.reconcile(t)
	if len(h.updates) != 0 {
		t.Fatalf("expected the record left alone, got %v", h.updates)
	}
}

// Other ways of not answering never count toward the verdict.
func TestReconcileWorkloadsOtherFailureOpensNoAbsence(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, true, runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, nil)
	h.runner.listWorkloads = func(context.Context, *runnerv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
		return nil, status.Error(codes.Unavailable, "connection closed")
	}
	h.reconcile(t)
	if _, ok := h.reconciler.absences.since[h.workload.GetRunnerId()]; ok {
		t.Fatal("expected only a missing terminator to open an absence")
	}
	if len(h.updates) != 0 {
		t.Fatalf("expected the record left alone, got %v", h.updates)
	}
}

// A retried wake the runner did not answer waits for the next pass: failing it
// here would have the runner discard the state it still holds.
func TestReconcileWorkloadsRetriedWakeWithoutAnswerStaysWaking(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING, true, runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, nil)
	h.runner.wakeWorkload = func(context.Context, *runnerv1.WakeWorkloadRequest, ...grpc.CallOption) (*runnerv1.WakeWorkloadResponse, error) {
		h.wakes++
		return nil, status.Error(codes.Unavailable, "connection closed")
	}
	h.reconcile(t)
	if h.wakes != 1 {
		t.Fatalf("expected one retried wake, got %d", h.wakes)
	}
	if len(h.updates) != 0 || h.stops != 0 || len(h.deleted) != 0 {
		t.Fatalf("expected nothing given up, got updates %v, %d stops, deleted %v", h.updates, h.stops, h.deleted)
	}
}

// The main loop's wake: the record says waking, nothing is failed, and the
// runner is not asked to discard anything.
func TestWakeWorkloadWithoutAnswerStaysWaking(t *testing.T) {
	for name, wakeErr := range map[string]error{
		"no terminators": status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial ziti service runner-1: service runner-1 has no terminators"`),
		"connection":     status.Error(codes.Unavailable, "error reading from server: EOF"),
		"wait ran out":   status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			workload := hibernatedAgentWorkload(uuid.New(), uuid.New(), "runner-1", time.Now().UTC().Add(-time.Hour))

			var statuses []runnersv1.WorkloadStatus
			runners := &fakeRunnersClient{updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
				statuses = append(statuses, req.GetStatus())
				return &runnersv1.UpdateWorkloadResponse{}, nil
			}}
			stops := 0
			runner := &fakeRunnerClient{
				wakeWorkload: func(context.Context, *runnerv1.WakeWorkloadRequest, ...grpc.CallOption) (*runnerv1.WakeWorkloadResponse, error) {
					return nil, wakeErr
				},
				stopWorkload: func(context.Context, *runnerv1.StopWorkloadRequest, ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
					stops++
					return &runnerv1.StopWorkloadResponse{}, nil
				},
			}
			agentID := uuid.MustParse(workload.GetAgentClassId())
			reconciler := newTestReconciler(Config{
				Runners:      runners,
				RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
				Assembler:    newTestAssembler(agentID, false),
			})

			if err := reconciler.wakeWorkload(ctx, workload); err == nil {
				t.Fatal("expected the unanswered wake to surface")
			}
			if len(statuses) != 1 || statuses[0] != runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING {
				t.Fatalf("expected only the move to waking, got %v", statuses)
			}
			if stops != 0 {
				t.Fatalf("expected the persisted state kept, got %d stops", stops)
			}
			if workload.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING {
				t.Fatalf("expected the workload waking, got %s", workload.GetStatus())
			}
		})
	}
}

// A stop that cannot reach the runner stays a stop in progress: nothing lost is
// recorded, and the retry comes once the runner answers.
func TestStopWorkloadWithoutTerminatorsRecordsNoLoss(t *testing.T) {
	ctx := context.Background()
	workloadID := "workload-1"
	instanceID := uuid.New().String()

	var statuses []runnersv1.WorkloadStatus
	runners := &fakeRunnersClient{updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
		statuses = append(statuses, req.GetStatus())
		return &runnersv1.UpdateWorkloadResponse{}, nil
	}}
	runner := &fakeRunnerClient{stopWorkload: func(context.Context, *runnerv1.StopWorkloadRequest, ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
		return nil, errNoTerminators
	}}
	reconciler := newTestReconciler(Config{
		Runners:      runners,
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Assembler:    newTestAssembler(uuid.New(), true),
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{
		Meta:            &runnersv1.EntityMeta{Id: workloadID},
		RunnerId:        "runner-1",
		AgentId:         testAgentID,
		AgentInstanceId: stringPtr(testAgentID),
		InstanceId:      stringPtr(instanceID),
		Status:          runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
	})
	if len(statuses) != 1 || statuses[0] != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING {
		t.Fatalf("expected only the move to stopping, got %v", statuses)
	}
}

func TestStopWorkloadDialWithoutTerminatorsLeavesRecord(t *testing.T) {
	var updates int
	runners := &fakeRunnersClient{updateWorkload: func(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
		updates++
		return &runnersv1.UpdateWorkloadResponse{}, nil
	}}
	reconciler := newTestReconciler(Config{
		Runners:      runners,
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return nil, errNoTerminators }},
		Assembler:    newTestAssembler(uuid.New(), true),
	})
	reconciler.stopWorkload(context.Background(), &runnersv1.Workload{
		Meta:            &runnersv1.EntityMeta{Id: "workload-1"},
		RunnerId:        "runner-1",
		AgentId:         testAgentID,
		AgentInstanceId: stringPtr(testAgentID),
		InstanceId:      stringPtr(uuid.New().String()),
		Status:          runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED,
	})
	if updates != 0 {
		t.Fatalf("expected the record left alone, got %d updates", updates)
	}
}

// A deprovisioning volume closes as deleted only once its runner is lost; until
// then the runner still has a disk to remove.
func TestReconcileVolumesClosesDeprovisioningOnlyOnLostRunner(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	threadID := uuid.New().String()

	var updates []*runnersv1.UpdateVolumeRequest
	runners := &fakeRunnersClient{
		listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				{Meta: &runnersv1.EntityMeta{Id: "volume-1"}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(threadID), OrganizationId: testOrganizationID, Status: runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING, ThreadId: threadID, VolumeId: uuid.New().String()},
			}}, nil
		},
		listRunners: func(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolume: func(_ context.Context, req *runnersv1.UpdateVolumeRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error) {
			updates = append(updates, req)
			return &runnersv1.UpdateVolumeResponse{}, nil
		},
	}
	runner := &fakeRunnerClient{listVolumes: func(context.Context, *runnerv1.ListVolumesRequest, ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
		return nil, errNoTerminators
	}}
	reconciler := newTestReconciler(Config{
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Runners:      runners,
		Agents:       &testutil.FakeAgentsClient{},
		Assembler:    newTestAssembler(uuid.New(), false),
	})

	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if len(updates) != 0 {
		t.Fatalf("expected the volume left deprovisioning, got %v", updates)
	}

	reconciler.absences.since[runnerID] = time.Now().UTC().Add(-runnerLostAfter)
	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if len(updates) != 1 || updates[0].GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_DELETED {
		t.Fatalf("expected the volume closed as deleted, got %v", updates)
	}
}
