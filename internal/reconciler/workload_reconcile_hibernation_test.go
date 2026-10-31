package reconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// reconcileRowHarness is one tracked agent workload on one enrolled runner, and
// what the runner says about it: present with a reported status, or absent.
type reconcileRowHarness struct {
	workload   *runnersv1.Workload
	runners    *fakeRunnersClient
	runner     *fakeRunnerClient
	zitiMgmt   *fakeZitiMgmtClient
	updates    []*runnersv1.UpdateWorkloadRequest
	wakes      int
	stops      int
	inspects   int
	deleted    []string
	reconciler *Reconciler
}

func newReconcileRowHarness(t *testing.T, recorded runnersv1.WorkloadStatus, present bool, reported runnerv1.WorkloadStatus, snapshotSize *int64) *reconcileRowHarness {
	t.Helper()
	runnerID := "runner-1"
	workload := hibernatedAgentWorkload(uuid.MustParse(testAgentID), uuid.New(), runnerID, time.Now().UTC().Add(-time.Hour))
	workload.Status = recorded
	if recorded != runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED {
		workload.HibernatedAt = nil
		workload.SnapshotSizeBytes = nil
	}
	workloadID := workload.GetMeta().GetId()
	h := &reconcileRowHarness{workload: workload}
	h.runners = &fakeRunnersClient{
		listWorkloads: func(_ context.Context, req *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			if !matchStatuses(req.GetFilter().GetStatusIn(), liveWorkloadStatuses) {
				return nil, errors.New("expected every live status, the sleeping ones included")
			}
			if len(req.GetFilter().GetOwnerKindIn()) > 0 {
				return &runnersv1.ListWorkloadsResponse{}, nil
			}
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{workload}}, nil
		},
		listRunners: func(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{hibernatingRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			if req.GetId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			h.updates = append(h.updates, req)
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}
	h.runner = &fakeRunnerClient{
		listWorkloads: func(context.Context, *runnerv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			if !present {
				return &runnerv1.ListWorkloadsResponse{}, nil
			}
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{{WorkloadKey: workloadID, InstanceId: workloadID, Status: reported, SnapshotSizeBytes: snapshotSize}}}, nil
		},
		inspectWorkload: func(context.Context, *runnerv1.InspectWorkloadRequest, ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			h.inspects++
			resp := &runnerv1.InspectWorkloadResponse{Status: reported, SnapshotSizeBytes: snapshotSize}
			if reported == runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
				resp.StateRunning = true
				resp.Containers = []*runnerv1.WorkloadContainer{{Name: "agent", Role: runnerv1.ContainerRole_CONTAINER_ROLE_MAIN, Status: runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING}}
			}
			return resp, nil
		},
		wakeWorkload: func(context.Context, *runnerv1.WakeWorkloadRequest, ...grpc.CallOption) (*runnerv1.WakeWorkloadResponse, error) {
			h.wakes++
			return &runnerv1.WakeWorkloadResponse{}, nil
		},
		stopWorkload: func(context.Context, *runnerv1.StopWorkloadRequest, ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			h.stops++
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	h.zitiMgmt = &fakeZitiMgmtClient{deleteIdentity: func(_ context.Context, req *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
		h.deleted = append(h.deleted, req.GetZitiIdentityId())
		return &zitimgmtv1.DeleteIdentityResponse{}, nil
	}}
	h.reconciler = newTestReconciler(Config{
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return h.runner, nil }},
		Runners:      h.runners,
		ZitiMgmt:     h.zitiMgmt,
		Agents:       &testutil.FakeAgentsClient{},
	})
	return h
}

func (h *reconcileRowHarness) reconcile(t *testing.T) {
	t.Helper()
	if err := h.reconciler.reconcileWorkloads(context.Background()); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
}

func (h *reconcileRowHarness) singleUpdate(t *testing.T) *runnersv1.UpdateWorkloadRequest {
	t.Helper()
	if len(h.updates) != 1 {
		t.Fatalf("expected one update, got %d: %v", len(h.updates), h.updates)
	}
	return h.updates[0]
}

// hibernating, reported hibernated: the runner's report was missed, so the
// record takes the sleep from the inspection -- status, when, and the size of
// the persisted state.
func TestReconcileWorkloadsRecordsMissedHibernatedReport(t *testing.T) {
	size := int64(300 << 20)
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING, true, runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, &size)
	h.reconcile(t)
	update := h.singleUpdate(t)
	if update.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED {
		t.Fatalf("expected hibernated, got %s", update.GetStatus())
	}
	if update.GetHibernatedAt() == nil {
		t.Fatal("expected hibernated_at on the record")
	}
	if update.SnapshotSizeBytes == nil || update.GetSnapshotSizeBytes() != size {
		t.Fatalf("expected snapshot size %d, got %v", size, update.SnapshotSizeBytes)
	}
}

// hibernating, still running: the runner could not persist it. The record
// reads running again, and the main loop waits hibernateRetry before trying.
func TestReconcileWorkloadsRevertsHibernatingStillRunning(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING, true, runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, nil)
	h.reconcile(t)
	update := h.singleUpdate(t)
	if update.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		t.Fatalf("expected running, got %s", update.GetStatus())
	}
	if update.GetHibernatedAt() != nil || update.GetRemovedAt() != nil {
		t.Fatal("expected neither a sleep nor an end recorded")
	}
	now := time.Now().UTC()
	workloadID := h.workload.GetMeta().GetId()
	if h.reconciler.hibernation.mayHibernate(workloadID, now) {
		t.Fatal("expected the next hibernation attempt to wait")
	}
	if !h.reconciler.hibernation.mayHibernate(workloadID, now.Add(hibernateRetry+time.Second)) {
		t.Fatal("expected the attempt to be allowed after the retry window")
	}
}

// hibernating, reported hibernating: still on its way, a hibernation is not
// interrupted.
func TestReconcileWorkloadsLeavesHibernatingInProgress(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING, true, runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING, nil)
	h.reconcile(t)
	if len(h.updates) != 0 || h.stops != 0 {
		t.Fatalf("expected no action, got updates=%v stops=%d", h.updates, h.stops)
	}
}

// hibernated, present: no-op -- the TTL is the main loop's to check, and a
// paused guest has nothing to inspect.
func TestReconcileWorkloadsLeavesHibernatedAlone(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, true, runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, nil)
	h.reconcile(t)
	if len(h.updates) != 0 || h.stops != 0 || h.wakes != 0 {
		t.Fatalf("expected no action, got updates=%v stops=%d wakes=%d", h.updates, h.stops, h.wakes)
	}
	if h.inspects != 0 {
		t.Fatalf("expected no inspection of a paused guest, got %d", h.inspects)
	}
}

// hibernated, absent: the runner no longer holds the state. The record fails
// with snapshot_lost, the identity goes, and the instance cold-starts on its
// volumes through the ordinary Start Decision.
func TestReconcileWorkloadsFailsMissingHibernatedAsSnapshotLost(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, false, runnerv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, nil)
	h.reconcile(t)
	update := h.singleUpdate(t)
	if update.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED || update.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_SNAPSHOT_LOST {
		t.Fatalf("expected failed with snapshot_lost, got %s %s", update.GetStatus(), update.GetFailureReason())
	}
	if update.GetRemovedAt() == nil {
		t.Fatal("expected removed_at on the failed record")
	}
	if len(h.deleted) != 1 || h.deleted[0] != h.workload.GetZitiIdentityId() {
		t.Fatalf("expected the identity deleted, got %v", h.deleted)
	}
}

// waking, reported running: the runner's report was missed. The record moves
// to running and nothing else is passed -- the Runners service carries the
// wake's resets with the transition.
func TestReconcileWorkloadsRecordsMissedWakeReport(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING, true, runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, nil)
	h.reconciler.hibernation.wakeAttempted(h.workload.GetMeta().GetId())
	h.reconcile(t)
	update := h.singleUpdate(t)
	if update.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		t.Fatalf("expected running, got %s", update.GetStatus())
	}
	if update.GetWokenAt() != nil || update.GetHibernatedAt() != nil || update.LastMeteringSampledAt != nil {
		t.Fatal("expected the resets left to the Runners service")
	}
	if h.reconciler.hibernation.wakeCount(h.workload.GetMeta().GetId()) != 0 {
		t.Fatal("expected the wake attempts forgotten once the workload runs")
	}
}

// waking, still hibernated: the wake did not take. WakeWorkload is tried again
// on each pass, and after wakeAttempts of them the workload fails with
// wake_failed, the runner discards its state and the identity goes.
func TestReconcileWorkloadsRetriesWakeThenFails(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING, true, runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, nil)
	// The main loop made the first attempt.
	h.reconciler.hibernation.wakeAttempted(h.workload.GetMeta().GetId())

	for pass := 1; pass < wakeAttempts; pass++ {
		h.reconcile(t)
		if h.wakes != pass {
			t.Fatalf("expected %d retried wakes after pass %d, got %d", pass, pass, h.wakes)
		}
		if len(h.updates) != 0 {
			t.Fatalf("expected no record write while retrying, got %v", h.updates)
		}
	}
	h.reconcile(t)
	if h.wakes != wakeAttempts-1 {
		t.Fatalf("expected no wake past the attempt budget, got %d", h.wakes)
	}
	update := h.singleUpdate(t)
	if update.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED || update.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_WAKE_FAILED {
		t.Fatalf("expected failed with wake_failed, got %s %s", update.GetStatus(), update.GetFailureReason())
	}
	if h.stops != 1 {
		t.Fatalf("expected the persisted state discarded once, got %d stops", h.stops)
	}
	if len(h.deleted) != 1 {
		t.Fatalf("expected the identity deleted, got %v", h.deleted)
	}
}

// waking, absent: wake_failed.
func TestReconcileWorkloadsFailsMissingWakingAsWakeFailed(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING, false, runnerv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, nil)
	h.reconcile(t)
	update := h.singleUpdate(t)
	if update.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED || update.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_WAKE_FAILED {
		t.Fatalf("expected failed with wake_failed, got %s %s", update.GetStatus(), update.GetFailureReason())
	}
	if len(h.deleted) != 1 {
		t.Fatalf("expected the identity deleted, got %v", h.deleted)
	}
}

// running, reported hibernated: a sleep this service never recorded. The record
// follows the runner rather than judging frozen containers.
func TestReconcileWorkloadsFollowsRunnerIntoHibernated(t *testing.T) {
	size := int64(1 << 30)
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, true, runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, &size)
	h.reconcile(t)
	update := h.singleUpdate(t)
	if update.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED || update.GetSnapshotSizeBytes() != size {
		t.Fatalf("expected hibernated with the reported size, got %s %v", update.GetStatus(), update.SnapshotSizeBytes)
	}
	if h.stops != 0 {
		t.Fatal("expected no stop of a sleeping workload")
	}
}

// hibernating, absent: the runner lost it mid-pause, as it would have lost a
// running one.
func TestReconcileWorkloadsFailsMissingHibernatingAsRuntimeLost(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING, false, runnerv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, nil)
	h.reconcile(t)
	update := h.singleUpdate(t)
	if update.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST {
		t.Fatalf("expected runtime_lost, got %s", update.GetFailureReason())
	}
}
