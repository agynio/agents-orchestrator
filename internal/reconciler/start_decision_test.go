package reconciler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestShouldStartWorkloadUsesRunnerIdentityContext(t *testing.T) {
	ctx := context.Background()
	threadID := uuid.New()
	agentID := uuid.MustParse(testAgentID)
	listCalls := 0

	runners := &fakeRunnersClient{
		listWorkloadsByThread: func(ctx context.Context, req *runnersv1.ListWorkloadsByThreadRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
			listCalls++
			if req.GetThreadId() != threadID.String() {
				return nil, errors.New("unexpected thread id")
			}
			metadataValues, ok := metadata.FromOutgoingContext(ctx)
			if !ok {
				return nil, errors.New("missing outgoing metadata")
			}
			identityValues := metadataValues.Get(identityMetadataKey)
			if len(identityValues) != 1 || identityValues[0] != agentID.String() {
				return nil, fmt.Errorf("unexpected identity metadata: %v", identityValues)
			}
			return &runnersv1.ListWorkloadsByThreadResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{Runners: runners})
	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, time.Now().UTC(), map[uuid.UUID]time.Time{}, nil)
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if !shouldStart {
		t.Fatal("expected start decision to allow workload")
	}
	if listCalls == 0 {
		t.Fatal("expected list workloads calls")
	}
}

func TestShouldStartWorkloadSkipsActiveWorkloads(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := uuid.New()
	threadID := uuid.New()
	fixture := startDecisionFixture{
		t:        t,
		agentID:  agentID,
		threadID: threadID,
		active: []*runnersv1.Workload{
			makeDecisionWorkload("active", runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, now.Add(-2*time.Minute), time.Time{}),
		},
	}

	runners := &fakeRunnersClient{listWorkloadsByThread: fixture.list}
	reconciler := newTestReconciler(Config{Runners: runners})

	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, now, map[uuid.UUID]time.Time{agentID: now.Add(-time.Hour)}, nil)
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if shouldStart {
		t.Fatal("expected start decision to be blocked")
	}
}

func TestShouldStartWorkloadAllowsStoppedOrEmpty(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := uuid.New()
	threadID := uuid.New()
	fixture := startDecisionFixture{
		t:        t,
		agentID:  agentID,
		threadID: threadID,
		latest: []*runnersv1.Workload{
			makeDecisionWorkload("stopped", runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, now.Add(-time.Hour), now.Add(-time.Hour)),
		},
	}

	runners := &fakeRunnersClient{listWorkloadsByThread: fixture.list}
	reconciler := newTestReconciler(Config{Runners: runners})

	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, now, map[uuid.UUID]time.Time{}, nil)
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if !shouldStart {
		t.Fatal("expected start decision to allow workload")
	}
}

func TestShouldStartWorkloadRetriesAfterAgentUpdate(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := uuid.New()
	threadID := uuid.New()
	removedAt := now.Add(-5 * time.Minute)
	fixture := startDecisionFixture{
		t:        t,
		agentID:  agentID,
		threadID: threadID,
		latest: []*runnersv1.Workload{
			makeDecisionWorkload("failed", runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, removedAt.Add(-time.Minute), removedAt),
		},
	}

	runners := &fakeRunnersClient{listWorkloadsByThread: fixture.list}
	reconciler := newTestReconciler(Config{Runners: runners})

	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, now, map[uuid.UUID]time.Time{agentID: removedAt.Add(time.Minute)}, nil)
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if !shouldStart {
		t.Fatal("expected start decision to allow workload")
	}
}

func TestShouldStartWorkloadBackoffSchedule(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2024, 10, 10, 9, 0, 0, 0, time.UTC)
	agentID := uuid.New()
	threadID := uuid.New()
	updatedAt := base.Add(-24 * time.Hour)

	for failures := 1; failures <= 6; failures++ {
		backoffIndex := failures - 1
		if backoffIndex >= len(startBackoffSchedule) {
			backoffIndex = len(startBackoffSchedule) - 1
		}
		backoff := startBackoffSchedule[backoffIndex]

		for _, delta := range []struct {
			name        string
			removedAt   time.Time
			shouldStart bool
		}{
			{name: "before", removedAt: base.Add(-backoff + time.Second), shouldStart: false},
			{name: "after", removedAt: base.Add(-backoff - time.Second), shouldStart: true},
		} {
			t.Run(fmt.Sprintf("failures-%d-%s", failures, delta.name), func(t *testing.T) {
				failuresList := makeDecisionFailures(failures, delta.removedAt)
				fixture := startDecisionFixture{
					t:        t,
					agentID:  agentID,
					threadID: threadID,
					latest:   []*runnersv1.Workload{failuresList[0]},
					failed:   failuresList,
				}

				runners := &fakeRunnersClient{listWorkloadsByThread: fixture.list}
				reconciler := newTestReconciler(Config{Runners: runners})

				shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, base, map[uuid.UUID]time.Time{agentID: updatedAt}, nil)
				if err != nil {
					t.Fatalf("should start workload: %v", err)
				}
				if shouldStart != delta.shouldStart {
					t.Fatalf("expected shouldStart=%v, got %v", delta.shouldStart, shouldStart)
				}
			})
		}
	}
}

func TestShouldStartWorkloadResetsOnLastStopped(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2024, 10, 10, 9, 0, 0, 0, time.UTC)
	agentID := uuid.New()
	threadID := uuid.New()
	updatedAt := base
	lastStoppedAt := base.Add(2 * time.Minute)
	latestRemovedAt := base.Add(4*time.Minute + 15*time.Second)

	fixture := startDecisionFixture{
		t:        t,
		agentID:  agentID,
		threadID: threadID,
		latest: []*runnersv1.Workload{
			makeDecisionWorkload("latest", runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, latestRemovedAt.Add(-time.Minute), latestRemovedAt),
		},
		stopped: []*runnersv1.Workload{
			makeDecisionWorkload("stopped", runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, lastStoppedAt, lastStoppedAt),
		},
		failed: []*runnersv1.Workload{
			makeDecisionWorkload("recent", runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, latestRemovedAt.Add(-2*time.Second), latestRemovedAt),
			makeDecisionWorkload("old", runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, base.Add(time.Minute), base.Add(time.Minute)),
		},
	}

	runners := &fakeRunnersClient{listWorkloadsByThread: fixture.list}
	reconciler := newTestReconciler(Config{Runners: runners})

	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, base.Add(4*time.Minute+30*time.Second), map[uuid.UUID]time.Time{agentID: updatedAt}, nil)
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if !shouldStart {
		t.Fatal("expected start decision to allow workload")
	}
}

func TestShouldStartWorkloadDegradesAfterMaxFailures(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := uuid.New()
	threadID := uuid.New()

	var degradedRequest *threadsv1.DegradeThreadRequest
	threads := &fakeThreadsClient{
		degradeThread: func(_ context.Context, req *threadsv1.DegradeThreadRequest, _ ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error) {
			degradedRequest = req
			return &threadsv1.DegradeThreadResponse{}, nil
		},
	}

	failures := makeDecisionFailures(maxStartAttempts, now.Add(-2*time.Minute))
	fixture := startDecisionFixture{
		t:        t,
		agentID:  agentID,
		threadID: threadID,
		latest:   []*runnersv1.Workload{failures[0]},
		failed:   failures,
	}

	runners := &fakeRunnersClient{listWorkloadsByThread: fixture.list}
	reconciler := newTestReconciler(Config{Runners: runners, Threads: threads})

	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, now, map[uuid.UUID]time.Time{agentID: now.Add(-time.Hour)}, newDegradeTracker())
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if shouldStart {
		t.Fatal("expected start decision to be blocked")
	}
	if degradedRequest == nil {
		t.Fatal("expected degraded thread request")
	}
	if degradedRequest.GetThreadId() != threadID.String() {
		t.Fatalf("unexpected degraded thread id: %s", degradedRequest.GetThreadId())
	}
	if degradedRequest.GetReason() != degradeReasonStartFailures {
		t.Fatalf("unexpected degraded reason: %s", degradedRequest.GetReason())
	}
}

type startDecisionFixture struct {
	t        *testing.T
	agentID  uuid.UUID
	threadID uuid.UUID
	active   []*runnersv1.Workload
	latest   []*runnersv1.Workload
	stopped  []*runnersv1.Workload
	failed   []*runnersv1.Workload
}

func (f startDecisionFixture) list(_ context.Context, req *runnersv1.ListWorkloadsByThreadRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
	f.t.Helper()
	if req.GetThreadId() != f.threadID.String() {
		return nil, fmt.Errorf("unexpected thread id: %s", req.GetThreadId())
	}
	if req.GetAgentId() != f.agentID.String() {
		return nil, fmt.Errorf("unexpected agent id: %s", req.GetAgentId())
	}
	statuses := req.GetStatuses()
	switch {
	case matchStatuses(statuses, []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
	}):
		return &runnersv1.ListWorkloadsByThreadResponse{Workloads: f.active}, nil
	case matchStatuses(statuses, []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	}):
		return &runnersv1.ListWorkloadsByThreadResponse{Workloads: f.latest}, nil
	case matchStatuses(statuses, []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
	}):
		return &runnersv1.ListWorkloadsByThreadResponse{Workloads: f.stopped}, nil
	case matchStatuses(statuses, []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	}):
		return &runnersv1.ListWorkloadsByThreadResponse{Workloads: f.failed}, nil
	default:
		return nil, fmt.Errorf("unexpected statuses: %v", statuses)
	}
}

func matchStatuses(got, want []runnersv1.WorkloadStatus) bool {
	if len(got) != len(want) {
		return false
	}
	gotCopy := append([]runnersv1.WorkloadStatus(nil), got...)
	wantCopy := append([]runnersv1.WorkloadStatus(nil), want...)
	sort.Slice(gotCopy, func(i, j int) bool { return gotCopy[i] < gotCopy[j] })
	sort.Slice(wantCopy, func(i, j int) bool { return wantCopy[i] < wantCopy[j] })
	for i := range gotCopy {
		if gotCopy[i] != wantCopy[i] {
			return false
		}
	}
	return true
}

func makeDecisionWorkload(id string, status runnersv1.WorkloadStatus, createdAt time.Time, removedAt time.Time) *runnersv1.Workload {
	workload := &runnersv1.Workload{
		Meta:   &runnersv1.EntityMeta{Id: id, CreatedAt: timestamppb.New(createdAt)},
		Status: status,
	}
	if !removedAt.IsZero() {
		workload.RemovedAt = timestamppb.New(removedAt)
	}
	return workload
}

func makeDecisionFailures(count int, latestRemovedAt time.Time) []*runnersv1.Workload {
	if count <= 0 {
		return nil
	}
	failures := make([]*runnersv1.Workload, 0, count)
	for i := 0; i < count; i++ {
		createdAt := latestRemovedAt.Add(-time.Duration(i) * time.Minute)
		removedAt := time.Time{}
		if i == 0 {
			removedAt = latestRemovedAt
		}
		failures = append(failures, makeDecisionWorkload(fmt.Sprintf("failure-%d", i), runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, createdAt, removedAt))
	}
	return failures
}
