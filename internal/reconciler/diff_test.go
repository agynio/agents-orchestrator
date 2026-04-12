package reconciler

import (
	"reflect"
	"sort"
	"testing"
	"time"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestComputeActions(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	agent1 := uuid.New()
	thread1 := uuid.New()
	agent2 := uuid.New()
	thread2 := uuid.New()
	agent3 := uuid.New()
	thread3 := uuid.New()
	missingAgent := uuid.New()
	missingThread := uuid.New()
	idleTimeouts := map[uuid.UUID]time.Duration{
		agent1: 30 * time.Minute,
		agent2: 15 * time.Minute,
		agent3: 10 * time.Minute,
	}
	idleFallback := 12 * time.Minute

	activityOld := now.Add(-20 * time.Minute)

	workload1 := makeWorkload(agent1, thread1, now, nil)
	workload2 := makeWorkload(agent3, thread3, now.Add(-1*time.Minute), &activityOld)
	workload3 := makeWorkload(agent1, thread1, now.Add(-20*time.Minute), nil)
	workload4 := makeWorkload(agent1, thread1, now.Add(-5*time.Minute), nil)
	workload5 := makeWorkload(agent2, thread2, now.Add(-20*time.Minute), nil)
	workload6 := makeWorkload(missingAgent, missingThread, now.Add(-20*time.Minute), nil)

	cases := []struct {
		name     string
		desired  []AgentThread
		actual   []*runnersv1.Workload
		expected Actions
	}{
		{
			name:     "empty",
			desired:  nil,
			actual:   nil,
			expected: Actions{ToStart: nil, ToStop: nil},
		},
		{
			name:    "start missing",
			desired: []AgentThread{{AgentID: agent1, ThreadID: thread1}},
			actual:  nil,
			expected: Actions{
				ToStart: []AgentThread{{AgentID: agent1, ThreadID: thread1}},
			},
		},
		{
			name:   "stop idle by activity",
			actual: []*runnersv1.Workload{workload2},
			expected: Actions{
				ToStop: []*runnersv1.Workload{workload2},
			},
		},
		{
			name:   "stop idle by created_at",
			actual: []*runnersv1.Workload{workload5},
			expected: Actions{
				ToStop: []*runnersv1.Workload{workload5},
			},
		},
		{
			name:   "stop idle with fallback",
			actual: []*runnersv1.Workload{workload6},
			expected: Actions{
				ToStop: []*runnersv1.Workload{workload6},
			},
		},
		{
			name:   "keep recent",
			actual: []*runnersv1.Workload{workload4},
		},
		{
			name:    "match",
			desired: []AgentThread{{AgentID: agent1, ThreadID: thread1}},
			actual:  []*runnersv1.Workload{workload1},
		},
		{
			name: "mixed",
			desired: []AgentThread{
				{AgentID: agent1, ThreadID: thread1},
				{AgentID: agent2, ThreadID: thread2},
			},
			actual: []*runnersv1.Workload{workload3, workload2},
			expected: Actions{
				ToStart: []AgentThread{{AgentID: agent2, ThreadID: thread2}},
				ToStop:  []*runnersv1.Workload{workload2},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := ComputeActions(testCase.desired, testCase.actual, idleTimeouts, idleFallback, now)
			if err != nil {
				t.Fatalf("compute actions: %v", err)
			}
			sortAgentThreads(result.ToStart)
			sortWorkloads(result.ToStop)
			sortAgentThreads(testCase.expected.ToStart)
			sortWorkloads(testCase.expected.ToStop)
			if !reflect.DeepEqual(result, testCase.expected) {
				t.Fatalf("expected %+v, got %+v", testCase.expected, result)
			}
		})
	}
}

func makeWorkload(agentID, threadID uuid.UUID, createdAt time.Time, lastActivityAt *time.Time) *runnersv1.Workload {
	workload := &runnersv1.Workload{
		Meta: &runnersv1.EntityMeta{
			Id:        uuid.NewString(),
			CreatedAt: timestamppb.New(createdAt),
		},
		AgentId:  agentID.String(),
		ThreadId: threadID.String(),
	}
	if lastActivityAt != nil {
		workload.LastActivityAt = timestamppb.New(*lastActivityAt)
	}
	return workload
}

func sortAgentThreads(values []AgentThread) {
	sort.Slice(values, func(i, j int) bool {
		if values[i].AgentID == values[j].AgentID {
			return values[i].ThreadID.String() < values[j].ThreadID.String()
		}
		return values[i].AgentID.String() < values[j].AgentID.String()
	})
}

func sortWorkloads(values []*runnersv1.Workload) {
	sort.Slice(values, func(i, j int) bool {
		return values[i].GetMeta().GetId() < values[j].GetMeta().GetId()
	})
}
