package reconciler

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/agynio/agents-orchestrator/internal/store"
	"github.com/google/uuid"
)

func TestComputeActions(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	idleTimeout := 10 * time.Minute

	agent1 := uuid.New()
	thread1 := uuid.New()
	agent2 := uuid.New()
	thread2 := uuid.New()
	agent3 := uuid.New()
	thread3 := uuid.New()

	workload1 := makeWorkload(agent1, thread1, now)
	workload2 := makeWorkload(agent3, thread3, now.Add(-20*time.Minute))
	workload3 := makeWorkload(agent1, thread1, now.Add(-20*time.Minute))
	workload4 := makeWorkload(agent1, thread1, now.Add(-5*time.Minute))

	cases := []struct {
		name     string
		desired  []AgentThread
		actual   []store.Workload
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
			name:   "stop idle",
			actual: []store.Workload{workload2},
			expected: Actions{
				ToStop: []store.Workload{workload2},
			},
		},
		{
			name:   "keep recent",
			actual: []store.Workload{workload4},
		},
		{
			name:    "match",
			desired: []AgentThread{{AgentID: agent1, ThreadID: thread1}},
			actual:  []store.Workload{workload1},
		},
		{
			name: "mixed",
			desired: []AgentThread{
				{AgentID: agent1, ThreadID: thread1},
				{AgentID: agent2, ThreadID: thread2},
			},
			actual: []store.Workload{workload3, workload2},
			expected: Actions{
				ToStart: []AgentThread{{AgentID: agent2, ThreadID: thread2}},
				ToStop:  []store.Workload{workload2},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result := ComputeActions(testCase.desired, testCase.actual, idleTimeout, now)
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

func makeWorkload(agentID, threadID uuid.UUID, startedAt time.Time) store.Workload {
	return store.Workload{
		ID:         uuid.New(),
		WorkloadID: uuid.NewString(),
		AgentID:    agentID,
		ThreadID:   threadID,
		StartedAt:  startedAt,
	}
}

func sortAgentThreads(values []AgentThread) {
	sort.Slice(values, func(i, j int) bool {
		if values[i].AgentID == values[j].AgentID {
			return values[i].ThreadID.String() < values[j].ThreadID.String()
		}
		return values[i].AgentID.String() < values[j].AgentID.String()
	})
}

func sortWorkloads(values []store.Workload) {
	sort.Slice(values, func(i, j int) bool {
		if values[i].WorkloadID == values[j].WorkloadID {
			return values[i].ID.String() < values[j].ID.String()
		}
		return values[i].WorkloadID < values[j].WorkloadID
	})
}
