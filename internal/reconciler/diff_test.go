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
	workload5 := makeWorkload(agent2, thread2, now.Add(-30*time.Minute))
	workload5.LastActivityAt = timestamppb.New(now.Add(-5 * time.Minute))

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
			name:   "stop idle",
			actual: []*runnersv1.Workload{workload2},
			expected: Actions{
				ToStop: []*runnersv1.Workload{workload2},
			},
		},
		{
			name:   "keep recent",
			actual: []*runnersv1.Workload{workload4},
		},
		{
			name:   "idle uses last activity",
			actual: []*runnersv1.Workload{workload5},
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
			result, err := ComputeActions(testCase.desired, testCase.actual, idleTimeout, now)
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

func makeWorkload(agentID, threadID uuid.UUID, startedAt time.Time) *runnersv1.Workload {
	return &runnersv1.Workload{
		Meta: &runnersv1.EntityMeta{
			Id:        uuid.NewString(),
			CreatedAt: timestamppb.New(startedAt),
		},
		AgentId:  agentID.String(),
		ThreadId: threadID.String(),
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

func sortWorkloads(values []*runnersv1.Workload) {
	sort.Slice(values, func(i, j int) bool {
		return values[i].GetMeta().GetId() < values[j].GetMeta().GetId()
	})
}
