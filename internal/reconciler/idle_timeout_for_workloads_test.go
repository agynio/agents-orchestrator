package reconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// An idle timeout only decides anything once the agent has gone idle, and by
// then it has no unacked inbox and has dropped out of the desired set. Reading
// the timeouts from the desired set alone meant a workload was stopped on the
// platform fallback the moment it finished answering: an agent asking for 10m
// was torn down 30s after its reply.
func TestIdleTimeoutsResolvedForRunningWorkloads(t *testing.T) {
	agentID := uuid.New()
	agents := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			idle := "10m"
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{
				Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
				OrganizationId: uuid.NewString(),
				IdleTimeout:    &idle,
			}}, nil
		},
	}
	reconciler := newTestReconciler(Config{Agents: agents, Idle: 30 * time.Second})

	idleTimeouts := map[uuid.UUID]time.Duration{}
	workloads := []*runnersv1.Workload{workloadForAgent(agentID)}
	if err := reconciler.addIdleTimeoutsForWorkloads(context.Background(), workloads, idleTimeouts); err != nil {
		t.Fatalf("add idle timeouts: %v", err)
	}

	if got := idleTimeouts[agentID]; got != 10*time.Minute {
		t.Fatalf("expected 10m for the running workload's agent, got %v", got)
	}
}

// A timeout already resolved from the desired set is authoritative; re-reading
// it would be a needless call per workload per cycle.
func TestIdleTimeoutsKeepAlreadyResolvedEntries(t *testing.T) {
	agentID := uuid.New()
	agents := &testutil.FakeAgentsClient{
		GetAgentFunc: func(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			t.Fatal("expected no lookup for an agent already resolved")
			return nil, nil
		},
	}
	reconciler := newTestReconciler(Config{Agents: agents})

	idleTimeouts := map[uuid.UUID]time.Duration{agentID: 5 * time.Minute}
	workloads := []*runnersv1.Workload{workloadForAgent(agentID)}
	if err := reconciler.addIdleTimeoutsForWorkloads(context.Background(), workloads, idleTimeouts); err != nil {
		t.Fatalf("add idle timeouts: %v", err)
	}
	if got := idleTimeouts[agentID]; got != 5*time.Minute {
		t.Fatalf("expected the resolved 5m to stand, got %v", got)
	}
}

// An agent that cannot be read must not fail the cycle: the fallback applies,
// which is what happened before.
func TestIdleTimeoutsSkipUnreadableAgents(t *testing.T) {
	agentID := uuid.New()
	agents := &testutil.FakeAgentsClient{
		GetAgentFunc: func(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return nil, errors.New("agents is down")
		},
	}
	reconciler := newTestReconciler(Config{Agents: agents})

	idleTimeouts := map[uuid.UUID]time.Duration{}
	workloads := []*runnersv1.Workload{workloadForAgent(agentID)}
	if err := reconciler.addIdleTimeoutsForWorkloads(context.Background(), workloads, idleTimeouts); err != nil {
		t.Fatalf("expected an unreadable agent to be skipped, got %v", err)
	}
	if _, ok := idleTimeouts[agentID]; ok {
		t.Fatal("expected no entry for an agent that could not be read")
	}
}

func workloadForAgent(agentID uuid.UUID) *runnersv1.Workload {
	id := agentID.String()
	return &runnersv1.Workload{AgentClassId: &id}
}
