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
// the policies from the desired set alone meant a workload was stopped on the
// platform fallback the moment it finished answering: an agent asking for 10m
// was torn down 30s after its reply.
func TestAgentPoliciesResolvedForLiveWorkloads(t *testing.T) {
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

	policies := map[uuid.UUID]agentPolicy{}
	workloads := []*runnersv1.Workload{workloadForAgent(agentID)}
	if err := reconciler.addAgentPoliciesForWorkloads(context.Background(), workloads, policies); err != nil {
		t.Fatalf("add agent policies: %v", err)
	}

	if got := policies[agentID].idleTimeout; got != 10*time.Minute {
		t.Fatalf("expected 10m for the running workload's agent, got %v", got)
	}
}

// A policy already resolved from the desired set is authoritative; re-reading
// it would be a needless call per workload per cycle.
func TestAgentPoliciesKeepAlreadyResolvedEntries(t *testing.T) {
	agentID := uuid.New()
	agents := &testutil.FakeAgentsClient{
		GetAgentFunc: func(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			t.Fatal("expected no lookup for an agent already resolved")
			return nil, nil
		},
	}
	reconciler := newTestReconciler(Config{Agents: agents})

	policies := map[uuid.UUID]agentPolicy{agentID: {idleTimeout: 5 * time.Minute}}
	workloads := []*runnersv1.Workload{workloadForAgent(agentID)}
	if err := reconciler.addAgentPoliciesForWorkloads(context.Background(), workloads, policies); err != nil {
		t.Fatalf("add agent policies: %v", err)
	}
	if got := policies[agentID].idleTimeout; got != 5*time.Minute {
		t.Fatalf("expected the resolved 5m to stand, got %v", got)
	}
}

// An agent that cannot be read must not fail the cycle: the fallback applies,
// which is what happened before -- and the fallback hibernates nothing.
func TestAgentPoliciesSkipUnreadableAgents(t *testing.T) {
	agentID := uuid.New()
	agents := &testutil.FakeAgentsClient{
		GetAgentFunc: func(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return nil, errors.New("agents is down")
		},
	}
	reconciler := newTestReconciler(Config{Agents: agents})

	policies := map[uuid.UUID]agentPolicy{}
	workloads := []*runnersv1.Workload{workloadForAgent(agentID)}
	if err := reconciler.addAgentPoliciesForWorkloads(context.Background(), workloads, policies); err != nil {
		t.Fatalf("expected an unreadable agent to be skipped, got %v", err)
	}
	if _, ok := policies[agentID]; ok {
		t.Fatal("expected no entry for an agent that could not be read")
	}
}

// The class's hibernation fields beside idle_timeout: read as given, defaulted
// as the resource definition defaults them when unset.
func TestAgentPolicyReadsHibernationFields(t *testing.T) {
	agentID := uuid.New()
	idle, after, ttl := "10m", "30s", "48h"
	policy, err := agentPolicyFromClass(&agentsv1.Agent{
		Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
		IdleTimeout:    &idle,
		Hibernate:      true,
		HibernateAfter: &after,
		HibernationTtl: &ttl,
	}, agentID, time.Minute)
	if err != nil {
		t.Fatalf("agent policy: %v", err)
	}
	expected := agentPolicy{idleTimeout: 10 * time.Minute, hibernate: true, hibernateAfter: 30 * time.Second, hibernationTTL: 48 * time.Hour}
	if policy != expected {
		t.Fatalf("expected %+v, got %+v", expected, policy)
	}

	defaulted, err := agentPolicyFromClass(&agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, Hibernate: true}, agentID, time.Minute)
	if err != nil {
		t.Fatalf("agent policy with defaults: %v", err)
	}
	expected = agentPolicy{idleTimeout: time.Minute, hibernate: true, hibernateAfter: 0, hibernationTTL: 168 * time.Hour}
	if defaulted != expected {
		t.Fatalf("expected defaults %+v, got %+v", expected, defaulted)
	}

	// An Agents service that predates the field states false, and nothing is
	// hibernated: the platform behaves as before until the class says so.
	legacy, err := agentPolicyFromClass(&agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}}, agentID, time.Minute)
	if err != nil {
		t.Fatalf("agent policy without hibernate: %v", err)
	}
	if legacy.hibernate {
		t.Fatal("expected a class that does not state hibernate to hibernate nothing")
	}

	bad := "-5s"
	if _, err := agentPolicyFromClass(&agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, HibernateAfter: &bad}, agentID, time.Minute); err == nil {
		t.Fatal("expected a negative hibernate_after to be refused")
	}
	zero := "0s"
	if _, err := agentPolicyFromClass(&agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, HibernationTtl: &zero}, agentID, time.Minute); err == nil {
		t.Fatal("expected a zero hibernation_ttl to be refused")
	}
}

func workloadForAgent(agentID uuid.UUID) *runnersv1.Workload {
	id := agentID.String()
	return &runnersv1.Workload{AgentClassId: &id}
}
