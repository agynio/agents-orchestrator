package reconciler

import (
	"context"
	"fmt"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

const desiredPageSize int32 = 100

type AgentInstanceTarget struct {
	AgentInstanceID uuid.UUID
	AgentID         uuid.UUID
	OrganizationID  uuid.UUID
	ThreadID        uuid.UUID
}

func (r *Reconciler) fetchDesired(ctx context.Context) ([]AgentInstanceTarget, map[uuid.UUID]time.Duration, map[uuid.UUID]time.Time, error) {
	instances, err := r.listActiveInstancesWithUnackedInbox(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	unique := make(map[AgentInstanceTarget]struct{}, len(instances))
	agentIDs := make(map[uuid.UUID]struct{})
	for _, instance := range instances {
		threadID, err := r.fetchFirstUnackedInboxThreadID(ctx, instance)
		if err != nil {
			return nil, nil, nil, err
		}
		target, err := agentInstanceTarget(instance, threadID)
		if err != nil {
			return nil, nil, nil, err
		}
		unique[target] = struct{}{}
		agentIDs[target.AgentID] = struct{}{}
	}
	idleTimeouts := make(map[uuid.UUID]time.Duration, len(agentIDs))
	agentUpdatedAt := make(map[uuid.UUID]time.Time, len(agentIDs))
	for agentID := range agentIDs {
		agent, err := r.fetchAgent(ctx, agentID)
		if err != nil {
			return nil, nil, nil, err
		}
		idleTimeout, err := agentIdleTimeout(agent, agentID, r.idle)
		if err != nil {
			return nil, nil, nil, err
		}
		updatedAt := agent.GetMeta().GetUpdatedAt()
		if updatedAt == nil {
			return nil, nil, nil, fmt.Errorf("agent %s updated_at missing", agentID.String())
		}
		idleTimeouts[agentID] = idleTimeout
		agentUpdatedAt[agentID] = updatedAt.AsTime().UTC()
	}
	result := make([]AgentInstanceTarget, 0, len(unique))
	for key := range unique {
		result = append(result, key)
	}
	return result, idleTimeouts, agentUpdatedAt, nil
}

func agentInstanceTarget(instance *agentsv1.AgentInstance, threadID uuid.UUID) (AgentInstanceTarget, error) {
	if instance == nil {
		return AgentInstanceTarget{}, fmt.Errorf("agent instance is nil")
	}
	meta := instance.GetMeta()
	if meta == nil {
		return AgentInstanceTarget{}, fmt.Errorf("agent instance meta missing")
	}
	agentInstanceID, err := uuidutil.ParseUUID(meta.GetId(), "agent_instance.meta.id")
	if err != nil {
		return AgentInstanceTarget{}, err
	}
	agentID, err := uuidutil.ParseUUID(instance.GetAgentId(), "agent_instance.agent_id")
	if err != nil {
		return AgentInstanceTarget{}, err
	}
	organizationID, err := uuidutil.ParseUUID(instance.GetOrganizationId(), "agent_instance.organization_id")
	if err != nil {
		return AgentInstanceTarget{}, err
	}
	return AgentInstanceTarget{
		AgentInstanceID: agentInstanceID,
		AgentID:         agentID,
		OrganizationID:  organizationID,
		ThreadID:        threadID,
	}, nil
}

func (r *Reconciler) fetchFirstUnackedInboxThreadID(ctx context.Context, instance *agentsv1.AgentInstance) (uuid.UUID, error) {
	if instance == nil {
		return uuid.Nil, fmt.Errorf("agent instance is nil")
	}
	meta := instance.GetMeta()
	if meta == nil {
		return uuid.Nil, fmt.Errorf("agent instance meta missing")
	}
	agentInstanceID := meta.GetId()
	if _, err := uuidutil.ParseUUID(agentInstanceID, "agent_instance.meta.id"); err != nil {
		return uuid.Nil, err
	}
	inboxCtx, err := runnerIdentityContext(ctx, agentInstanceID)
	if err != nil {
		return uuid.Nil, err
	}
	resp, err := r.agents.GetUnackedInboxItems(inboxCtx, &agentsv1.GetUnackedInboxItemsRequest{
		AgentInstanceId: agentInstanceID,
		PageSize:        1,
	})
	if err != nil {
		return uuid.Nil, fmt.Errorf("get first unacked inbox item for agent instance %s: %w", agentInstanceID, err)
	}
	items := resp.GetItems()
	if len(items) == 0 {
		return uuid.Nil, fmt.Errorf("agent instance %s has_unacked but returned no inbox items", agentInstanceID)
	}
	threadID, err := uuidutil.ParseUUID(items[0].GetThreadId(), "inbox_item.thread_id")
	if err != nil {
		return uuid.Nil, err
	}
	return threadID, nil
}

func agentIdleTimeout(agent *agentsv1.Agent, agentID uuid.UUID, fallback time.Duration) (time.Duration, error) {
	if agent.IdleTimeout == nil {
		return fallback, nil
	}
	value := agent.GetIdleTimeout()
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse agent %s idle_timeout: %w", agentID, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("agent %s idle_timeout must be greater than 0", agentID)
	}
	return parsed, nil
}

func (r *Reconciler) listActiveInstancesWithUnackedInbox(ctx context.Context) ([]*agentsv1.AgentInstance, error) {
	resp := []*agentsv1.AgentInstance{}
	token := ""
	hasUnacked := true
	for {
		page, err := r.agents.ListInstances(ctx, &agentsv1.ListInstancesRequest{
			PageSize:   desiredPageSize,
			PageToken:  token,
			StateIn:    []agentsv1.AgentInstanceState{agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_ACTIVE},
			HasUnacked: &hasUnacked,
		})
		if err != nil {
			return nil, fmt.Errorf("list agent instances with unacked inbox: %w", err)
		}
		resp = append(resp, page.GetInstances()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (r *Reconciler) listAgents(ctx context.Context) ([]*agentsv1.Agent, error) {
	resp := []*agentsv1.Agent{}
	token := ""
	for {
		page, err := r.agents.ListAgents(ctx, &agentsv1.ListAgentsRequest{
			PageSize:  desiredPageSize,
			PageToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list agents: %w", err)
		}
		resp = append(resp, page.GetAgents()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (r *Reconciler) fetchAgent(ctx context.Context, agentID uuid.UUID) (*agentsv1.Agent, error) {
	resp, err := r.agents.GetAgent(ctx, &agentsv1.GetAgentRequest{Id: agentID.String()})
	if err != nil {
		return nil, fmt.Errorf("get agent %s: %w", agentID.String(), err)
	}
	agent := resp.GetAgent()
	if agent == nil {
		return nil, fmt.Errorf("agent %s missing", agentID.String())
	}
	meta := agent.GetMeta()
	if meta == nil {
		return nil, fmt.Errorf("agent %s meta missing", agentID.String())
	}
	parsedAgentID, err := uuidutil.ParseUUID(meta.GetId(), "agent.meta.id")
	if err != nil {
		return nil, err
	}
	if parsedAgentID != agentID {
		return nil, fmt.Errorf("agent %s response id mismatch: %s", agentID.String(), parsedAgentID.String())
	}
	if _, err := uuidutil.ParseUUID(agent.GetOrganizationId(), "agent.organization_id"); err != nil {
		return nil, err
	}
	return agent, nil
}
