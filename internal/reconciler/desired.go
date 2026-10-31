package reconciler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
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

func (r *Reconciler) fetchDesired(ctx context.Context) ([]AgentInstanceTarget, map[uuid.UUID]agentPolicy, map[uuid.UUID]time.Time, error) {
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
	policies := make(map[uuid.UUID]agentPolicy, len(agentIDs))
	agentUpdatedAt := make(map[uuid.UUID]time.Time, len(agentIDs))
	for agentID := range agentIDs {
		agent, err := r.fetchAgent(ctx, agentID)
		if err != nil {
			return nil, nil, nil, err
		}
		policy, err := agentPolicyFromClass(agent, agentID, r.idle)
		if err != nil {
			return nil, nil, nil, err
		}
		updatedAt := agent.GetMeta().GetUpdatedAt()
		if updatedAt == nil {
			return nil, nil, nil, fmt.Errorf("agent %s updated_at missing", agentID.String())
		}
		policies[agentID] = policy
		configUpdatedAt := updatedAt.AsTime().UTC()
		// A repaired environment must unblock every instance running it, not
		// only those whose own class changed. An environment sub-resource write
		// touches the environment, so its updated_at covers volumes, MCPs, init
		// scripts and ENVs alike.
		environmentUpdatedAt, err := r.environmentUpdatedAt(ctx, agent.GetEnvironmentId())
		if err != nil {
			return nil, nil, nil, err
		}
		if environmentUpdatedAt.After(configUpdatedAt) {
			configUpdatedAt = environmentUpdatedAt
		}
		agentUpdatedAt[agentID] = configUpdatedAt
	}
	result := make([]AgentInstanceTarget, 0, len(unique))
	for key := range unique {
		result = append(result, key)
	}
	return result, policies, agentUpdatedAt, nil
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
	// As the platform, not as the instance whose inbox this is. Agents admits
	// cluster admin on the inbox reads for exactly this: the Orchestrator has to
	// know which thread the work arrived on before it can place a workload.
	resp, err := r.agents.GetUnackedInboxItems(r.platformContext(ctx), &agentsv1.GetUnackedInboxItemsRequest{
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

// agentPolicy is what a class says about the life of its idle workloads: how
// long one may sit idle before it is stopped; whether it is hibernated instead,
// on a runner that can, and how long after a turn; and how long a hibernated
// one is kept before it is stopped.
type agentPolicy struct {
	idleTimeout    time.Duration
	hibernate      bool
	hibernateAfter time.Duration
	hibernationTTL time.Duration
}

const (
	// defaultHibernateAfter hibernates at the turn boundary.
	defaultHibernateAfter = 0
	// defaultHibernationTTL is the seven days the class field defaults to.
	defaultHibernationTTL = 168 * time.Hour
)

// agentPolicyFromClass reads the policy off the class. The durations default as
// the resource definition does when the class leaves them unset. hibernate is
// read as the class states it: an Agents service that predates the field
// states false, and hibernates nothing.
func agentPolicyFromClass(agent *agentsv1.Agent, agentID uuid.UUID, fallbackIdleTimeout time.Duration) (agentPolicy, error) {
	idleTimeout, err := agentIdleTimeout(agent, agentID, fallbackIdleTimeout)
	if err != nil {
		return agentPolicy{}, err
	}
	policy := agentPolicy{
		idleTimeout:    idleTimeout,
		hibernate:      agent.GetHibernate(),
		hibernateAfter: defaultHibernateAfter,
		hibernationTTL: defaultHibernationTTL,
	}
	if agent.HibernateAfter != nil {
		parsed, err := time.ParseDuration(agent.GetHibernateAfter())
		if err != nil {
			return agentPolicy{}, fmt.Errorf("parse agent %s hibernate_after: %w", agentID, err)
		}
		if parsed < 0 {
			return agentPolicy{}, fmt.Errorf("agent %s hibernate_after must not be negative", agentID)
		}
		policy.hibernateAfter = parsed
	}
	if agent.HibernationTtl != nil {
		parsed, err := time.ParseDuration(agent.GetHibernationTtl())
		if err != nil {
			return agentPolicy{}, fmt.Errorf("parse agent %s hibernation_ttl: %w", agentID, err)
		}
		if parsed <= 0 {
			return agentPolicy{}, fmt.Errorf("agent %s hibernation_ttl must be greater than 0", agentID)
		}
		policy.hibernationTTL = parsed
	}
	return policy, nil
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

// addAgentPoliciesForWorkloads fills in the policy of every agent that has a
// workload live, so the stop, hibernation and TTL decisions know what the agent
// asked for even after it has dropped out of the desired set. Policies already
// resolved are left alone, and an agent that cannot be read is skipped rather
// than failing the cycle: the fallback still applies, which is what it did
// before -- the platform idle timeout, and no hibernation.
func (r *Reconciler) addAgentPoliciesForWorkloads(ctx context.Context, workloads []*runnersv1.Workload, policies map[uuid.UUID]agentPolicy) error {
	if policies == nil {
		return fmt.Errorf("agent policies map is nil")
	}
	for _, workload := range workloads {
		if workload == nil {
			continue
		}
		agentID, err := uuidutil.ParseUUID(workloadAgentClassID(workload), "workload.agent_class_id")
		if err != nil {
			return err
		}
		if _, ok := policies[agentID]; ok {
			continue
		}
		agent, err := r.fetchAgent(ctx, agentID)
		if err != nil {
			log.Printf("reconciler: read policy for agent %s: %v; using the default", agentID, err)
			continue
		}
		policy, err := agentPolicyFromClass(agent, agentID, r.idle)
		if err != nil {
			log.Printf("reconciler: %v; using the default", err)
			continue
		}
		policies[agentID] = policy
	}
	return nil
}

// environmentUpdatedAt reports when an agent's environment last changed. A blank
// id means the agent names none, which is not an error: agents predating
// environments keep their inline configuration.
func (r *Reconciler) environmentUpdatedAt(ctx context.Context, environmentID string) (time.Time, error) {
	environmentID = strings.TrimSpace(environmentID)
	if environmentID == "" {
		return time.Time{}, nil
	}
	resp, err := r.agents.GetEnvironment(ctx, &agentsv1.GetEnvironmentRequest{Id: environmentID})
	if err != nil {
		return time.Time{}, fmt.Errorf("get environment %s: %w", environmentID, err)
	}
	updated := resp.GetEnvironment().GetMeta().GetUpdatedAt()
	if updated == nil {
		return time.Time{}, nil
	}
	return updated.AsTime().UTC(), nil
}
