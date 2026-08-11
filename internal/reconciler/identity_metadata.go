package reconciler

import (
	"context"
	"fmt"
	"strings"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"google.golang.org/grpc/metadata"
)

// The reconciler names no principal on any call it makes.
//
// It used to. Every call to Runners carried an x-identity-id, and the id was
// whichever agent happened to be convenient -- the instance whose workload was
// being placed, or the first agent found in the organization being listed. That
// is a platform service claiming to be one of the things it manages, done so the
// callee's per-principal checks would let it through.
//
// Runners and Agents already serve an absent x-identity-id as a platform call:
// see identityFromMetadataOptional in Runners, which skips the organization
// filter entirely when no caller is named. So the reconciler names none, which
// is both honest and what lets it list the whole cluster in one call instead of
// once per organization. Groups is the exception -- it needs a caller -- and
// there the Orchestrator sends its own platform identity, which Groups settles
// against cluster admin.

// agentOrganizations is the set of organizations that have at least one agent.
//
// This used to be a map of organization to a borrowed agent identity, and both
// halves were used: the keys to decide which workloads this reconciler owns, and
// the values as a credential. Only the keys were ever legitimate.
func (r *Reconciler) agentOrganizations(ctx context.Context) (map[string]struct{}, error) {
	agents, err := r.listAllAgents(ctx)
	if err != nil {
		return nil, err
	}
	return agentOrganizationsFrom(agents)
}

// listAllAgents pages every agent the platform knows about. Callers that need
// more than one thing per agent derive them from this rather than listing again.
func (r *Reconciler) listAllAgents(ctx context.Context) ([]*agentsv1.Agent, error) {
	if r.agents == nil {
		return nil, fmt.Errorf("agents client is nil")
	}
	agents := []*agentsv1.Agent{}
	pageToken := ""
	for {
		resp, err := r.agents.ListAgents(ctx, &agentsv1.ListAgentsRequest{
			PageSize:  desiredPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list agents: %w", err)
		}
		agents = append(agents, resp.GetAgents()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return agents, nil
		}
	}
}

func agentOrganizationsFrom(agents []*agentsv1.Agent) (map[string]struct{}, error) {
	organizations := map[string]struct{}{}
	for _, agent := range agents {
		if agent == nil {
			return nil, fmt.Errorf("agent is nil")
		}
		meta := agent.GetMeta()
		if meta == nil {
			return nil, fmt.Errorf("agent meta missing")
		}
		if _, err := uuidutil.ParseUUID(strings.TrimSpace(meta.GetId()), "agent.meta.id"); err != nil {
			return nil, err
		}
		orgID := strings.TrimSpace(agent.GetOrganizationId())
		parsedOrgID, err := uuidutil.ParseUUID(orgID, "agent.organization_id")
		if err != nil {
			return nil, err
		}
		organizations[parsedOrgID.String()] = struct{}{}
	}
	return organizations, nil
}

// platformContext names the Orchestrator's own identity, for the callees that
// require a caller rather than serving an absent one as the platform.
func (r *Reconciler) platformContext(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(
		identityMetadataKey, r.platformIdentityID.String(),
		identityTypeMetadataKey, platformIdentityType,
	))
}

const (
	identityMetadataKey     = "x-identity-id"
	identityTypeMetadataKey = "x-identity-type"
	platformIdentityType    = "platform"
)
