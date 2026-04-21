package reconciler

import (
	"context"
	"fmt"
	"strings"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

const identityMetadataKey = "x-identity-id"

func runnerIdentityContext(ctx context.Context, identityID string) (context.Context, error) {
	identityID = strings.TrimSpace(identityID)
	if _, err := uuidutil.ParseUUID(identityID, "identity_id"); err != nil {
		return nil, err
	}
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}
	md = md.Copy()
	md.Set(identityMetadataKey, identityID)
	return metadata.NewOutgoingContext(ctx, md), nil
}

func (r *Reconciler) runnerIdentityContextForAgent(ctx context.Context, agentID uuid.UUID) (context.Context, error) {
	return runnerIdentityContext(ctx, agentID.String())
}

func (r *Reconciler) agentIdentityByOrg(ctx context.Context) (map[string]string, error) {
	if r.agents == nil {
		return nil, fmt.Errorf("agents client is nil")
	}
	orgIdentities := map[string]string{}
	pageToken := ""
	for {
		resp, err := r.agents.ListAgents(ctx, &agentsv1.ListAgentsRequest{
			PageSize:  desiredPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list agents: %w", err)
		}
		for _, agent := range resp.GetAgents() {
			if agent == nil {
				return nil, fmt.Errorf("agent is nil")
			}
			meta := agent.GetMeta()
			if meta == nil {
				return nil, fmt.Errorf("agent meta missing")
			}
			agentID := strings.TrimSpace(meta.GetId())
			parsedAgentID, err := uuidutil.ParseUUID(agentID, "agent.meta.id")
			if err != nil {
				return nil, err
			}
			orgID := strings.TrimSpace(agent.GetOrganizationId())
			parsedOrgID, err := uuidutil.ParseUUID(orgID, "agent.organization_id")
			if err != nil {
				return nil, err
			}
			orgIDValue := parsedOrgID.String()
			if _, ok := orgIdentities[orgIDValue]; ok {
				continue
			}
			orgIdentities[orgIDValue] = parsedAgentID.String()
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return orgIdentities, nil
		}
	}
}
