package assembler

import (
	"context"
	"fmt"
	"strings"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

// ImageGrant is what a workload's pull credential is scoped to: the catalog
// images it may pull and the organization they are billed to.
type ImageGrant struct {
	OrganizationID string
	ImageIDs       []string
}

// AgentImageGrant resolves the grant an agent workload's credential carries
// without assembling its spec. The wake of a hibernated workload mints a fresh
// credential -- the one it started with was revoked when it went to sleep --
// and the images are the ones the start resolved: the environment's workspace
// and agent runtime images and every MCP sidecar's. Without a catalog there is
// nothing to grant, as there was nothing to mint at the start.
func (a *Assembler) AgentImageGrant(ctx context.Context, agentID uuid.UUID) (*ImageGrant, error) {
	agent, err := a.fetchAgent(ctx, agentID)
	if err != nil {
		return nil, err
	}
	grant := &ImageGrant{OrganizationID: agent.GetOrganizationId()}
	rewriter := newImageRewriter(a.images, a.organizations, a.cfg.ImageProxyHost)
	if !rewriter.enabled() {
		return grant, nil
	}
	if environmentIDValue := strings.TrimSpace(agent.GetEnvironmentId()); environmentIDValue != "" {
		environmentID, err := uuidutil.ParseUUID(environmentIDValue, "agent.environment_id")
		if err != nil {
			return nil, err
		}
		environment, err := a.fetchEnvironment(ctx, environmentID)
		if err != nil {
			return nil, err
		}
		if err := grantEnvironmentImages(ctx, rewriter, environment); err != nil {
			return nil, err
		}
	}
	mcps, err := a.listMcps(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("list mcps: %w", err)
	}
	for _, mcp := range mcps {
		if mcp.GetImageId() == "" {
			continue
		}
		if _, err := rewriter.Rewrite(ctx, mcp.GetImageId(), mcp.GetImageTag()); err != nil {
			return nil, fmt.Errorf("mcp %s image: %w", mcp.GetMeta().GetId(), err)
		}
	}
	grant.ImageIDs = rewriter.GrantedImageIDs()
	return grant, nil
}

// SandboxImageGrant is the sandbox counterpart: the environment's images, and
// nothing else, because a sandbox runs the environment's contributions alone.
func (a *Assembler) SandboxImageGrant(ctx context.Context, sandbox *agentsv1.Sandbox) (*ImageGrant, error) {
	if sandbox == nil {
		return nil, fmt.Errorf("sandbox missing")
	}
	grant := &ImageGrant{OrganizationID: sandbox.GetOrganizationId()}
	rewriter := newImageRewriter(a.images, a.organizations, a.cfg.ImageProxyHost)
	if !rewriter.enabled() {
		return grant, nil
	}
	environmentID, err := uuidutil.ParseUUID(sandbox.GetEnvironmentId(), "sandbox.environment_id")
	if err != nil {
		return nil, err
	}
	environment, err := a.fetchEnvironment(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	if err := grantEnvironmentImages(ctx, rewriter, environment); err != nil {
		return nil, err
	}
	grant.ImageIDs = rewriter.GrantedImageIDs()
	return grant, nil
}

func grantEnvironmentImages(ctx context.Context, rewriter *imageRewriter, environment *agentsv1.Environment) error {
	environmentID := environment.GetMeta().GetId()
	if environment.GetWorkspaceImageId() != "" {
		if _, err := rewriter.Rewrite(ctx, environment.GetWorkspaceImageId(), environment.GetWorkspaceImageTag()); err != nil {
			return fmt.Errorf("environment %s workspace image: %w", environmentID, err)
		}
	}
	if environment.GetAgentRuntimeImageId() != "" {
		if _, err := rewriter.Rewrite(ctx, environment.GetAgentRuntimeImageId(), environment.GetAgentRuntimeImageTag()); err != nil {
			return fmt.Errorf("environment %s agent runtime image: %w", environmentID, err)
		}
	}
	return nil
}
