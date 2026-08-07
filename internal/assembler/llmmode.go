package assembler

import (
	"context"
	"fmt"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	llmv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/llm/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
)

// The placeholder is shaped like a real credential so the agent CLI's own local
// validation passes. It is never used: the LLM Proxy strips the Authorization
// header the CLI builds from it and injects the real subscription token.
const placeholderCredential = "agyn-placeholder-not-a-credential"

// nativeRoleAttributePrefix opts a workload into vendor interception. It is the
// only thing that does -- no OpenZiti service or policy is created per workload,
// per environment, or per attachment.
const nativeRoleAttributePrefix = "llm-native-"

// LLMMode is what a workload needs to reach a model: the mode itself, the role
// attributes that opt its identity into interception, and the environment
// variables the container carries.
type LLMMode struct {
	Native bool
	// RoleAttributes are stamped on the workload's OpenZiti identity.
	RoleAttributes []string
	// EnvVars are injected into the main container. The placeholder credential
	// is here rather than in agynd's subprocess environment because a sandbox
	// shell comes from the runner's Exec against the pod and inherits the
	// container spec's environment, never one agynd assembled.
	EnvVars []*runnerv1.EnvVar
}

// resolveLLMMode asks the LLM service which vendors have a subscription
// attached at either scope, and turns that into role attributes and container
// environment. agentID is empty for a sandbox, which runs no agent class.
func (a *Assembler) resolveLLMMode(ctx context.Context, environment *agentsv1.Environment, agentID string, modelName string) (LLMMode, error) {
	if environment == nil || environment.GetLlmMode() != agentsv1.LLMMode_LLM_MODE_NATIVE {
		// Platform mode: no attribute, no placeholder, and vendor traffic is not
		// intercepted at all.
		return LLMMode{EnvVars: []*runnerv1.EnvVar{{Name: "LLM_MODE", Value: "platform"}}}, nil
	}

	environmentID := environment.GetMeta().GetId()
	if a.llm == nil {
		return LLMMode{}, fmt.Errorf("environment %s is in native LLM mode but the LLM service is not configured", environmentID)
	}

	attachments, err := a.listSubscriptionAttachments(ctx, environment.GetOrganizationId(), agentID, environmentID)
	if err != nil {
		return LLMMode{}, err
	}
	if len(attachments) == 0 {
		// Reported here rather than at the workload's first model call, which is
		// the only other place it would surface.
		return LLMMode{}, fmt.Errorf(
			"environment %s is in native LLM mode but no subscription is attached for any vendor", environmentID)
	}

	mode := LLMMode{
		Native:  true,
		EnvVars: []*runnerv1.EnvVar{{Name: "LLM_MODE", Value: "native"}},
	}
	if modelName != "" {
		mode.EnvVars = append(mode.EnvVars, &runnerv1.EnvVar{Name: "LLM_MODEL_NAME", Value: modelName})
	}

	// An agent-scoped attachment shadows the environment's for the same vendor,
	// so a vendor contributes one attribute and one placeholder however many
	// attachments name it.
	seen := map[llmv1.Vendor]bool{}
	for _, attachment := range attachments {
		vendor := attachment.GetVendor()
		if seen[vendor] {
			continue
		}
		seen[vendor] = true

		name, ok := vendorRoleAttribute(vendor)
		if !ok {
			continue
		}
		mode.RoleAttributes = append(mode.RoleAttributes, name)
		// The placeholder variable name comes from the LLM service rather than a
		// vendor table held here; a vendor with no placeholder yields none.
		if env := attachment.GetPlaceholderEnv(); env != "" {
			mode.EnvVars = append(mode.EnvVars, &runnerv1.EnvVar{Name: env, Value: placeholderCredential})
		}
	}
	return mode, nil
}

func vendorRoleAttribute(vendor llmv1.Vendor) (string, bool) {
	switch vendor {
	case llmv1.Vendor_VENDOR_CLAUDE:
		return nativeRoleAttributePrefix + "claude", true
	case llmv1.Vendor_VENDOR_CODEX:
		return nativeRoleAttributePrefix + "codex", true
	default:
		return "", false
	}
}

// listSubscriptionAttachments collects both scopes. A sandbox has no agent, so
// it sees the environment's attachments and nothing else.
func (a *Assembler) listSubscriptionAttachments(ctx context.Context, organizationID, agentID, environmentID string) ([]*llmv1.SubscriptionAttachment, error) {
	var attachments []*llmv1.SubscriptionAttachment

	if agentID != "" {
		agentScoped, err := a.listAttachmentsFiltered(ctx, &llmv1.ListSubscriptionAttachmentsRequest{
			OrganizationId: organizationID,
			AgentId:        &agentID,
		})
		if err != nil {
			return nil, err
		}
		attachments = append(attachments, agentScoped...)
	}

	environmentScoped, err := a.listAttachmentsFiltered(ctx, &llmv1.ListSubscriptionAttachmentsRequest{
		OrganizationId: organizationID,
		EnvironmentId:  &environmentID,
	})
	if err != nil {
		return nil, err
	}
	return append(attachments, environmentScoped...), nil
}

func (a *Assembler) listAttachmentsFiltered(ctx context.Context, req *llmv1.ListSubscriptionAttachmentsRequest) ([]*llmv1.SubscriptionAttachment, error) {
	var out []*llmv1.SubscriptionAttachment
	req.PageSize = listPageSize
	for {
		resp, err := a.llm.ListSubscriptionAttachments(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("list subscription attachments: %w", err)
		}
		out = append(out, resp.GetSubscriptionAttachments()...)
		if resp.GetNextPageToken() == "" {
			return out, nil
		}
		req.PageToken = resp.GetNextPageToken()
	}
}
