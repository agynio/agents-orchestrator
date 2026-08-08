package assembler

import (
	"context"
	"errors"
	"strings"
	"testing"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	llmv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/llm/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	"google.golang.org/grpc"
)

type fakeLLMClient struct {
	attachments []*llmv1.SubscriptionAttachment
	err         error
	requests    []*llmv1.ListSubscriptionAttachmentsRequest
}

func (f *fakeLLMClient) ListSubscriptionAttachments(_ context.Context, req *llmv1.ListSubscriptionAttachmentsRequest, _ ...grpc.CallOption) (*llmv1.ListSubscriptionAttachmentsResponse, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return nil, f.err
	}
	// Only answer the scope the attachment targets, so agent-scope and
	// environment-scope filtering is actually exercised.
	var matching []*llmv1.SubscriptionAttachment
	for _, a := range f.attachments {
		switch {
		case req.AgentId != nil && a.GetAgentId() == req.GetAgentId():
			matching = append(matching, a)
		case req.EnvironmentId != nil && a.GetEnvironmentId() == req.GetEnvironmentId():
			matching = append(matching, a)
		}
	}
	return &llmv1.ListSubscriptionAttachmentsResponse{SubscriptionAttachments: matching}, nil
}

func nativeEnvironment(id string) *agentsv1.Environment {
	return &agentsv1.Environment{
		Meta:           &agentsv1.EntityMeta{Id: id},
		OrganizationId: "org-1",
		LlmMode:        agentsv1.LLMMode_LLM_MODE_NATIVE,
	}
}

func envValue(vars []*runnerv1.EnvVar, name string) (string, bool) {
	for _, v := range vars {
		if v.GetName() == name {
			return v.GetValue(), true
		}
	}
	return "", false
}

// Platform mode must not reach for the LLM service at all, and must stamp
// nothing: its vendor traffic is not intercepted.
func TestResolveLLMModePlatformStampsNothing(t *testing.T) {
	llm := &fakeLLMClient{}
	a := &Assembler{llm: llm}

	mode, err := a.resolveLLMMode(context.Background(), &agentsv1.Environment{
		Meta: &agentsv1.EntityMeta{Id: "env-1"},
	}, "agent-1", "")
	if err != nil {
		t.Fatalf("resolve llm mode: %v", err)
	}
	if mode.Native {
		t.Fatal("expected platform mode")
	}
	if len(mode.RoleAttributes) != 0 {
		t.Fatalf("expected no role attributes, got %v", mode.RoleAttributes)
	}
	if len(llm.requests) != 0 {
		t.Fatalf("expected no LLM calls in platform mode, got %d", len(llm.requests))
	}
	if value, _ := envValue(mode.EnvVars, "LLM_MODE"); value != "platform" {
		t.Fatalf("expected LLM_MODE=platform, got %q", value)
	}
}

func TestResolveLLMModeNativeStampsAttributeAndPlaceholder(t *testing.T) {
	llm := &fakeLLMClient{attachments: []*llmv1.SubscriptionAttachment{{
		Vendor:          llmv1.Vendor_VENDOR_ANTHROPIC,
		PlaceholderKind: llmv1.PlaceholderKind_PLACEHOLDER_KIND_ENV, PlaceholderEnv: "CLAUDE_CODE_OAUTH_TOKEN",
		Target: &llmv1.SubscriptionAttachment_EnvironmentId{EnvironmentId: "env-1"},
	}}}
	a := &Assembler{llm: llm}

	mode, err := a.resolveLLMMode(context.Background(), nativeEnvironment("env-1"), "", "claude-sonnet-4-6")
	if err != nil {
		t.Fatalf("resolve llm mode: %v", err)
	}
	if !mode.Native {
		t.Fatal("expected native mode")
	}
	if len(mode.RoleAttributes) != 1 || mode.RoleAttributes[0] != "llm-native-anthropic" {
		t.Fatalf("expected llm-native-claude, got %v", mode.RoleAttributes)
	}
	if value, ok := envValue(mode.EnvVars, "CLAUDE_CODE_OAUTH_TOKEN"); !ok || value != placeholderCredential {
		t.Fatalf("expected the placeholder credential, got %q (present=%t)", value, ok)
	}
	if value, _ := envValue(mode.EnvVars, "LLM_MODEL_NAME"); value != "claude-sonnet-4-6" {
		t.Fatalf("expected the pinned model name, got %q", value)
	}
}

// A sandbox pins no model and has no agent scope to consult.
func TestResolveLLMModeSandboxPinsNoModel(t *testing.T) {
	llm := &fakeLLMClient{attachments: []*llmv1.SubscriptionAttachment{{
		Vendor:          llmv1.Vendor_VENDOR_ANTHROPIC,
		PlaceholderKind: llmv1.PlaceholderKind_PLACEHOLDER_KIND_ENV, PlaceholderEnv: "CLAUDE_CODE_OAUTH_TOKEN",
		Target: &llmv1.SubscriptionAttachment_EnvironmentId{EnvironmentId: "env-1"},
	}}}
	a := &Assembler{llm: llm}

	mode, err := a.resolveLLMMode(context.Background(), nativeEnvironment("env-1"), "", "")
	if err != nil {
		t.Fatalf("resolve llm mode: %v", err)
	}
	if _, ok := envValue(mode.EnvVars, "LLM_MODEL_NAME"); ok {
		t.Fatal("expected no pinned model for a sandbox")
	}
	for _, req := range llm.requests {
		if req.AgentId != nil {
			t.Fatalf("expected no agent-scoped lookup for a sandbox, got %v", req)
		}
	}
}

// A vendor attached at both scopes contributes one attribute and one
// placeholder, not two.
func TestResolveLLMModeAgentScopeShadowsEnvironment(t *testing.T) {
	llm := &fakeLLMClient{attachments: []*llmv1.SubscriptionAttachment{
		{
			Vendor:          llmv1.Vendor_VENDOR_ANTHROPIC,
			PlaceholderKind: llmv1.PlaceholderKind_PLACEHOLDER_KIND_ENV, PlaceholderEnv: "CLAUDE_CODE_OAUTH_TOKEN",
			Target: &llmv1.SubscriptionAttachment_AgentId{AgentId: "agent-1"},
		},
		{
			Vendor:          llmv1.Vendor_VENDOR_ANTHROPIC,
			PlaceholderKind: llmv1.PlaceholderKind_PLACEHOLDER_KIND_ENV, PlaceholderEnv: "CLAUDE_CODE_OAUTH_TOKEN",
			Target: &llmv1.SubscriptionAttachment_EnvironmentId{EnvironmentId: "env-1"},
		},
	}}
	a := &Assembler{llm: llm}

	mode, err := a.resolveLLMMode(context.Background(), nativeEnvironment("env-1"), "agent-1", "")
	if err != nil {
		t.Fatalf("resolve llm mode: %v", err)
	}
	if len(mode.RoleAttributes) != 1 {
		t.Fatalf("expected one attribute, got %v", mode.RoleAttributes)
	}
	placeholders := 0
	for _, v := range mode.EnvVars {
		if v.GetName() == "CLAUDE_CODE_OAUTH_TOKEN" {
			placeholders++
		}
	}
	if placeholders != 1 {
		t.Fatalf("expected one placeholder, got %d", placeholders)
	}
}

// Reported at assembly rather than at the workload's first model call, which is
// the only other place it would surface.
func TestResolveLLMModeNativeWithoutSubscriptionFails(t *testing.T) {
	a := &Assembler{llm: &fakeLLMClient{}}

	_, err := a.resolveLLMMode(context.Background(), nativeEnvironment("env-1"), "agent-1", "")
	if err == nil {
		t.Fatal("expected assembly to fail")
	}
	if !strings.Contains(err.Error(), "no subscription is attached") {
		t.Fatalf("expected a descriptive error, got %v", err)
	}
}

func TestResolveLLMModeNativeWithoutLLMClientFails(t *testing.T) {
	a := &Assembler{}

	_, err := a.resolveLLMMode(context.Background(), nativeEnvironment("env-1"), "agent-1", "")
	if err == nil {
		t.Fatal("expected assembly to fail")
	}
	if !strings.Contains(err.Error(), "LLM service is not configured") {
		t.Fatalf("expected a descriptive error, got %v", err)
	}
}

func TestResolveLLMModePropagatesLookupFailure(t *testing.T) {
	a := &Assembler{llm: &fakeLLMClient{err: errors.New("unavailable")}}

	if _, err := a.resolveLLMMode(context.Background(), nativeEnvironment("env-1"), "", ""); err == nil {
		t.Fatal("expected the lookup failure to fail assembly")
	}
}

// The agent scope is looked up by the agent's own id. Passing anything else --
// the environment id is the near miss, since both are on the same struct --
// silently finds no agent-scoped attachment, so an agent overriding its
// environment's vendor would fall back to the environment's subscription, or
// fail assembly outright when the agent's is the only one attached.
func TestResolveLLMModeQueriesTheAgentScopeByAgentID(t *testing.T) {
	const agentID = "agent-1"
	const environmentID = "env-1"

	client := &fakeLLMClient{
		attachments: []*llmv1.SubscriptionAttachment{
			{
				Meta:            &llmv1.EntityMeta{Id: "att-1"},
				SubscriptionId:  "sub-1",
				Vendor:          llmv1.Vendor_VENDOR_ANTHROPIC,
				PlaceholderKind: llmv1.PlaceholderKind_PLACEHOLDER_KIND_ENV, PlaceholderEnv: "CLAUDE_CODE_OAUTH_TOKEN",
				Target: &llmv1.SubscriptionAttachment_AgentId{AgentId: agentID},
			},
		},
	}
	assembler := &Assembler{llm: client}
	environment := &agentsv1.Environment{
		Meta:    &agentsv1.EntityMeta{Id: environmentID},
		LlmMode: agentsv1.LLMMode_LLM_MODE_NATIVE,
	}

	mode, err := assembler.resolveLLMMode(context.Background(), environment, agentID, "")
	if err != nil {
		t.Fatalf("resolve llm mode: %v", err)
	}
	if !mode.Native {
		t.Fatal("expected native mode")
	}
	if len(mode.RoleAttributes) != 1 || mode.RoleAttributes[0] != "llm-native-anthropic" {
		t.Fatalf("role attributes = %v, want the agent-scoped vendor", mode.RoleAttributes)
	}

	// The agent filter has to carry the agent id, not the environment's.
	var sawAgentFilter bool
	for _, req := range client.requests {
		if req.AgentId != nil {
			sawAgentFilter = true
			if req.GetAgentId() != agentID {
				t.Fatalf("agent filter = %q, want %q", req.GetAgentId(), agentID)
			}
		}
	}
	if !sawAgentFilter {
		t.Fatal("agent scope was never queried")
	}
}

// A file placeholder is agynd's to write -- it lands at a CLI-specific path
// under HOME that the orchestrator cannot resolve. Injecting it as a variable
// here would put a JSON document in the environment and still leave the file
// the CLI actually reads missing.
func TestResolveLLMModeLeavesFilePlaceholdersToAgynd(t *testing.T) {
	client := &fakeLLMClient{
		attachments: []*llmv1.SubscriptionAttachment{
			{
				Meta:                &llmv1.EntityMeta{Id: "att-1"},
				SubscriptionId:      "sub-1",
				Vendor:              llmv1.Vendor_VENDOR_OPENAI,
				PlaceholderKind:     llmv1.PlaceholderKind_PLACEHOLDER_KIND_FILE,
				PlaceholderPath:     ".codex/auth.json",
				PlaceholderContents: `{"tokens":{"access_token":"placeholder"}}`,
				Target:              &llmv1.SubscriptionAttachment_EnvironmentId{EnvironmentId: "env-1"},
			},
		},
	}
	assembler := &Assembler{llm: client}
	environment := &agentsv1.Environment{
		Meta:    &agentsv1.EntityMeta{Id: "env-1"},
		LlmMode: agentsv1.LLMMode_LLM_MODE_NATIVE,
	}

	mode, err := assembler.resolveLLMMode(context.Background(), environment, "", "")
	if err != nil {
		t.Fatalf("resolve llm mode: %v", err)
	}
	// The attribute is still stamped: interception is what the role attribute
	// opts into, and that is the orchestrator's either way.
	if len(mode.RoleAttributes) != 1 || mode.RoleAttributes[0] != "llm-native-openai" {
		t.Fatalf("role attributes = %v", mode.RoleAttributes)
	}
	for _, env := range mode.EnvVars {
		if env.GetName() != "LLM_MODE" {
			t.Fatalf("file placeholder leaked into the container environment as %s", env.GetName())
		}
	}
}
