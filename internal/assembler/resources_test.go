package assembler

import (
	"context"
	"errors"
	"testing"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

func TestAssemblerAggregatesResourceRequests(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	mcpID := uuid.New()
	hookID := uuid.New()

	agent := &agentsv1.Agent{
		Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
		OrganizationId: "org-1",
		Name:           "agent",
		Role:           "assistant",
		Model:          "gpt",
		Image:          "agent-image",
		InitImage:      "agent-init",
		Resources: &agentsv1.ComputeResources{
			RequestsCpu:    "250m",
			RequestsMemory: "512Mi",
		},
	}

	mcp := &agentsv1.Mcp{
		Meta:      &agentsv1.EntityMeta{Id: mcpID.String()},
		Name:      "mcp",
		Image:     "mcp-image",
		Command:   "mcp run",
		Resources: &agentsv1.ComputeResources{RequestsCpu: "500m", RequestsMemory: "1Gi"},
	}
	hook := &agentsv1.Hook{
		Meta:      &agentsv1.EntityMeta{Id: hookID.String()},
		Image:     "hook-image",
		Function:  "hook run",
		Resources: &agentsv1.ComputeResources{RequestsCpu: "100m", RequestsMemory: "256Mi"},
	}

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListSkillsFunc: func(context.Context, *agentsv1.ListSkillsRequest, ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(context.Context, *agentsv1.ListInitScriptsRequest, ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(context.Context, *agentsv1.ListVolumeAttachmentsRequest, ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(context.Context, *agentsv1.ListImagePullSecretAttachmentsRequest, ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListMcpsFunc: func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{mcp}}, nil
		},
		ListHooksFunc: func(context.Context, *agentsv1.ListHooksRequest, ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{Hooks: []*agentsv1.Hook{hook}}, nil
		},
	}

	cfg := config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}
	assembler := New(agentsClient, &testutil.FakeSecretsClient{}, &cfg)

	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if result.AllocatedCPUMillicores != 850 {
		t.Fatalf("expected cpu millicores 850, got %d", result.AllocatedCPUMillicores)
	}
	expectedRAM := int64(512<<20 + 1<<30 + 256<<20)
	if result.AllocatedRAMBytes != expectedRAM {
		t.Fatalf("expected ram bytes %d, got %d", expectedRAM, result.AllocatedRAMBytes)
	}
}
