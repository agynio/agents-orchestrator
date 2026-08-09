package assembler

import (
	"context"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	llmv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/llm/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	"google.golang.org/grpc"
)

type agentsClient interface {
	GetAgent(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error)
	GetEnvironment(context.Context, *agentsv1.GetEnvironmentRequest, ...grpc.CallOption) (*agentsv1.GetEnvironmentResponse, error)
	ListMcps(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error)
	ListEnvs(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error)
	ListVolumes(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error)
}

type secretsClient interface {
	ResolveSecret(context.Context, *secretsv1.ResolveSecretRequest, ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error)
}

type runnersClient interface {
	ListFlavors(context.Context, *runnersv1.ListFlavorsRequest, ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error)
}

// llmClient answers which vendors a workload has a subscription for. Optional:
// without it a native-mode environment fails assembly rather than starting a
// workload whose first model call would fail.
type llmClient interface {
	ListSubscriptionAttachments(context.Context, *llmv1.ListSubscriptionAttachmentsRequest, ...grpc.CallOption) (*llmv1.ListSubscriptionAttachmentsResponse, error)
}
