package assembler

import (
	"context"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	"google.golang.org/grpc"
)

type agentsClient interface {
	GetAgent(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error)
	GetEnvironment(context.Context, *agentsv1.GetEnvironmentRequest, ...grpc.CallOption) (*agentsv1.GetEnvironmentResponse, error)
	ListMcps(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error)
	ListEnvs(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error)
	ListVolumeAttachments(context.Context, *agentsv1.ListVolumeAttachmentsRequest, ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error)
	GetVolume(context.Context, *agentsv1.GetVolumeRequest, ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error)
}

type secretsClient interface {
	ResolveSecret(context.Context, *secretsv1.ResolveSecretRequest, ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error)
}

type runnersClient interface {
	ListFlavors(context.Context, *runnersv1.ListFlavorsRequest, ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error)
}
