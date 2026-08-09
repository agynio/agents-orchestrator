package subscriber

import (
	"context"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	"google.golang.org/grpc"
)

type agentsClient interface {
	ListInstances(context.Context, *agentsv1.ListInstancesRequest, ...grpc.CallOption) (*agentsv1.ListInstancesResponse, error)
	ListSandboxes(context.Context, *agentsv1.ListSandboxesRequest, ...grpc.CallOption) (*agentsv1.ListSandboxesResponse, error)
}
