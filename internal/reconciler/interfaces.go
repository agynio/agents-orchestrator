package reconciler

import (
	"context"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc"
)

type agentsClient interface {
	ListAgents(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error)
	GetVolume(context.Context, *agentsv1.GetVolumeRequest, ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error)
	GetSandbox(context.Context, *agentsv1.GetSandboxRequest, ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error)
	ListSandboxes(context.Context, *agentsv1.ListSandboxesRequest, ...grpc.CallOption) (*agentsv1.ListSandboxesResponse, error)
	UpdateSandboxRuntimeState(context.Context, *agentsv1.UpdateSandboxRuntimeStateRequest, ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error)
	DeleteSandbox(context.Context, *agentsv1.DeleteSandboxRequest, ...grpc.CallOption) (*agentsv1.DeleteSandboxResponse, error)
}

type runnersClient interface {
	ListRunners(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error)
	GetRunner(context.Context, *runnersv1.GetRunnerRequest, ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error)
	CreateWorkload(context.Context, *runnersv1.CreateWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error)
	UpdateWorkload(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error)
	ListWorkloads(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error)
	ListWorkloadsByThread(context.Context, *runnersv1.ListWorkloadsByThreadRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error)
	CreateVolume(context.Context, *runnersv1.CreateVolumeRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeResponse, error)
	GetVolume(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error)
	UpdateVolume(context.Context, *runnersv1.UpdateVolumeRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error)
	ListVolumes(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error)
	ListVolumesByThread(context.Context, *runnersv1.ListVolumesByThreadRequest, ...grpc.CallOption) (*runnersv1.ListVolumesByThreadResponse, error)
	BatchUpdateWorkloadSampledAt(context.Context, *runnersv1.BatchUpdateWorkloadSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error)
	BatchUpdateVolumeSampledAt(context.Context, *runnersv1.BatchUpdateVolumeSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateVolumeSampledAtResponse, error)
}
