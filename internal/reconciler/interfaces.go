package reconciler

import (
	"context"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	exposev1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/expose/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc"
)

// exposeClient answers whether a port exposure stands on a workload. An exposed
// port is a standing request to be reachable, which a frozen guest is not, so
// an exposure keeps its workload awake.
type exposeClient interface {
	ListExposures(context.Context, *exposev1.ListExposuresRequest, ...grpc.CallOption) (*exposev1.ListExposuresResponse, error)
}

type agentsClient interface {
	GetAgent(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error)
	GetEnvironment(context.Context, *agentsv1.GetEnvironmentRequest, ...grpc.CallOption) (*agentsv1.GetEnvironmentResponse, error)
	ListAgents(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error)
	ListInstances(context.Context, *agentsv1.ListInstancesRequest, ...grpc.CallOption) (*agentsv1.ListInstancesResponse, error)
	GetUnackedInboxItems(context.Context, *agentsv1.GetUnackedInboxItemsRequest, ...grpc.CallOption) (*agentsv1.GetUnackedInboxItemsResponse, error)
	SetSandboxLayoutDirectories(context.Context, *agentsv1.SetSandboxLayoutDirectoriesRequest, ...grpc.CallOption) (*agentsv1.SetSandboxLayoutDirectoriesResponse, error)
	PauseInstance(context.Context, *agentsv1.PauseInstanceRequest, ...grpc.CallOption) (*agentsv1.PauseInstanceResponse, error)
	// Read for a hibernated workload: an instance that is paused or terminated
	// will never wake it.
	GetInstance(context.Context, *agentsv1.GetInstanceRequest, ...grpc.CallOption) (*agentsv1.GetInstanceResponse, error)
	// The inbox check made immediately before a workload is hibernated.
	GetUnackedInboxCount(context.Context, *agentsv1.GetUnackedInboxCountRequest, ...grpc.CallOption) (*agentsv1.GetUnackedInboxCountResponse, error)
	GetSandbox(context.Context, *agentsv1.GetSandboxRequest, ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error)
	ListSandboxes(context.Context, *agentsv1.ListSandboxesRequest, ...grpc.CallOption) (*agentsv1.ListSandboxesResponse, error)
	UpdateSandboxRuntimeState(context.Context, *agentsv1.UpdateSandboxRuntimeStateRequest, ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error)
	DeleteSandbox(context.Context, *agentsv1.DeleteSandboxRequest, ...grpc.CallOption) (*agentsv1.DeleteSandboxResponse, error)
	// Read for a volume definition's TTL while reconciling provisioned disks.
	GetVolume(context.Context, *agentsv1.GetVolumeRequest, ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error)
}

type runnersClient interface {
	ListRunners(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error)
	GetRunner(context.Context, *runnersv1.GetRunnerRequest, ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error)
	CreateWorkload(context.Context, *runnersv1.CreateWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error)
	UpdateWorkload(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error)
	ListWorkloads(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error)
	ListWorkloadsByThread(context.Context, *runnersv1.ListWorkloadsByThreadRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error)
	ListWorkloadsByAgentInstance(context.Context, *runnersv1.ListWorkloadsByAgentInstanceRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByAgentInstanceResponse, error)
	CreateVolume(context.Context, *runnersv1.CreateVolumeRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeResponse, error)
	GetVolume(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error)
	UpdateVolume(context.Context, *runnersv1.UpdateVolumeRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error)
	ListVolumes(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error)
	ListVolumesByThread(context.Context, *runnersv1.ListVolumesByThreadRequest, ...grpc.CallOption) (*runnersv1.ListVolumesByThreadResponse, error)
	ListVolumesByAgentInstance(context.Context, *runnersv1.ListVolumesByAgentInstanceRequest, ...grpc.CallOption) (*runnersv1.ListVolumesByAgentInstanceResponse, error)
	BatchUpdateWorkloadSampledAt(context.Context, *runnersv1.BatchUpdateWorkloadSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error)
	BatchUpdateVolumeSampledAt(context.Context, *runnersv1.BatchUpdateVolumeSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateVolumeSampledAtResponse, error)
}
