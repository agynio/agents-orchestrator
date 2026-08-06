package reconciler

import (
	"strings"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
)

func workloadAgentInstanceID(workload interface{ GetAgentInstanceId() string }) string {
	return workload.GetAgentInstanceId()
}

func workloadAgentClassID(workload interface{ GetAgentClassId() string }) string {
	return workload.GetAgentClassId()
}

func volumeAgentInstanceID(volume interface{ GetAgentInstanceId() string }) string {
	return volume.GetAgentInstanceId()
}

// workloadRunnerIdentityID is the identity the Orchestrator presents to a
// runner when acting on a workload. An agent workload is reached as its
// instance; a sandbox has no instance and is reached as its owner.
func workloadRunnerIdentityID(workload *runnersv1.Workload) string {
	if workload.GetOwnerKind() == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		return strings.TrimSpace(workload.GetOwnerId())
	}
	return strings.TrimSpace(workload.GetAgentInstanceId())
}
