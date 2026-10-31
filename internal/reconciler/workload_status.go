package reconciler

import runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"

// liveWorkloadStatuses are the records that stand for a workload the platform
// still holds: on its way up, running, asleep or on its way into or out of
// sleep, and on its way down. A hibernated workload is live -- it precludes a
// second workload for its owner, keeps its OpenZiti identity and keeps its
// volumes' TTL clock stopped -- it just occupies no compute.
var liveWorkloadStatuses = []runnersv1.WorkloadStatus{
	runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
	runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
	runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING,
	runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED,
	runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING,
	runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
}

func isLiveWorkloadStatus(status runnersv1.WorkloadStatus) bool {
	for _, live := range liveWorkloadStatuses {
		if status == live {
			return true
		}
	}
	return false
}

// isFrozenWorkloadStatus reports whether the workload's containers cannot be
// read: a paused guest answers no exec, and one on its way into or out of the
// pause may stop answering at any moment.
func isFrozenWorkloadStatus(status runnersv1.WorkloadStatus) bool {
	switch status {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING:
		return true
	default:
		return false
	}
}
