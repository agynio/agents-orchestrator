package reconciler

func workloadAgentInstanceID(workload interface{ GetAgentInstanceId() string }) string {
	return workload.GetAgentInstanceId()
}

func workloadAgentClassID(workload interface{ GetAgentClassId() string }) string {
	return workload.GetAgentClassId()
}

func volumeAgentInstanceID(volume interface{ GetAgentInstanceId() string }) string {
	return volume.GetAgentInstanceId()
}
