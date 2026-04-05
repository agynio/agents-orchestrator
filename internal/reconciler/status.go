package reconciler

import (
	"context"
	"fmt"
	"log"
	"strings"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
)

const (
	dockerStateRunning    = "running"
	dockerStateCreated    = "created"
	dockerStateRestarting = "restarting"
	dockerStateStarting   = "starting"
	dockerStatePaused     = "paused"
	dockerStateExited     = "exited"
	dockerStateDead       = "dead"
	dockerStateRemoving   = "removing"
	dockerStateStopped    = "stopped"
)

func (r *Reconciler) updateWorkloadStatuses(ctx context.Context) error {
	workloads, err := r.listActiveWorkloads(ctx)
	if err != nil {
		return err
	}
	for _, workload := range workloads {
		workloadID := workload.GetMeta().GetId()
		runnerID := workload.GetRunnerId()
		if runnerID == "" {
			log.Printf("reconciler: warn: workload %s missing runner id", workloadID)
			continue
		}
		runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
		if err != nil {
			log.Printf("reconciler: warn: dial runner %s for workload %s: %v", runnerID, workloadID, err)
			continue
		}
		inspectResp, err := runnerClient.InspectWorkload(ctx, &runnerv1.InspectWorkloadRequest{WorkloadId: workloadID})
		if err != nil {
			log.Printf("reconciler: warn: inspect workload %s on runner %s: %v", workloadID, runnerID, err)
			continue
		}
		status, err := workloadStatusFromInspect(inspectResp)
		if err != nil {
			log.Printf("reconciler: warn: map workload %s status: %v", workloadID, err)
			continue
		}
		if status == workload.GetStatus() {
			continue
		}
		if _, err := r.runners.UpdateWorkloadStatus(ctx, &runnersv1.UpdateWorkloadStatusRequest{
			Id:         workloadID,
			Status:     status,
			Containers: workload.GetContainers(),
		}); err != nil {
			log.Printf("reconciler: warn: update workload %s status: %v", workloadID, err)
		}
	}
	return nil
}

func workloadStatusFromInspect(resp *runnerv1.InspectWorkloadResponse) (runnersv1.WorkloadStatus, error) {
	if resp == nil {
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, fmt.Errorf("inspect response missing")
	}
	if resp.GetStateRunning() {
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, nil
	}
	status := strings.ToLower(resp.GetStateStatus())
	switch status {
	case dockerStateRunning:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, nil
	case dockerStateCreated, dockerStateRestarting, dockerStateStarting:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, nil
	case dockerStatePaused:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, nil
	case "pending":
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, nil
	case "succeeded":
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, nil
	case "failed":
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, nil
	case "unknown":
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, fmt.Errorf("pod phase unknown for workload")
	case "", dockerStateExited, dockerStateDead, dockerStateRemoving, dockerStateStopped:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, nil
	default:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, fmt.Errorf("unknown runner state %q", status)
	}
}
