package reconciler

import (
	"context"
	"log"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (r *Reconciler) reconcileWorkloads(ctx context.Context) error {
	tracked, err := r.listActiveWorkloads(ctx)
	if err != nil {
		return err
	}
	runnerIDs := map[string]struct{}{}
	workloadsByRunner := make(map[string]map[string]*runnersv1.Workload)
	for _, workload := range tracked {
		runnerID := workload.GetRunnerId()
		if runnerID == "" {
			log.Printf("reconciler: warn: workload %s missing runner id", workload.GetMeta().GetId())
			continue
		}
		workloadID := workload.GetMeta().GetId()
		if workloadID == "" {
			log.Printf("reconciler: warn: workload missing id")
			continue
		}
		runnerIDs[runnerID] = struct{}{}
		if workloadsByRunner[runnerID] == nil {
			workloadsByRunner[runnerID] = map[string]*runnersv1.Workload{}
		}
		workloadsByRunner[runnerID][workloadID] = workload
	}
	if runners, err := r.listRunners(ctx); err != nil {
		log.Printf("reconciler: warn: list runners for workload reconciliation: %v", err)
	} else {
		for _, runner := range runners {
			if runner == nil {
				continue
			}
			runnerID := runner.GetMeta().GetId()
			if runnerID == "" {
				continue
			}
			runnerIDs[runnerID] = struct{}{}
		}
	}

	for runnerID := range runnerIDs {
		runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
		if err != nil {
			log.Printf("reconciler: warn: dial runner %s for workload reconciliation: %v", runnerID, err)
			continue
		}
		resp, err := runnerClient.ListWorkloads(ctx, &runnerv1.ListWorkloadsRequest{})
		if err != nil {
			log.Printf("reconciler: warn: list workloads for runner %s: %v", runnerID, err)
			continue
		}
		runnerWorkloads := make(map[string]*runnerv1.WorkloadListItem)
		for _, item := range resp.GetWorkloads() {
			if item == nil {
				continue
			}
			workloadKey := item.GetWorkloadKey()
			if workloadKey == "" {
				log.Printf("reconciler: warn: runner %s workload missing workload_key", runnerID)
				continue
			}
			if _, ok := runnerWorkloads[workloadKey]; ok {
				log.Printf("reconciler: warn: runner %s workload_key %s duplicated", runnerID, workloadKey)
				continue
			}
			runnerWorkloads[workloadKey] = item
		}

		trackedWorkloads := workloadsByRunner[runnerID]
		for workloadID, workload := range trackedWorkloads {
			item, ok := runnerWorkloads[workloadID]
			if !ok {
				if err := r.handleMissingRunnerWorkload(ctx, workload); err != nil {
					log.Printf("reconciler: warn: handle missing workload %s: %v", workloadID, err)
				}
				continue
			}
			delete(runnerWorkloads, workloadID)
			if err := r.handlePresentRunnerWorkload(ctx, runnerClient, workload, item); err != nil {
				log.Printf("reconciler: warn: handle workload %s on runner %s: %v", workloadID, runnerID, err)
			}
		}

		for _, item := range runnerWorkloads {
			instanceID := normalizeRunnerWorkloadID(item.GetInstanceId())
			if instanceID == "" {
				log.Printf("reconciler: warn: runner %s orphan workload missing instance id", runnerID)
				continue
			}
			if err := r.stopRunnerWorkload(ctx, runnerClient, instanceID); err != nil {
				log.Printf("reconciler: warn: stop orphan workload %s on runner %s: %v", instanceID, runnerID, err)
			}
		}
	}
	return nil
}

func (r *Reconciler) handleMissingRunnerWorkload(ctx context.Context, workload *runnersv1.Workload) error {
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		return nil
	}
	missingAt := timestamppb.New(time.Now().UTC())
	switch workload.GetStatus() {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
		_, err := r.runners.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{
			Id:        workloadID,
			Status:    &status,
			RemovedAt: missingAt,
		})
		return err
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
		_, err := r.runners.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{
			Id:        workloadID,
			Status:    &status,
			RemovedAt: missingAt,
		})
		return err
	default:
		return nil
	}
}

func (r *Reconciler) handlePresentRunnerWorkload(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, workload *runnersv1.Workload, item *runnerv1.WorkloadListItem) error {
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		return nil
	}
	instanceID := normalizeRunnerWorkloadID(item.GetInstanceId())
	if instanceID == "" {
		return nil
	}
	switch workload.GetStatus() {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING:
		status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
		_, err := r.runners.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{
			Id:         workloadID,
			Status:     &status,
			InstanceId: stringPtr(instanceID),
		})
		return err
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		if workload.GetInstanceId() == instanceID {
			return nil
		}
		_, err := r.runners.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{
			Id:         workloadID,
			InstanceId: stringPtr(instanceID),
		})
		return err
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		return r.stopRunnerWorkload(ctx, runnerClient, instanceID)
	default:
		return nil
	}
}
