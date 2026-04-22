package reconciler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/runnerdial"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (r *Reconciler) reconcileWorkloads(ctx context.Context) error {
	orgIdentities, err := r.agentIdentityByOrg(ctx)
	if err != nil {
		return err
	}
	tracked, err := r.listActiveWorkloads(ctx, orgIdentities)
	if err != nil {
		return err
	}
	runnerIDs := map[string]struct{}{}
	workloadsByRunner := make(map[string]map[string]*runnersv1.Workload)
	runnerIdentities := map[string]string{}
	for _, workload := range tracked {
		runnerID := workload.GetRunnerId()
		if runnerID == "" {
			log.Printf("reconciler: warn: workload %s missing runner id", workload.GetMeta().GetId())
			continue
		}
		identityID := strings.TrimSpace(workload.GetAgentId())
		if identityID == "" {
			return fmt.Errorf("workload %s missing agent id", workload.GetMeta().GetId())
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
		if _, ok := runnerIdentities[runnerID]; !ok {
			runnerIdentities[runnerID] = identityID
		}
	}
	runners, err := r.listRunnersByOrg(ctx, orgIdentities)
	if err != nil {
		return err
	}
	enrolledRunnerIDs := map[string]struct{}{}
	for _, runner := range runners {
		if runner == nil {
			continue
		}
		runnerID := runner.GetMeta().GetId()
		if runnerID == "" {
			continue
		}
		if runner.GetStatus() != runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED {
			continue
		}
		enrolledRunnerIDs[runnerID] = struct{}{}
		runnerIDs[runnerID] = struct{}{}
		if _, ok := runnerIdentities[runnerID]; ok {
			continue
		}
		orgID := strings.TrimSpace(runner.GetOrganizationId())
		if orgID == "" {
			return fmt.Errorf("runner %s organization id missing", runnerID)
		}
		identityID, ok := orgIdentities[orgID]
		if !ok {
			return fmt.Errorf("runner %s missing identity for org %s", runnerID, orgID)
		}
		runnerIdentities[runnerID] = identityID
	}

	degraded := newDegradeTracker()

	for runnerID := range runnerIDs {
		trackedWorkloads := workloadsByRunner[runnerID]
		identityID, ok := runnerIdentities[runnerID]
		if !ok {
			return fmt.Errorf("runner %s missing identity", runnerID)
		}
		if _, ok := enrolledRunnerIDs[runnerID]; !ok {
			for workloadID, workload := range trackedWorkloads {
				workloadCtx, err := runnerIdentityContext(ctx, workload.GetAgentId())
				if err != nil {
					return err
				}
				if err := r.handleMissingRunnerWorkload(workloadCtx, workload); err != nil {
					log.Printf("reconciler: warn: handle missing workload %s on unenrolled runner: %v", workloadID, err)
				}
				r.degradeThread(workloadCtx, workload.GetThreadId(), degradeReasonRunnerDeprovisioned, degraded)
			}
			continue
		}
		runnerCtx, err := runnerIdentityContext(ctx, identityID)
		if err != nil {
			return err
		}
		runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
		if err != nil {
			if runnerdial.IsNoTerminators(err) {
				for workloadID, workload := range trackedWorkloads {
					workloadCtx, err := runnerIdentityContext(ctx, workload.GetAgentId())
					if err != nil {
						return err
					}
					if err := r.handleMissingRunnerWorkload(workloadCtx, workload); err != nil {
						log.Printf("reconciler: warn: handle missing workload %s after runner dial failure: %v", workloadID, err)
					}
				}
				continue
			}
			log.Printf("reconciler: warn: dial runner %s for workload reconciliation: %v", runnerID, err)
			continue
		}
		resp, err := runnerClient.ListWorkloads(runnerCtx, &runnerv1.ListWorkloadsRequest{})
		if err != nil {
			if runnerdial.IsNoTerminators(err) {
				for workloadID, workload := range trackedWorkloads {
					workloadCtx, err := runnerIdentityContext(ctx, workload.GetAgentId())
					if err != nil {
						return err
					}
					if err := r.handleMissingRunnerWorkload(workloadCtx, workload); err != nil {
						log.Printf("reconciler: warn: handle missing workload %s after runner list failure: %v", workloadID, err)
					}
				}
				continue
			}
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

		for workloadID, workload := range trackedWorkloads {
			workloadCtx, err := runnerIdentityContext(ctx, workload.GetAgentId())
			if err != nil {
				return err
			}
			item, ok := runnerWorkloads[workloadID]
			if !ok {
				if err := r.handleMissingRunnerWorkload(workloadCtx, workload); err != nil {
					log.Printf("reconciler: warn: handle missing workload %s: %v", workloadID, err)
				}
				continue
			}
			delete(runnerWorkloads, workloadID)
			if err := r.handlePresentRunnerWorkload(workloadCtx, runnerClient, workload, item); err != nil {
				log.Printf("reconciler: warn: handle workload %s on runner %s: %v", workloadID, runnerID, err)
			}
		}

		for _, item := range runnerWorkloads {
			instanceID := normalizeRunnerWorkloadID(item.GetInstanceId())
			if instanceID == "" {
				log.Printf("reconciler: warn: runner %s orphan workload missing instance id", runnerID)
				continue
			}
			if err := r.stopRunnerWorkload(runnerCtx, runnerClient, instanceID); err != nil {
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
	callCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	missingAt := timestamppb.New(time.Now().UTC())
	switch workload.GetStatus() {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
		_, err := r.runners.UpdateWorkload(callCtx, &runnersv1.UpdateWorkloadRequest{
			Id:        workloadID,
			Status:    &status,
			RemovedAt: missingAt,
		})
		return err
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
		_, err := r.runners.UpdateWorkload(callCtx, &runnersv1.UpdateWorkloadRequest{
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
	callCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	switch workload.GetStatus() {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING:
		status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
		_, err := r.runners.UpdateWorkload(callCtx, &runnersv1.UpdateWorkloadRequest{
			Id:         workloadID,
			Status:     &status,
			InstanceId: stringPtr(instanceID),
		})
		return err
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		if workload.GetInstanceId() == instanceID {
			return nil
		}
		_, err := r.runners.UpdateWorkload(callCtx, &runnersv1.UpdateWorkloadRequest{
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
