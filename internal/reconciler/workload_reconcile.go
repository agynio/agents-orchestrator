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

const (
	startGracePeriod     = 60 * time.Second
	initRetryThreshold   = 3
	crashloopThreshold   = 3
	crashLoopBackoffFlag = "CrashLoopBackOff"
)

var imagePullReasons = map[string]struct{}{
	"ImagePullBackOff": {},
	"ErrImagePull":     {},
}

var configInvalidReasons = map[string]struct{}{
	"CreateContainerConfigError": {},
	"CreateContainerError":       {},
	"InvalidImageName":           {},
}

func (r *Reconciler) handleMissingRunnerWorkload(ctx context.Context, workload *runnersv1.Workload) error {
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		return nil
	}
	switch workload.GetStatus() {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		failureReason := runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST
		failureMessage := "workload missing on runner"
		r.markWorkloadFailed(ctx, workloadID, stringPtr(workload.GetInstanceId()), failureReason, failureMessage, nil)
		if r.zitiMgmt != nil && workload.GetZitiIdentityId() != "" {
			if err := r.deleteIdentity(ctx, workload.GetZitiIdentityId()); err != nil {
				log.Printf("reconciler: delete ziti identity %s after missing workload %s: %v", workload.GetZitiIdentityId(), workloadID, err)
			}
		}
		return nil
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		missingAt := timestamppb.New(time.Now().UTC())
		status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
		_, err := r.runners.UpdateWorkload(runnersContext(ctx), &runnersv1.UpdateWorkloadRequest{
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
	updateReq := &runnersv1.UpdateWorkloadRequest{Id: workloadID}
	shouldUpdate := false
	if workload.GetInstanceId() != instanceID {
		updateReq.InstanceId = stringPtr(instanceID)
		shouldUpdate = true
	}
	inspectResp, err := r.inspectRunnerWorkload(ctx, runnerClient, instanceID)
	var containers []*runnersv1.Container
	if err != nil {
		log.Printf("reconciler: warn: inspect workload %s: %v", workloadID, err)
	} else {
		mapped, mapErr := mapRunnerContainers(inspectResp.GetContainers())
		if mapErr != nil {
			log.Printf("reconciler: warn: map workload %s containers: %v", workloadID, mapErr)
		} else if mapped != nil {
			containers = mapped
			updateReq.Containers = containers
			shouldUpdate = true
		}
	}
	switch workload.GetStatus() {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING:
		if containers != nil {
			ready, failure, err := classifyStartingContainers(containers, workload, time.Now().UTC())
			if err != nil {
				return err
			}
			if failure != nil {
				r.failWorkloadOnRunner(ctx, runnerClient, workload, instanceID, failure, containers)
				return nil
			}
			if ready {
				status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
				updateReq.Status = &status
				shouldUpdate = true
			}
		} else if inspectResp != nil && inspectResp.GetStateRunning() {
			status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
			updateReq.Status = &status
			shouldUpdate = true
		}
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		if containers != nil {
			failure, err := classifyRunningContainers(containers)
			if err != nil {
				return err
			}
			if failure != nil {
				r.failWorkloadOnRunner(ctx, runnerClient, workload, instanceID, failure, containers)
				return nil
			}
		}
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		// stop below
	default:
	}
	if shouldUpdate {
		if _, err := r.runners.UpdateWorkload(runnersContext(ctx), updateReq); err != nil {
			return err
		}
	}
	if workload.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING {
		return r.stopRunnerWorkload(ctx, runnerClient, instanceID)
	}
	return nil
}

type workloadFailure struct {
	reason  runnersv1.WorkloadFailureReason
	message string
}

func classifyStartingContainers(containers []*runnersv1.Container, workload *runnersv1.Workload, now time.Time) (bool, *workloadFailure, error) {
	createdAt, err := workloadCreatedAt(workload)
	if err != nil {
		return false, nil, err
	}
	startAge := now.Sub(createdAt)
	initComplete := true
	mainRunning := true
	foundMain := false
	for _, container := range containers {
		if container == nil {
			return false, nil, fmt.Errorf("workload %s has nil container", workload.GetMeta().GetId())
		}
		switch container.GetRole() {
		case runnersv1.ContainerRole_CONTAINER_ROLE_INIT:
			switch container.GetStatus() {
			case runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING:
				if isImagePullFailure(container) && startAge > startGracePeriod {
					return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_IMAGE_PULL_FAILED, message: containerFailureMessage(container)}, nil
				}
				if isConfigInvalidFailure(container) && startAge > startGracePeriod {
					return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CONFIG_INVALID, message: containerFailureMessage(container)}, nil
				}
				initComplete = false
			case runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED:
				if container.GetExitCode() != 0 && container.GetRestartCount() >= initRetryThreshold {
					return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED, message: containerFailureMessage(container)}, nil
				}
				if container.GetExitCode() != 0 {
					initComplete = false
				}
			default:
				initComplete = false
			}
		case runnersv1.ContainerRole_CONTAINER_ROLE_MAIN:
			foundMain = true
			if container.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING {
				mainRunning = false
			}
			if container.GetStatus() == runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING {
				if container.GetReason() == crashLoopBackoffFlag && container.GetRestartCount() >= crashloopThreshold {
					return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP, message: containerFailureMessage(container)}, nil
				}
				if isImagePullFailure(container) && startAge > startGracePeriod {
					return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_IMAGE_PULL_FAILED, message: containerFailureMessage(container)}, nil
				}
				if isConfigInvalidFailure(container) && startAge > startGracePeriod {
					return false, &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CONFIG_INVALID, message: containerFailureMessage(container)}, nil
				}
			}
		default:
		}
	}
	if !foundMain {
		return false, nil, fmt.Errorf("workload %s missing main container", workload.GetMeta().GetId())
	}
	if initComplete && mainRunning {
		return true, nil, nil
	}
	return false, nil, nil
}

func classifyRunningContainers(containers []*runnersv1.Container) (*workloadFailure, error) {
	for _, container := range containers {
		if container == nil {
			return nil, fmt.Errorf("container is nil")
		}
		if container.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_MAIN {
			continue
		}
		if container.GetStatus() == runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING && container.GetReason() == crashLoopBackoffFlag && container.GetRestartCount() >= crashloopThreshold {
			return &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP, message: containerFailureMessage(container)}, nil
		}
	}
	return nil, nil
}

func (r *Reconciler) failWorkloadOnRunner(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, workload *runnersv1.Workload, instanceID string, failure *workloadFailure, containers []*runnersv1.Container) {
	if failure == nil {
		return
	}
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		return
	}
	r.markWorkloadFailed(ctx, workloadID, stringPtr(instanceID), failure.reason, failure.message, containers)
	if err := r.stopRunnerWorkload(ctx, runnerClient, instanceID); err != nil {
		log.Printf("reconciler: stop workload %s (instance %s) after failure: %v", workloadID, instanceID, err)
	}
	if r.zitiMgmt != nil && workload.GetZitiIdentityId() != "" {
		if err := r.deleteIdentity(ctx, workload.GetZitiIdentityId()); err != nil {
			log.Printf("reconciler: delete ziti identity %s after workload %s failure: %v", workload.GetZitiIdentityId(), workloadID, err)
		}
	}
}

func isImagePullFailure(container *runnersv1.Container) bool {
	_, ok := imagePullReasons[container.GetReason()]
	return ok
}

func isConfigInvalidFailure(container *runnersv1.Container) bool {
	_, ok := configInvalidReasons[container.GetReason()]
	return ok
}

func containerFailureMessage(container *runnersv1.Container) string {
	if container.GetMessage() != "" {
		return container.GetMessage()
	}
	return container.GetReason()
}
