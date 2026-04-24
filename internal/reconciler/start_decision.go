package reconciler

import (
	"context"
	"fmt"
	"time"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
)

const maxStartAttempts = 10

var startBackoffSchedule = []time.Duration{
	10 * time.Second,
	30 * time.Second,
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
}

func (r *Reconciler) shouldStartWorkload(ctx context.Context, target AgentThread, now time.Time, agentUpdatedAt map[uuid.UUID]time.Time, degraded *degradeTracker) (bool, error) {
	runnerCtx, err := r.runnerIdentityContextForAgent(ctx, target.AgentID)
	if err != nil {
		return false, err
	}
	threadID := target.ThreadID.String()
	agentID := target.AgentID.String()
	active, err := r.listWorkloadsByThread(runnerCtx, threadID, &agentID, []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
	}, 1)
	if err != nil {
		return false, err
	}
	if len(active) > 0 {
		return false, nil
	}
	latest, err := r.latestWorkloadByThread(runnerCtx, threadID, &agentID, []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	})
	if err != nil {
		return false, err
	}
	if latest == nil || latest.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED {
		return true, nil
	}
	updatedAt, ok := agentUpdatedAt[target.AgentID]
	if !ok {
		return false, fmt.Errorf("agent %s updated_at missing", target.AgentID.String())
	}
	latestRemovedAt, err := workloadRemovedAt(latest)
	if err != nil {
		return false, err
	}
	if updatedAt.After(latestRemovedAt) {
		return true, nil
	}
	lastStopped, err := r.latestWorkloadByThread(runnerCtx, threadID, &agentID, []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
	})
	if err != nil {
		return false, err
	}
	resetFloor := updatedAt
	if lastStopped != nil {
		stoppedAt, err := workloadCreatedAt(lastStopped)
		if err != nil {
			return false, err
		}
		if stoppedAt.After(resetFloor) {
			resetFloor = stoppedAt
		}
	}
	recentFailures, err := r.listWorkloadsByThread(runnerCtx, threadID, &agentID, []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	}, maxStartAttempts+1)
	if err != nil {
		return false, err
	}
	consecutiveFailures := 0
	for _, failure := range recentFailures {
		failureAt, err := workloadCreatedAt(failure)
		if err != nil {
			return false, err
		}
		if !failureAt.After(resetFloor) {
			break
		}
		consecutiveFailures++
	}
	if consecutiveFailures >= maxStartAttempts {
		r.degradeThread(runnerCtx, threadID, degradeReasonStartFailures, degraded)
		return false, nil
	}
	if consecutiveFailures == 0 {
		return true, nil
	}
	backoffIndex := consecutiveFailures - 1
	if backoffIndex >= len(startBackoffSchedule) {
		backoffIndex = len(startBackoffSchedule) - 1
	}
	backoff := startBackoffSchedule[backoffIndex]
	if now.Sub(latestRemovedAt) < backoff {
		return false, nil
	}
	return true, nil
}

func (r *Reconciler) latestWorkloadByThread(ctx context.Context, threadID string, agentID *string, statuses []runnersv1.WorkloadStatus) (*runnersv1.Workload, error) {
	workloads, err := r.listWorkloadsByThread(ctx, threadID, agentID, statuses, 1)
	if err != nil {
		return nil, err
	}
	if len(workloads) == 0 {
		return nil, nil
	}
	return workloads[0], nil
}

func (r *Reconciler) listWorkloadsByThread(ctx context.Context, threadID string, agentID *string, statuses []runnersv1.WorkloadStatus, limit int) ([]*runnersv1.Workload, error) {
	if threadID == "" {
		return nil, fmt.Errorf("thread id missing")
	}
	workloads := []*runnersv1.Workload{}
	pageToken := ""
	for {
		pageSize := workloadHistoryPageSize
		if limit > 0 {
			remaining := limit - len(workloads)
			if remaining <= 0 {
				break
			}
			if remaining < int(pageSize) {
				pageSize = int32(remaining)
			}
		}
		req := &runnersv1.ListWorkloadsByThreadRequest{
			ThreadId:  threadID,
			PageSize:  pageSize,
			PageToken: pageToken,
		}
		if agentID != nil {
			req.AgentId = agentID
		}
		if len(statuses) > 0 {
			req.Statuses = statuses
		}
		resp, err := r.runners.ListWorkloadsByThread(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("list workloads for thread %s: %w", threadID, err)
		}
		for _, workload := range resp.GetWorkloads() {
			if workload == nil {
				return nil, fmt.Errorf("workload is nil")
			}
			meta := workload.GetMeta()
			if meta == nil {
				return nil, fmt.Errorf("workload meta missing")
			}
			if meta.GetId() == "" {
				return nil, fmt.Errorf("workload meta id missing")
			}
			workloads = append(workloads, workload)
			if limit > 0 && len(workloads) >= limit {
				break
			}
		}
		if limit > 0 && len(workloads) >= limit {
			break
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return workloads, nil
}

func workloadCreatedAt(workload *runnersv1.Workload) (time.Time, error) {
	meta := workload.GetMeta()
	if meta == nil {
		return time.Time{}, fmt.Errorf("workload meta missing")
	}
	createdAt := meta.GetCreatedAt()
	if createdAt == nil {
		return time.Time{}, fmt.Errorf("workload created_at missing")
	}
	return createdAt.AsTime().UTC(), nil
}

func workloadRemovedAt(workload *runnersv1.Workload) (time.Time, error) {
	removedAt := workload.GetRemovedAt()
	if removedAt == nil {
		return time.Time{}, fmt.Errorf("workload removed_at missing")
	}
	return removedAt.AsTime().UTC(), nil
}
