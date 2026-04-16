package reconciler

import (
	"fmt"
	"time"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

type Actions struct {
	ToStart []AgentThread
	ToStop  []*runnersv1.Workload
}

type workloadEntry struct {
	workload   *runnersv1.Workload
	activityAt time.Time
}

func ComputeActions(desired []AgentThread, actual []*runnersv1.Workload, idleTimeouts map[uuid.UUID]time.Duration, fallbackIdleTimeout time.Duration, now time.Time) (Actions, error) {
	desiredSet := make(map[AgentThread]struct{}, len(desired))
	for _, item := range desired {
		desiredSet[item] = struct{}{}
	}
	actualSet := make(map[AgentThread]workloadEntry, len(actual))
	var duplicates []*runnersv1.Workload
	for _, workload := range actual {
		agentID, err := uuidutil.ParseUUID(workload.GetAgentId(), "workload.agent_id")
		if err != nil {
			return Actions{}, err
		}
		threadID, err := uuidutil.ParseUUID(workload.GetThreadId(), "workload.thread_id")
		if err != nil {
			return Actions{}, err
		}
		activityAt, err := workloadActivityAt(workload)
		if err != nil {
			return Actions{}, err
		}
		key := AgentThread{AgentID: agentID, ThreadID: threadID}
		entry := workloadEntry{workload: workload, activityAt: activityAt}
		if existing, ok := actualSet[key]; ok {
			if entry.activityAt.Before(existing.activityAt) {
				duplicates = append(duplicates, existing.workload)
				actualSet[key] = entry
			} else {
				duplicates = append(duplicates, entry.workload)
			}
			continue
		}
		actualSet[key] = entry
	}
	result := Actions{ToStop: duplicates}
	for _, item := range desired {
		if _, ok := actualSet[item]; !ok {
			result.ToStart = append(result.ToStart, item)
		}
	}
	for key, entry := range actualSet {
		if _, ok := desiredSet[key]; ok {
			continue
		}
		if entry.workload.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING {
			result.ToStop = append(result.ToStop, entry.workload)
			continue
		}
		idleTimeout, ok := idleTimeouts[key.AgentID]
		if !ok {
			idleTimeout = fallbackIdleTimeout
		}
		if now.Sub(entry.activityAt) > idleTimeout {
			result.ToStop = append(result.ToStop, entry.workload)
		}
	}
	return result, nil
}

func workloadActivityAt(workload *runnersv1.Workload) (time.Time, error) {
	meta := workload.GetMeta()
	if meta == nil {
		return time.Time{}, fmt.Errorf("workload meta missing")
	}
	if lastActivity := workload.GetLastActivityAt(); lastActivity != nil {
		return lastActivity.AsTime(), nil
	}
	createdAt := meta.GetCreatedAt()
	if createdAt == nil {
		return time.Time{}, fmt.Errorf("workload meta created_at missing")
	}
	return createdAt.AsTime(), nil
}
