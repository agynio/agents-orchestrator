package reconciler

import (
	"fmt"
	"time"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
)

type Actions struct {
	ToStart []AgentThread
	ToStop  []*runnersv1.Workload
}

type workloadEntry struct {
	workload  *runnersv1.Workload
	startedAt time.Time
}

func ComputeActions(desired []AgentThread, actual []*runnersv1.Workload, idleTimeout time.Duration, now time.Time) (Actions, error) {
	desiredSet := make(map[AgentThread]struct{}, len(desired))
	for _, item := range desired {
		desiredSet[item] = struct{}{}
	}
	actualSet := make(map[AgentThread]workloadEntry, len(actual))
	for _, workload := range actual {
		agentID, err := uuidutil.ParseUUID(workload.GetAgentId(), "workload.agent_id")
		if err != nil {
			return Actions{}, err
		}
		threadID, err := uuidutil.ParseUUID(workload.GetThreadId(), "workload.thread_id")
		if err != nil {
			return Actions{}, err
		}
		meta := workload.GetMeta()
		createdAt := meta.GetCreatedAt()
		if createdAt == nil {
			return Actions{}, fmt.Errorf("workload meta created_at missing")
		}
		key := AgentThread{AgentID: agentID, ThreadID: threadID}
		actualSet[key] = workloadEntry{workload: workload, startedAt: createdAt.AsTime()}
	}
	result := Actions{}
	for _, item := range desired {
		if _, ok := actualSet[item]; !ok {
			result.ToStart = append(result.ToStart, item)
		}
	}
	for key, entry := range actualSet {
		if _, ok := desiredSet[key]; ok {
			continue
		}
		status := entry.workload.GetStatus()
		if status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED || status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
			continue
		}
		if entry.startedAt.IsZero() || entry.startedAt.Year() < 2020 {
			return Actions{}, fmt.Errorf("workload %s has invalid createdAt: %v", entry.workload.GetMeta().GetId(), entry.startedAt)
		}
		if now.Sub(entry.startedAt) > idleTimeout {
			result.ToStop = append(result.ToStop, entry.workload)
		}
	}
	return result, nil
}
