package reconciler

import (
	"time"

	"github.com/agynio/agents-orchestrator/internal/store"
)

type Actions struct {
	ToStart []AgentThread
	ToStop  []store.Workload
}

func ComputeActions(desired []AgentThread, actual []store.Workload, idleTimeout time.Duration, now time.Time) Actions {
	desiredSet := make(map[AgentThread]struct{}, len(desired))
	for _, item := range desired {
		desiredSet[item] = struct{}{}
	}
	actualSet := make(map[AgentThread]store.Workload, len(actual))
	for _, workload := range actual {
		key := AgentThread{AgentID: workload.AgentID, ThreadID: workload.ThreadID}
		actualSet[key] = workload
	}
	result := Actions{}
	for _, item := range desired {
		if _, ok := actualSet[item]; !ok {
			result.ToStart = append(result.ToStart, item)
		}
	}
	for key, workload := range actualSet {
		if _, ok := desiredSet[key]; ok {
			continue
		}
		if now.Sub(workload.StartedAt) > idleTimeout {
			result.ToStop = append(result.ToStop, workload)
		}
	}
	return result
}
