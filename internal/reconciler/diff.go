package reconciler

import (
	"fmt"
	"time"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

// Actions is what one pass of the main loop decides from the records alone.
type Actions struct {
	ToStart []AgentInstanceTarget
	ToStop  []*runnersv1.Workload
	// ToWake are hibernated workloads whose instance has work waiting: the
	// Start Decision wakes rather than starts.
	ToWake []*runnersv1.Workload
	// ToHibernate are running workloads whose record says they may sleep: the
	// agent reported its turn over, the class allows it and hibernate_after has
	// passed, and nothing is waiting as far as the listing knows. The checks
	// only a call can make -- the runner's capability, an exposed port, the
	// inbox immediately before the pause -- follow in the loop.
	ToHibernate []*runnersv1.Workload
	// Hibernated are sleeping workloads within their TTL that nothing is
	// waiting on. The loop stops those whose instance is paused or terminated,
	// which nothing will ever wake.
	Hibernated []*runnersv1.Workload
}

type workloadEntry struct {
	workload   *runnersv1.Workload
	activityAt time.Time
}

func ComputeActions(desired []AgentInstanceTarget, actual []*runnersv1.Workload, policies map[uuid.UUID]agentPolicy, fallback agentPolicy, now time.Time) (Actions, error) {
	desiredSet := make(map[uuid.UUID]AgentInstanceTarget, len(desired))
	for _, item := range desired {
		desiredSet[item.AgentInstanceID] = item
	}
	actualSet := make(map[uuid.UUID]workloadEntry, len(actual))
	var duplicates []*runnersv1.Workload
	for _, workload := range actual {
		agentInstanceID, err := uuidutil.ParseUUID(workloadAgentInstanceID(workload), "workload.agent_instance_id")
		if err != nil {
			return Actions{}, err
		}
		activityAt, err := workloadActivityAt(workload)
		if err != nil {
			return Actions{}, err
		}
		entry := workloadEntry{workload: workload, activityAt: activityAt}
		if existing, ok := actualSet[agentInstanceID]; ok {
			if entry.activityAt.Before(existing.activityAt) {
				duplicates = append(duplicates, existing.workload)
				actualSet[agentInstanceID] = entry
			} else {
				duplicates = append(duplicates, entry.workload)
			}
			continue
		}
		actualSet[agentInstanceID] = entry
	}
	result := Actions{ToStop: duplicates}
	for _, item := range desired {
		if _, ok := actualSet[item.AgentInstanceID]; !ok {
			result.ToStart = append(result.ToStart, item)
		}
	}
	for agentInstanceID, entry := range actualSet {
		workload := entry.workload
		if _, ok := desiredSet[agentInstanceID]; ok {
			if workload.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED {
				result.ToWake = append(result.ToWake, workload)
			}
			continue
		}
		switch workload.GetStatus() {
		case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
			result.ToStop = append(result.ToStop, workload)
			continue
		case runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING:
			// A hibernation is not interrupted and a wake is already on its
			// way; reconciliation owns both transitions.
			continue
		}
		agentID, err := uuidutil.ParseUUID(workloadAgentClassID(workload), "workload.agent_class_id")
		if err != nil {
			return Actions{}, err
		}
		policy, ok := policies[agentID]
		if !ok {
			policy = fallback
		}
		if workload.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED {
			if hibernationExpired(workload, policy, now) {
				result.ToStop = append(result.ToStop, workload)
			} else {
				result.Hibernated = append(result.Hibernated, workload)
			}
			continue
		}
		if now.Sub(entry.activityAt) > policy.idleTimeout {
			// The stop of last resort. On a runner that can hibernate a
			// workload is asleep long before this, so one still awake here is
			// one whose hibernation keeps failing -- or whose class puts
			// hibernate_after at or past idle_timeout, which means the stop
			// comes first.
			result.ToStop = append(result.ToStop, workload)
			continue
		}
		if hibernationDue(workload, entry.activityAt, policy, now) {
			result.ToHibernate = append(result.ToHibernate, workload)
		}
	}
	return result, nil
}

// hibernationDue reads the conditions the record carries: a running workload
// whose agent reported its turn over -- or the sweep found quiet, and nothing
// has touched it since; a terminal session attached to it touches it -- of a
// class that hibernates, with hibernate_after elapsed since the last activity.
func hibernationDue(workload *runnersv1.Workload, activityAt time.Time, policy agentPolicy, now time.Time) bool {
	if workload.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		return false
	}
	if workload.GetAgentState() != runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_IDLE {
		return false
	}
	if !policy.hibernate {
		return false
	}
	return now.Sub(activityAt) >= policy.hibernateAfter
}

// hibernationExpired reports whether a hibernated workload has slept past its
// class's TTL. One without a hibernated_at has nothing to measure from: the
// Runners service stamps it with the status, so this is a record that has not
// settled yet, and it waits.
func hibernationExpired(workload *runnersv1.Workload, policy agentPolicy, now time.Time) bool {
	hibernatedAt := workload.GetHibernatedAt()
	if hibernatedAt == nil {
		return false
	}
	return now.Sub(hibernatedAt.AsTime().UTC()) > policy.hibernationTTL
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
