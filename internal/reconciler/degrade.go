package reconciler

import (
	"context"
	"log"
	"strings"

	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
)

const (
	degradeReasonRunnerDeprovisioned = "runner_deprovisioned"
	degradeReasonVolumeLost          = "volume_lost"
)

type degradeTracker struct {
	seen map[string]struct{}
}

func newDegradeTracker() *degradeTracker {
	return &degradeTracker{seen: make(map[string]struct{})}
}

func (d *degradeTracker) shouldDegrade(threadID string) bool {
	if d == nil {
		return true
	}
	if _, ok := d.seen[threadID]; ok {
		return false
	}
	d.seen[threadID] = struct{}{}
	return true
}

func (r *Reconciler) degradeThread(ctx context.Context, agentID, threadID, reason string, tracker *degradeTracker) {
	if threadID == "" {
		log.Printf("reconciler: warn: degrade thread missing id for reason %s", reason)
		return
	}
	trimmedAgentID := strings.TrimSpace(agentID)
	if trimmedAgentID == "" {
		log.Printf("reconciler: warn: degrade thread %s missing agent id for reason %s", threadID, reason)
		return
	}
	if tracker != nil && !tracker.shouldDegrade(threadID) {
		return
	}
	if r.threads == nil {
		log.Printf("reconciler: warn: threads client not configured for degrade thread %s", threadID)
		return
	}
	parsedAgentID, err := uuidutil.ParseUUID(trimmedAgentID, "agent_id")
	if err != nil {
		log.Printf("reconciler: warn: degrade thread %s invalid agent id: %v", threadID, err)
		return
	}
	callCtx := r.agentContext(ctx, parsedAgentID)
	if _, err := r.threads.DegradeThread(callCtx, &threadsv1.DegradeThreadRequest{
		ThreadId: threadID,
		Reason:   reason,
	}); err != nil {
		log.Printf("reconciler: degrade thread %s (%s): %v", threadID, reason, err)
	}
}
