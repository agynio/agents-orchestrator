package reconciler

import (
	"sync"
	"time"

	"github.com/agynio/agents-orchestrator/internal/runnerdial"
)

// runnerLostAfter is how long a runner stays without an OpenZiti terminator,
// never once answering, before it is taken to be gone. It outlasts a restart of
// the runner together with the overlay it re-enrolls through, which is the
// usual reason for having none.
const runnerLostAfter = 30 * time.Minute

// runnerAbsences remembers since when each runner has had no terminator. It is
// bookkeeping about calls this process made, as hibernationState is: a restart
// forgets it, which postpones a verdict and costs nothing else. Every method
// tolerates a nil receiver so a Reconciler built without New still decides.
type runnerAbsences struct {
	mu    sync.Mutex
	since map[string]time.Time
}

func newRunnerAbsences() *runnerAbsences {
	return &runnerAbsences{since: map[string]time.Time{}}
}

// lost notes that the runner has no terminator and reports whether that has
// been so for runnerLostAfter.
func (a *runnerAbsences) lost(runnerID string, now time.Time) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	since, ok := a.since[runnerID]
	if !ok {
		a.since[runnerID] = now
		return false
	}
	return now.Sub(since) >= runnerLostAfter
}

// answered ends an absence: whatever the runner said, it is there.
func (a *runnerAbsences) answered(runnerID string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.since, runnerID)
}

// runnerLost reports whether a failed runner call means the runner is gone,
// the only thing that lets its workloads be read as missing. A runner that did
// not answer has said nothing about them: one restarting has no terminator
// until it has re-enrolled, and still holds everything it held.
func (r *Reconciler) runnerLost(runnerID string, err error) bool {
	if !runnerdial.IsNoTerminators(err) {
		return false
	}
	return r.absences.lost(runnerID, time.Now().UTC())
}
