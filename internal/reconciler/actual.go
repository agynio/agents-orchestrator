package reconciler

import (
	"context"
	"log"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/store"
)

func (r *Reconciler) fetchActual(ctx context.Context) ([]store.Workload, error) {
	tracked, err := r.store.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := r.runner.FindWorkloadsByLabels(ctx, &runnerv1.FindWorkloadsByLabelsRequest{
		Labels: map[string]string{assembler.LabelManagedBy: assembler.ManagedByValue},
		All:    true,
	})
	if err != nil {
		return nil, err
	}
	running := make(map[string]struct{}, len(resp.GetTargetIds()))
	for _, id := range resp.GetTargetIds() {
		running[id] = struct{}{}
	}
	actual := make([]store.Workload, 0, len(tracked))
	for _, workload := range tracked {
		if _, ok := running[workload.WorkloadID]; ok {
			actual = append(actual, workload)
			continue
		}
		deleted, err := r.store.Delete(ctx, workload.WorkloadID)
		if err != nil {
			log.Printf("reconciler: warn: delete stale workload %s: %v", workload.WorkloadID, err)
			continue
		}
		if deleted {
			log.Printf("reconciler: warn: removed stale workload %s", workload.WorkloadID)
		}
	}
	return actual, nil
}
