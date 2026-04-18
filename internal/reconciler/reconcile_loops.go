package reconciler

import (
	"context"
	"log"
	"time"
)

func (r *Reconciler) runWorkloadReconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(r.workloadReconcileInterval)
	defer ticker.Stop()
	r.runWorkloadReconcileCycle(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runWorkloadReconcileCycle(ctx)
		}
	}
}

func (r *Reconciler) runWorkloadReconcileCycle(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	if err := r.reconcileWorkloads(rctx); err != nil {
		log.Printf("reconciler: workload reconciliation failed: %v", err)
	}
}

func (r *Reconciler) runVolumeReconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(volumeReconcileInterval)
	defer ticker.Stop()
	r.runVolumeReconcileCycle(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runVolumeReconcileCycle(ctx)
		}
	}
}

func (r *Reconciler) runVolumeReconcileCycle(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	if err := r.reconcileVolumes(rctx); err != nil {
		log.Printf("reconciler: volume reconciliation failed: %v", err)
	}
}
