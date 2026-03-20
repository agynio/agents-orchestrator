package reconciler

import (
	"context"
	"log"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/store"
)

type Reconciler struct {
	threads   threadsv1.ThreadsServiceClient
	agents    agentsv1.AgentsServiceClient
	runner    runnerv1.RunnerServiceClient
	store     *store.Store
	assembler *assembler.Assembler
	wake      <-chan struct{}
	poll      time.Duration
	idle      time.Duration
	stopSec   uint32
}

func New(threads threadsv1.ThreadsServiceClient, agents agentsv1.AgentsServiceClient, runner runnerv1.RunnerServiceClient, store *store.Store, assembler *assembler.Assembler, wake <-chan struct{}, poll, idle time.Duration, stopSec uint32) *Reconciler {
	return &Reconciler{
		threads:   threads,
		agents:    agents,
		runner:    runner,
		store:     store,
		assembler: assembler,
		wake:      wake,
		poll:      poll,
		idle:      idle,
		stopSec:   stopSec,
	}
}

func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.poll)
	defer ticker.Stop()

	if err := r.reconcile(ctx); err != nil {
		log.Printf("reconciler: initial reconcile failed: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.wake:
			if err := r.reconcile(ctx); err != nil {
				log.Printf("reconciler: reconcile failed: %v", err)
			}
		case <-ticker.C:
			if err := r.reconcile(ctx); err != nil {
				log.Printf("reconciler: reconcile failed: %v", err)
			}
		}
	}
}

func (r *Reconciler) reconcile(ctx context.Context) error {
	desired, err := r.fetchDesired(ctx)
	if err != nil {
		return err
	}
	actual, err := r.fetchActual(ctx)
	if err != nil {
		return err
	}
	actions := ComputeActions(desired, actual, r.idle, time.Now().UTC())
	for _, candidate := range actions.ToStart {
		r.startWorkload(ctx, candidate)
	}
	for _, workload := range actions.ToStop {
		r.stopWorkload(ctx, workload)
	}
	return nil
}

func (r *Reconciler) startWorkload(ctx context.Context, target AgentThread) {
	request, err := r.assembler.Assemble(ctx, target.AgentID, target.ThreadID)
	if err != nil {
		log.Printf("reconciler: assemble workload for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		return
	}
	resp, err := r.runner.StartWorkload(ctx, request)
	if err != nil {
		log.Printf("reconciler: start workload for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		return
	}
	if resp.GetStatus() == runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		log.Printf("reconciler: workload failed for agent %s thread %s: %s", target.AgentID.String(), target.ThreadID.String(), failureSummary(resp.GetFailure()))
		return
	}
	if resp.GetId() == "" {
		log.Printf("reconciler: workload started without id for agent %s thread %s", target.AgentID.String(), target.ThreadID.String())
		return
	}
	if _, err := r.store.Insert(ctx, resp.GetId(), target.AgentID, target.ThreadID); err != nil {
		log.Printf("reconciler: store workload %s for agent %s thread %s: %v", resp.GetId(), target.AgentID.String(), target.ThreadID.String(), err)
	}
}

func (r *Reconciler) stopWorkload(ctx context.Context, workload store.Workload) {
	_, err := r.runner.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{
		WorkloadId: workload.WorkloadID,
		TimeoutSec: r.stopSec,
	})
	if err != nil {
		log.Printf("reconciler: stop workload %s: %v", workload.WorkloadID, err)
		return
	}
	if _, err := r.store.Delete(ctx, workload.WorkloadID); err != nil {
		log.Printf("reconciler: delete workload %s from store: %v", workload.WorkloadID, err)
	}
}

func failureSummary(failure *runnerv1.WorkloadFailure) string {
	if failure == nil {
		return "unknown failure"
	}
	if failure.GetMessage() != "" {
		return failure.GetMessage()
	}
	return failure.GetCode()
}
