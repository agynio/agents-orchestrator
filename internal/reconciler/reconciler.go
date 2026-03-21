package reconciler

import (
	"context"
	"fmt"
	"log"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/store"
)

// TODO: resolve real tenant_id when multi-tenancy is implemented across all services.
const placeholderTenantID = "00000000-0000-0000-0000-000000000000"

type Reconciler struct {
	threads   threadsv1.ThreadsServiceClient
	agents    agentsv1.AgentsServiceClient
	runner    runnerv1.RunnerServiceClient
	zitiMgmt  zitimgmtv1.ZitiManagementServiceClient
	store     store.WorkloadStore
	assembler *assembler.Assembler
	wake      <-chan struct{}
	poll      time.Duration
	idle      time.Duration
	stopSec   uint32
}

func New(threads threadsv1.ThreadsServiceClient, agents agentsv1.AgentsServiceClient, runner runnerv1.RunnerServiceClient, zitiMgmt zitimgmtv1.ZitiManagementServiceClient, store store.WorkloadStore, assembler *assembler.Assembler, wake <-chan struct{}, poll, idle time.Duration, stopSec uint32) *Reconciler {
	return &Reconciler{
		threads:   threads,
		agents:    agents,
		runner:    runner,
		zitiMgmt:  zitiMgmt,
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
	if err := r.reconcileOrphanIdentities(ctx); err != nil {
		return err
	}
	return nil
}

func (r *Reconciler) startWorkload(ctx context.Context, target AgentThread) {
	request, err := r.assembler.Assemble(ctx, target.AgentID, target.ThreadID)
	if err != nil {
		log.Printf("reconciler: assemble workload for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		return
	}
	identityResp, err := r.zitiMgmt.CreateAgentIdentity(ctx, &zitimgmtv1.CreateAgentIdentityRequest{
		AgentId:  target.AgentID.String(),
		TenantId: placeholderTenantID,
	})
	if err != nil {
		log.Printf("reconciler: create ziti identity for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		return
	}
	zitiIdentityID := identityResp.GetZitiIdentityId()
	enrollmentJWT := identityResp.GetEnrollmentJwt()
	if zitiIdentityID == "" || enrollmentJWT == "" {
		log.Printf("reconciler: ziti identity response missing fields for agent %s thread %s", target.AgentID.String(), target.ThreadID.String())
		if zitiIdentityID != "" {
			if err := r.deleteIdentity(ctx, zitiIdentityID); err != nil {
				log.Printf("reconciler: delete ziti identity %s after missing enrollment jwt: %v", zitiIdentityID, err)
			}
		}
		return
	}
	request.Main.Env = append(request.Main.Env, &runnerv1.EnvVar{Name: "ZITI_ENROLLMENT_JWT", Value: enrollmentJWT})
	resp, err := r.runner.StartWorkload(ctx, request)
	if err != nil {
		log.Printf("reconciler: start workload for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		if err := r.deleteIdentity(ctx, zitiIdentityID); err != nil {
			log.Printf("reconciler: delete ziti identity %s after start failure: %v", zitiIdentityID, err)
		}
		return
	}
	if resp.GetStatus() == runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		log.Printf("reconciler: workload failed for agent %s thread %s: %s", target.AgentID.String(), target.ThreadID.String(), failureSummary(resp.GetFailure()))
		if err := r.deleteIdentity(ctx, zitiIdentityID); err != nil {
			log.Printf("reconciler: delete ziti identity %s after workload failure: %v", zitiIdentityID, err)
		}
		return
	}
	if resp.GetId() == "" {
		log.Printf("reconciler: workload started without id for agent %s thread %s", target.AgentID.String(), target.ThreadID.String())
		if err := r.deleteIdentity(ctx, zitiIdentityID); err != nil {
			log.Printf("reconciler: delete ziti identity %s after missing workload id: %v", zitiIdentityID, err)
		}
		return
	}
	if _, err := r.store.Insert(ctx, resp.GetId(), target.AgentID, target.ThreadID, &zitiIdentityID); err != nil {
		log.Printf("reconciler: store workload %s for agent %s thread %s: %v", resp.GetId(), target.AgentID.String(), target.ThreadID.String(), err)
		if _, err := r.runner.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{
			WorkloadId: resp.GetId(),
			TimeoutSec: r.stopSec,
		}); err != nil {
			log.Printf("reconciler: stop workload %s after store failure: %v", resp.GetId(), err)
		}
		if err := r.deleteIdentity(ctx, zitiIdentityID); err != nil {
			log.Printf("reconciler: delete ziti identity %s after store failure: %v", zitiIdentityID, err)
		}
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
	if workload.ZitiIdentityID != nil {
		if err := r.deleteIdentity(ctx, *workload.ZitiIdentityID); err != nil {
			log.Printf("reconciler: delete ziti identity %s after stopping workload %s: %v", *workload.ZitiIdentityID, workload.WorkloadID, err)
		}
	}
	if _, err := r.store.Delete(ctx, workload.WorkloadID); err != nil {
		log.Printf("reconciler: delete workload %s from store: %v", workload.WorkloadID, err)
	}
}

func (r *Reconciler) deleteIdentity(ctx context.Context, identityID string) error {
	if identityID == "" {
		return fmt.Errorf("ziti identity id is empty")
	}
	_, err := r.zitiMgmt.DeleteIdentity(ctx, &zitimgmtv1.DeleteIdentityRequest{ZitiIdentityId: identityID})
	return err
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
