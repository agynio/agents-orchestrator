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

const reconcileTimeout = 30 * time.Second

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

type Config struct {
	Threads   threadsv1.ThreadsServiceClient
	Agents    agentsv1.AgentsServiceClient
	Runner    runnerv1.RunnerServiceClient
	ZitiMgmt  zitimgmtv1.ZitiManagementServiceClient
	Store     store.WorkloadStore
	Assembler *assembler.Assembler
	Wake      <-chan struct{}
	Poll      time.Duration
	Idle      time.Duration
	StopSec   uint32
}

func New(cfg Config) *Reconciler {
	return &Reconciler{
		threads:   cfg.Threads,
		agents:    cfg.Agents,
		runner:    cfg.Runner,
		zitiMgmt:  cfg.ZitiMgmt,
		store:     cfg.Store,
		assembler: cfg.Assembler,
		wake:      cfg.Wake,
		poll:      cfg.Poll,
		idle:      cfg.Idle,
		stopSec:   cfg.StopSec,
	}
}

func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.poll)
	defer ticker.Stop()

	r.runCycle(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.wake:
			r.runCycle(ctx)
		case <-ticker.C:
			r.runCycle(ctx)
		}
	}
}

func (r *Reconciler) runCycle(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	if err := r.reconcile(rctx); err != nil {
		log.Printf("reconciler: cycle failed: %v", err)
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
	if r.zitiMgmt != nil {
		if err := r.reconcileOrphanIdentities(ctx); err != nil {
			return err
		}
	}
	log.Printf(
		"reconciler: cycle complete - desired=%d actual=%d started=%d stopped=%d",
		len(desired),
		len(actual),
		len(actions.ToStart),
		len(actions.ToStop),
	)
	return nil
}

type identityInfo struct {
	id            string
	enrollmentJWT string
}

func (i *identityInfo) idPtr() *string {
	if i == nil {
		return nil
	}
	return &i.id
}

func (r *Reconciler) createIdentity(ctx context.Context, target AgentThread) (*identityInfo, error) {
	if r.zitiMgmt == nil {
		return nil, nil
	}
	identityResp, err := r.zitiMgmt.CreateAgentIdentity(ctx, &zitimgmtv1.CreateAgentIdentityRequest{
		AgentId: target.AgentID.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("create ziti identity for agent %s thread %s: %w", target.AgentID.String(), target.ThreadID.String(), err)
	}
	identityID := identityResp.GetZitiIdentityId()
	enrollmentJWT := identityResp.GetEnrollmentJwt()
	if identityID == "" || enrollmentJWT == "" {
		var identityPtr *string
		if identityID != "" {
			identityPtr = &identityID
		}
		r.compensateIdentity(ctx, identityPtr, "missing identity fields")
		return nil, fmt.Errorf("ziti identity response missing fields for agent %s thread %s", target.AgentID.String(), target.ThreadID.String())
	}
	return &identityInfo{id: identityID, enrollmentJWT: enrollmentJWT}, nil
}

func (r *Reconciler) compensateIdentity(ctx context.Context, zitiIdentityID *string, reason string) {
	if zitiIdentityID == nil {
		return
	}
	if err := r.deleteIdentity(ctx, *zitiIdentityID); err != nil {
		log.Printf("reconciler: delete ziti identity %s after %s: %v", *zitiIdentityID, reason, err)
	}
}

func (r *Reconciler) startWorkload(ctx context.Context, target AgentThread) {
	request, err := r.assembler.Assemble(ctx, target.AgentID, target.ThreadID)
	if err != nil {
		log.Printf("reconciler: assemble workload for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		return
	}
	identity, err := r.createIdentity(ctx, target)
	if err != nil {
		log.Printf("reconciler: %v", err)
		return
	}
	if identity != nil {
		request.Main.Env = append(request.Main.Env, &runnerv1.EnvVar{Name: "ZITI_ENROLLMENT_JWT", Value: identity.enrollmentJWT})
	}
	zitiIdentityID := identity.idPtr()
	resp, err := r.runner.StartWorkload(ctx, request)
	if err != nil {
		log.Printf("reconciler: start workload for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		r.compensateIdentity(ctx, zitiIdentityID, "start failure")
		return
	}
	if resp.GetStatus() == runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		log.Printf("reconciler: workload failed for agent %s thread %s: %s", target.AgentID.String(), target.ThreadID.String(), failureSummary(resp.GetFailure()))
		r.compensateIdentity(ctx, zitiIdentityID, "workload failure")
		return
	}
	if resp.GetId() == "" {
		log.Printf("reconciler: workload started without id for agent %s thread %s", target.AgentID.String(), target.ThreadID.String())
		r.compensateIdentity(ctx, zitiIdentityID, "missing workload id")
		return
	}
	if _, err := r.store.Insert(ctx, resp.GetId(), target.AgentID, target.ThreadID, zitiIdentityID); err != nil {
		log.Printf("reconciler: store workload %s for agent %s thread %s: %v", resp.GetId(), target.AgentID.String(), target.ThreadID.String(), err)
		if _, err := r.runner.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{
			WorkloadId: resp.GetId(),
			TimeoutSec: r.stopSec,
		}); err != nil {
			log.Printf("reconciler: stop workload %s after store failure: %v", resp.GetId(), err)
		}
		r.compensateIdentity(ctx, zitiIdentityID, "store failure")
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
	if r.zitiMgmt != nil && workload.ZitiIdentityID != nil {
		if err := r.deleteIdentity(ctx, *workload.ZitiIdentityID); err != nil {
			log.Printf("reconciler: delete ziti identity %s after stopping workload %s: %v", *workload.ZitiIdentityID, workload.WorkloadID, err)
		}
	}
	if _, err := r.store.Delete(ctx, workload.WorkloadID); err != nil {
		log.Printf("reconciler: delete workload %s from store: %v", workload.WorkloadID, err)
	}
}

func (r *Reconciler) deleteIdentity(ctx context.Context, identityID string) error {
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
