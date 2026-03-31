package reconciler

import (
	"context"
	"fmt"
	"log"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
)

const reconcileTimeout = 30 * time.Second

type Reconciler struct {
	threads   threadsv1.ThreadsServiceClient
	agents    agentsv1.AgentsServiceClient
	runner    runnerv1.RunnerServiceClient
	runners   runnersv1.RunnersServiceClient
	zitiMgmt  zitimgmtv1.ZitiManagementServiceClient
	assembler *assembler.Assembler
	runnerID  string
	wake      <-chan struct{}
	poll      time.Duration
	idle      time.Duration
	stopSec   uint32
}

type Config struct {
	Threads   threadsv1.ThreadsServiceClient
	Agents    agentsv1.AgentsServiceClient
	Runner    runnerv1.RunnerServiceClient
	Runners   runnersv1.RunnersServiceClient
	ZitiMgmt  zitimgmtv1.ZitiManagementServiceClient
	Assembler *assembler.Assembler
	RunnerID  string
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
		runners:   cfg.Runners,
		zitiMgmt:  cfg.ZitiMgmt,
		assembler: cfg.Assembler,
		runnerID:  cfg.RunnerID,
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
	actions, err := ComputeActions(desired, actual, r.idle, time.Now().UTC())
	if err != nil {
		return err
	}
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

func (r *Reconciler) rollbackWorkload(ctx context.Context, workloadID string, zitiIdentityID *string, reason string) {
	if _, err := r.runner.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{
		WorkloadId: workloadID,
		TimeoutSec: r.stopSec,
	}); err != nil {
		log.Printf("reconciler: stop workload %s after %s: %v", workloadID, reason, err)
	}
	r.compensateIdentity(ctx, zitiIdentityID, reason)
}

func (r *Reconciler) startWorkload(ctx context.Context, target AgentThread) {
	assembled, err := r.assembler.Assemble(ctx, target.AgentID, target.ThreadID)
	if err != nil {
		log.Printf("reconciler: assemble workload for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		return
	}
	request := assembled.Request
	identity, err := r.createIdentity(ctx, target)
	if err != nil {
		log.Printf("reconciler: %v", err)
		return
	}
	zitiIdentityID := identity.idPtr()
	if identity != nil {
		if err := attachZitiEnrollmentJWT(request, identity.enrollmentJWT); err != nil {
			log.Printf("reconciler: set ziti enrollment jwt for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
			r.compensateIdentity(ctx, zitiIdentityID, "missing ziti sidecar")
			return
		}
	}
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
	status, err := runnerStatus(resp.GetStatus())
	if err != nil {
		log.Printf("reconciler: map workload status for workload %s: %v", resp.GetId(), err)
		r.rollbackWorkload(ctx, resp.GetId(), zitiIdentityID, "status map failure")
		return
	}
	containers := buildContainers(request, resp)
	zitiIdentityValue := ""
	if zitiIdentityID != nil {
		zitiIdentityValue = *zitiIdentityID
	}
	if _, err := r.runners.CreateWorkload(ctx, &runnersv1.CreateWorkloadRequest{
		Id:             resp.GetId(),
		RunnerId:       r.runnerID,
		ThreadId:       target.ThreadID.String(),
		AgentId:        target.AgentID.String(),
		OrganizationId: assembled.OrganizationID,
		Status:         status,
		Containers:     containers,
		ZitiIdentityId: zitiIdentityValue,
	}); err != nil {
		log.Printf("reconciler: create workload %s for agent %s thread %s: %v", resp.GetId(), target.AgentID.String(), target.ThreadID.String(), err)
		r.rollbackWorkload(ctx, resp.GetId(), zitiIdentityID, "create failure")
	}
}

func (r *Reconciler) stopWorkload(ctx context.Context, workload *runnersv1.Workload) {
	_, err := r.runner.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{
		WorkloadId: workload.GetMeta().GetId(),
		TimeoutSec: r.stopSec,
	})
	if err != nil {
		log.Printf("reconciler: stop workload %s: %v", workload.GetMeta().GetId(), err)
		return
	}
	if r.zitiMgmt != nil && workload.GetZitiIdentityId() != "" {
		if err := r.deleteIdentity(ctx, workload.GetZitiIdentityId()); err != nil {
			log.Printf("reconciler: delete ziti identity %s after stopping workload %s: %v", workload.GetZitiIdentityId(), workload.GetMeta().GetId(), err)
		}
	}
	if _, err := r.runners.DeleteWorkload(ctx, &runnersv1.DeleteWorkloadRequest{Id: workload.GetMeta().GetId()}); err != nil {
		log.Printf("reconciler: delete workload %s from runners: %v", workload.GetMeta().GetId(), err)
	}
}

func (r *Reconciler) deleteIdentity(ctx context.Context, identityID string) error {
	_, err := r.zitiMgmt.DeleteIdentity(ctx, &zitimgmtv1.DeleteIdentityRequest{ZitiIdentityId: identityID})
	return err
}

func runnerStatus(status runnerv1.WorkloadStatus) (runnersv1.WorkloadStatus, error) {
	switch status {
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, fmt.Errorf("runner returned unspecified workload status")
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, nil
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, nil
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, nil
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, nil
	default:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, fmt.Errorf("unknown runner workload status: %v", status)
	}
}

func buildContainers(request *runnerv1.StartWorkloadRequest, resp *runnerv1.StartWorkloadResponse) []*runnersv1.Container {
	containerInfo := resp.GetContainers()
	if containerInfo == nil {
		return nil
	}
	mainSpec := request.Main
	containers := []*runnersv1.Container{}
	if containerInfo.GetMain() != "" {
		container := &runnersv1.Container{
			ContainerId: containerInfo.GetMain(),
			Role:        runnersv1.ContainerRole_CONTAINER_ROLE_MAIN,
		}
		container.Name = mainSpec.GetName()
		container.Image = mainSpec.GetImage()
		containers = append(containers, container)
	}
	sidecarSpecs := make(map[string]*runnerv1.ContainerSpec, len(request.Sidecars))
	for _, sidecar := range request.Sidecars {
		sidecarSpecs[sidecar.GetName()] = sidecar
	}
	for _, sidecar := range containerInfo.GetSidecars() {
		if sidecar == nil || sidecar.GetId() == "" {
			continue
		}
		container := &runnersv1.Container{
			ContainerId: sidecar.GetId(),
			Name:        sidecar.GetName(),
			Role:        runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR,
		}
		if spec, ok := sidecarSpecs[sidecar.GetName()]; ok && spec != nil {
			container.Image = spec.GetImage()
		}
		containers = append(containers, container)
	}
	return containers
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

func attachZitiEnrollmentJWT(request *runnerv1.StartWorkloadRequest, jwt string) error {
	for _, container := range request.InitContainers {
		if container.Name == assembler.ZitiSidecarInitContainerName {
			container.Env = append(container.Env, &runnerv1.EnvVar{Name: "ZITI_ENROLL_TOKEN", Value: jwt})
			return nil
		}
	}
	return fmt.Errorf("missing ziti sidecar init container")
}
