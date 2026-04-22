package reconciler

import (
	"context"
	"fmt"
	"log"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	meteringv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/metering/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/runnerdial"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	reconcileTimeout        = 30 * time.Second
	volumeReconcileInterval = time.Minute
)

type Reconciler struct {
	threads                   threadsv1.ThreadsServiceClient
	agents                    agentsv1.AgentsServiceClient
	runnerDialer              runnerdial.RunnerDialer
	runners                   runnersv1.RunnersServiceClient
	metering                  meteringv1.MeteringServiceClient
	meteringSampleInterval    time.Duration
	zitiMgmt                  zitimgmtv1.ZitiManagementServiceClient
	assembler                 *assembler.Assembler
	wake                      <-chan struct{}
	poll                      time.Duration
	workloadReconcileInterval time.Duration
	idle                      time.Duration
	stopSec                   uint32
}

type Config struct {
	Threads                   threadsv1.ThreadsServiceClient
	Agents                    agentsv1.AgentsServiceClient
	RunnerDialer              runnerdial.RunnerDialer
	Runners                   runnersv1.RunnersServiceClient
	Metering                  meteringv1.MeteringServiceClient
	ZitiMgmt                  zitimgmtv1.ZitiManagementServiceClient
	Assembler                 *assembler.Assembler
	Wake                      <-chan struct{}
	Poll                      time.Duration
	WorkloadReconcileInterval time.Duration
	Idle                      time.Duration
	StopSec                   uint32
	MeteringSampleInterval    time.Duration
}

func New(cfg Config) *Reconciler {
	return &Reconciler{
		threads:                   cfg.Threads,
		agents:                    cfg.Agents,
		runnerDialer:              cfg.RunnerDialer,
		runners:                   cfg.Runners,
		metering:                  cfg.Metering,
		meteringSampleInterval:    cfg.MeteringSampleInterval,
		zitiMgmt:                  cfg.ZitiMgmt,
		assembler:                 cfg.Assembler,
		wake:                      cfg.Wake,
		poll:                      cfg.Poll,
		workloadReconcileInterval: cfg.WorkloadReconcileInterval,
		idle:                      cfg.Idle,
		stopSec:                   cfg.StopSec,
	}
}

func (r *Reconciler) Run(ctx context.Context) error {
	if r.metering == nil {
		return fmt.Errorf("metering client not configured")
	}
	if r.meteringSampleInterval <= 0 {
		return fmt.Errorf("metering sample interval must be greater than 0")
	}
	go r.runWorkloadReconcileLoop(ctx)
	go r.runVolumeReconcileLoop(ctx)
	go r.runMeteringSampleLoop(ctx)

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
	desired, idleTimeouts, err := r.fetchDesired(ctx)
	if err != nil {
		return err
	}
	actual, err := r.fetchActual(ctx)
	if err != nil {
		return err
	}
	actions, err := ComputeActions(desired, actual, idleTimeouts, r.idle, time.Now().UTC())
	if err != nil {
		return err
	}
	degraded := newDegradeTracker()
	for _, candidate := range actions.ToStart {
		r.startWorkload(ctx, candidate, degraded)
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

func (r *Reconciler) createIdentity(ctx context.Context, target AgentThread, workloadID uuid.UUID) (*identityInfo, error) {
	if r.zitiMgmt == nil {
		return nil, nil
	}
	identityResp, err := r.zitiMgmt.CreateAgentIdentity(ctx, &zitimgmtv1.CreateAgentIdentityRequest{
		AgentId:    target.AgentID.String(),
		WorkloadId: workloadID.String(),
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

func (r *Reconciler) startWorkload(ctx context.Context, target AgentThread, degraded *degradeTracker) {
	assembled, err := r.assembler.Assemble(ctx, target.AgentID, target.ThreadID)
	if err != nil {
		log.Printf("reconciler: assemble workload for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		return
	}
	runnerCtx, err := r.runnerIdentityContextForAgent(ctx, target.AgentID)
	if err != nil {
		log.Printf("reconciler: build runner identity for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		return
	}
	pinnedRunnerID, err := r.pinnedRunnerForThread(runnerCtx, target.ThreadID.String())
	if err != nil {
		log.Printf("reconciler: list volumes for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		return
	}
	var selectedRunner *runnersv1.Runner
	if pinnedRunnerID != "" {
		runner, enrolled, err := r.getRunnerIfEnrolled(runnerCtx, pinnedRunnerID)
		if err != nil {
			log.Printf("reconciler: get runner %s for agent %s thread %s: %v", pinnedRunnerID, target.AgentID.String(), target.ThreadID.String(), err)
			return
		}
		if !enrolled {
			r.degradeThread(ctx, target.ThreadID.String(), degradeReasonRunnerDeprovisioned, degraded)
			return
		}
		selectedRunner = runner
	} else {
		selectedRunner, err = r.selectRunner(runnerCtx, target.AgentID.String(), assembled.OrganizationID, assembled.RunnerLabels, assembled.Request.GetCapabilities())
		if err != nil {
			log.Printf("reconciler: select runner for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
			return
		}
	}
	runnerID := selectedRunner.GetMeta().GetId()
	if runnerID == "" {
		log.Printf("reconciler: runner missing id for agent %s thread %s", target.AgentID.String(), target.ThreadID.String())
		return
	}
	runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
	if err != nil {
		log.Printf("reconciler: dial runner %s for agent %s thread %s: %v", runnerID, target.AgentID.String(), target.ThreadID.String(), err)
		return
	}
	request := assembled.Request
	workloadID := uuid.New()
	workloadIDValue := workloadID.String()
	if request.AdditionalProperties == nil {
		request.AdditionalProperties = map[string]string{}
	}
	request.AdditionalProperties[assembler.LabelKeyPrefix+assembler.LabelWorkloadKey] = workloadIDValue
	volumeRecords, err := buildVolumeRecords(assembled.PersistentVolumes)
	if err != nil {
		log.Printf("reconciler: build volume records for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		return
	}
	request.WorkloadId = workloadIDValue
	request.Main.Env = append(request.Main.Env, &runnerv1.EnvVar{Name: "WORKLOAD_ID", Value: workloadIDValue})
	identity, err := r.createIdentity(ctx, target, workloadID)
	if err != nil {
		log.Printf("reconciler: %v", err)
		return
	}
	zitiIdentityID := identity.idPtr()
	if identity != nil {
		if err := attachZitiEnrollmentToken(request, identity.enrollmentJWT); err != nil {
			log.Printf("reconciler: set ziti enrollment jwt for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
			r.compensateIdentity(ctx, zitiIdentityID, "missing ziti sidecar container")
			return
		}
	}
	createdVolumes, err := r.createVolumeRecords(runnerCtx, volumeRecords, runnerID, target, assembled.OrganizationID)
	if err != nil {
		log.Printf("reconciler: create volume records for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		r.markVolumeRecordsFailed(runnerCtx, createdVolumes)
		r.compensateIdentity(ctx, zitiIdentityID, "volume record failure")
		return
	}
	if err := r.createWorkloadRecord(runnerCtx, workloadIDValue, runnerID, target, assembled.OrganizationID, zitiIdentityID, assembled.AllocatedCPUMillicores, assembled.AllocatedRAMBytes); err != nil {
		log.Printf("reconciler: create workload record %s for agent %s thread %s: %v", workloadIDValue, target.AgentID.String(), target.ThreadID.String(), err)
		r.markVolumeRecordsFailed(runnerCtx, createdVolumes)
		r.compensateIdentity(ctx, zitiIdentityID, "workload record failure")
		return
	}
	resp, err := runnerClient.StartWorkload(runnerCtx, request)
	if err != nil {
		log.Printf("reconciler: start workload for agent %s thread %s: %v", target.AgentID.String(), target.ThreadID.String(), err)
		r.markWorkloadFailed(runnerCtx, workloadIDValue, nil)
		r.markVolumeRecordsFailed(runnerCtx, createdVolumes)
		r.compensateIdentity(ctx, zitiIdentityID, "start failure")
		return
	}
	rawInstanceID := resp.GetId()
	instanceID := normalizeRunnerWorkloadID(rawInstanceID)
	if resp.GetStatus() == runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		log.Printf("reconciler: workload failed for agent %s thread %s: %s", target.AgentID.String(), target.ThreadID.String(), failureSummary(resp.GetFailure()))
		if instanceID != "" {
			if err := r.stopRunnerWorkload(runnerCtx, runnerClient, instanceID); err != nil {
				log.Printf("reconciler: stop workload %s after failure: %v", instanceID, err)
			}
		}
		r.markWorkloadFailed(runnerCtx, workloadIDValue, stringPtr(instanceID))
		r.markVolumeRecordsFailed(runnerCtx, createdVolumes)
		r.compensateIdentity(ctx, zitiIdentityID, "workload failure")
		return
	}
	if rawInstanceID == "" {
		log.Printf("reconciler: workload started without id for agent %s thread %s", target.AgentID.String(), target.ThreadID.String())
		r.markWorkloadFailed(runnerCtx, workloadIDValue, nil)
		r.markVolumeRecordsFailed(runnerCtx, createdVolumes)
		r.compensateIdentity(ctx, zitiIdentityID, "missing workload id")
		return
	}
	if resp.GetId() != workloadIDValue {
		log.Printf("reconciler: workload id mismatch for agent %s thread %s (expected %s got %s)", target.AgentID.String(), target.ThreadID.String(), workloadIDValue, resp.GetId())
		instanceID := resp.GetId()
		if err := r.stopRunnerWorkload(runnerCtx, runnerClient, instanceID); err != nil {
			log.Printf("reconciler: stop workload %s after id mismatch: %v", instanceID, err)
		}
		r.markWorkloadFailed(runnerCtx, workloadIDValue, stringPtr(instanceID))
		r.markVolumeRecordsFailed(runnerCtx, createdVolumes)
		r.compensateIdentity(ctx, zitiIdentityID, "workload id mismatch")
		return
	}
	status, err := runnerStatus(resp.GetStatus())
	if err != nil {
		log.Printf("reconciler: map workload status for workload %s: %v", rawInstanceID, err)
		if err := r.stopRunnerWorkload(runnerCtx, runnerClient, instanceID); err != nil {
			log.Printf("reconciler: stop workload %s after status map failure: %v", instanceID, err)
		}
		r.markWorkloadFailed(runnerCtx, workloadIDValue, stringPtr(instanceID))
		r.markVolumeRecordsFailed(runnerCtx, createdVolumes)
		r.compensateIdentity(ctx, zitiIdentityID, "status map failure")
		return
	}
	containers := buildContainers(request, resp)
	updateReq := &runnersv1.UpdateWorkloadRequest{
		Id:         workloadIDValue,
		Status:     workloadStatusPtr(status),
		InstanceId: stringPtr(instanceID),
	}
	if containers != nil {
		updateReq.Containers = containers
	}
	if _, err := r.runners.UpdateWorkload(runnerCtx, updateReq); err != nil {
		log.Printf("reconciler: update workload record %s after start: %v", workloadIDValue, err)
	}
}

func (r *Reconciler) stopWorkload(ctx context.Context, workload *runnersv1.Workload) {
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		log.Printf("reconciler: workload missing id")
		return
	}
	runnerCtx, err := runnerIdentityContext(ctx, workload.GetAgentId())
	if err != nil {
		log.Printf("reconciler: build runner identity for workload %s: %v", workloadID, err)
		return
	}
	instanceID := normalizeRunnerWorkloadID(workload.GetInstanceId())
	if instanceID == "" {
		log.Printf("reconciler: workload %s missing instance id", workloadID)
		r.markWorkloadFailed(runnerCtx, workloadID, nil)
		return
	}
	runnerID := workload.GetRunnerId()
	if runnerID == "" {
		log.Printf("reconciler: workload %s missing runner id", workloadID)
		return
	}
	runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
	if err != nil {
		if runnerdial.IsNoTerminators(err) {
			if err := r.handleMissingRunnerWorkload(runnerCtx, workload); err != nil {
				log.Printf("reconciler: handle missing workload %s after runner dial failure: %v", workloadID, err)
			}
			return
		}
		log.Printf("reconciler: dial runner %s for workload %s: %v", runnerID, workloadID, err)
		return
	}
	stoppingStatus := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING
	if _, err := r.runners.UpdateWorkload(runnerCtx, &runnersv1.UpdateWorkloadRequest{
		Id:     workloadID,
		Status: &stoppingStatus,
	}); err != nil {
		log.Printf("reconciler: update workload %s to stopping: %v", workloadID, err)
	}
	workload.Status = stoppingStatus
	if err := r.stopRunnerWorkload(runnerCtx, runnerClient, instanceID); err != nil {
		if runnerdial.IsNoTerminators(err) {
			if err := r.handleMissingRunnerWorkload(runnerCtx, workload); err != nil {
				log.Printf("reconciler: handle missing workload %s after runner stop failure: %v", workloadID, err)
			}
			return
		}
		log.Printf("reconciler: stop workload %s: %v", workloadID, err)
		return
	}
	stoppedStatus := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
	if _, err := r.runners.UpdateWorkload(runnerCtx, &runnersv1.UpdateWorkloadRequest{
		Id:        workloadID,
		Status:    &stoppedStatus,
		RemovedAt: timestamppb.New(time.Now().UTC()),
	}); err != nil {
		log.Printf("reconciler: update workload %s to stopped: %v", workloadID, err)
	}
	if r.zitiMgmt != nil && workload.GetZitiIdentityId() != "" {
		if err := r.deleteIdentity(ctx, workload.GetZitiIdentityId()); err != nil {
			log.Printf("reconciler: delete ziti identity %s after stopping workload %s: %v", workload.GetZitiIdentityId(), workloadID, err)
		}
	}
}

func (r *Reconciler) stopRunnerWorkload(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, instanceID string) error {
	if err := r.stopRunnerWorkloadID(ctx, runnerClient, instanceID); err == nil {
		return nil
	} else if status.Code(err) != codes.NotFound {
		return err
	} else if _, parseErr := uuid.Parse(instanceID); parseErr != nil {
		return err
	}
	return r.stopRunnerWorkloadWithPrefix(ctx, runnerClient, instanceID)
}

func (r *Reconciler) stopRunnerWorkloadID(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, workloadID string) error {
	_, err := runnerClient.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{
		WorkloadId: workloadID,
		TimeoutSec: r.stopSec,
	})
	return err
}

func (r *Reconciler) stopRunnerWorkloadWithPrefix(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, instanceID string) error {
	prefixedID := runnerWorkloadPrefix + instanceID
	if err := r.stopRunnerWorkloadID(ctx, runnerClient, prefixedID); err == nil {
		return nil
	} else if status.Code(err) != codes.NotFound {
		return err
	}
	return nil
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
			Status:      runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING,
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
			log.Printf("reconciler: warn: skipping sidecar with missing id")
			continue
		}
		container := &runnersv1.Container{
			ContainerId: sidecar.GetId(),
			Name:        sidecar.GetName(),
			Role:        runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR,
			Status:      runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING,
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

func attachZitiEnrollmentToken(request *runnerv1.StartWorkloadRequest, jwt string) error {
	for _, container := range request.Sidecars {
		if container.Name == assembler.ZitiSidecarContainerName {
			container.Env = append(container.Env, &runnerv1.EnvVar{Name: assembler.ZitiEnrollmentTokenEnvVar, Value: jwt})
			return nil
		}
	}
	return fmt.Errorf("missing ziti sidecar container")
}
