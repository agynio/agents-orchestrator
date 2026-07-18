package reconciler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const sandboxPageSize int32 = 100

type sandboxWorkloadPlan struct {
	sandbox         *agentsv1.Sandbox
	sandboxID       uuid.UUID
	workspaceVolume *runnersv1.Volume
	activeWorkload  *runnersv1.Workload
}

func (r *Reconciler) reconcileSandboxes(ctx context.Context) error {
	sandboxes, err := r.listTrackedSandboxes(ctx)
	if err != nil {
		return err
	}
	for _, sandbox := range sandboxes {
		if err := r.reconcileSandbox(ctx, sandbox, time.Now().UTC()); err != nil {
			log.Printf("reconciler: sandbox %s failed: %v", sandbox.GetMeta().GetId(), err)
		}
	}
	return nil
}

func (r *Reconciler) listTrackedSandboxes(ctx context.Context) ([]*agentsv1.Sandbox, error) {
	orgIdentities, err := r.agentIdentityByOrg(ctx)
	if err != nil {
		return nil, err
	}
	if len(orgIdentities) == 0 {
		return nil, nil
	}
	var sandboxes []*agentsv1.Sandbox
	for orgID := range orgIdentities {
		pageToken := ""
		for {
			resp, err := r.agents.ListSandboxes(ctx, &agentsv1.ListSandboxesRequest{
				OrganizationId:    orgID,
				IncludeTerminated: true,
				PageSize:          sandboxPageSize,
				PageToken:         pageToken,
			})
			if err != nil {
				return nil, fmt.Errorf("list sandboxes for org %s: %w", orgID, err)
			}
			sandboxes = append(sandboxes, resp.GetSandboxes()...)
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
	}
	return sandboxes, nil
}

func (r *Reconciler) reconcileSandbox(ctx context.Context, sandbox *agentsv1.Sandbox, now time.Time) error {
	plan, err := r.loadSandboxWorkloadPlan(ctx, sandbox)
	if err != nil {
		return err
	}
	if ttlExpired(sandbox, now) && sandbox.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_TERMINATED {
		return r.terminateSandbox(ctx, plan)
	}
	switch sandbox.GetStatus() {
	case agentsv1.SandboxStatus_SANDBOX_STATUS_STARTING:
		if plan.activeWorkload == nil {
			return r.startSandboxWorkload(ctx, plan)
		}
		return nil
	case agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING:
		if plan.activeWorkload == nil {
			return r.startSandboxWorkload(ctx, plan)
		}
		if sandboxIdle(sandbox, plan.activeWorkload, now) {
			return r.stopSandboxWorkload(ctx, plan.activeWorkload)
		}
		return nil
	case agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED:
		if plan.activeWorkload != nil && sandboxIdle(sandbox, plan.activeWorkload, now) {
			return r.stopSandboxWorkload(ctx, plan.activeWorkload)
		}
		return nil
	case agentsv1.SandboxStatus_SANDBOX_STATUS_FAILED:
		if plan.activeWorkload != nil {
			return r.stopSandboxWorkload(ctx, plan.activeWorkload)
		}
		return nil
	case agentsv1.SandboxStatus_SANDBOX_STATUS_TERMINATED:
		if plan.activeWorkload != nil {
			if err := r.stopSandboxWorkload(ctx, plan.activeWorkload); err != nil {
				return err
			}
		}
		return r.deleteSandboxWorkspace(ctx, plan)
	case agentsv1.SandboxStatus_SANDBOX_STATUS_UNSPECIFIED:
		return fmt.Errorf("sandbox %s status unspecified", plan.sandboxID.String())
	default:
		return fmt.Errorf("sandbox %s status %s unsupported", plan.sandboxID.String(), sandbox.GetStatus().String())
	}
}

func (r *Reconciler) loadSandboxWorkloadPlan(ctx context.Context, sandbox *agentsv1.Sandbox) (*sandboxWorkloadPlan, error) {
	if sandbox == nil || sandbox.GetMeta() == nil {
		return nil, fmt.Errorf("sandbox meta missing")
	}
	sandboxID, err := uuidutil.ParseUUID(sandbox.GetMeta().GetId(), "sandbox.meta.id")
	if err != nil {
		return nil, err
	}
	workloads, err := r.listSandboxWorkloads(ctx, sandboxID.String())
	if err != nil {
		return nil, err
	}
	volumes, err := r.listSandboxVolumes(ctx, sandboxID.String())
	if err != nil {
		return nil, err
	}
	plan := &sandboxWorkloadPlan{sandbox: sandbox, sandboxID: sandboxID}
	for _, workload := range workloads {
		if isActiveWorkloadStatus(workload.GetStatus()) {
			if plan.activeWorkload != nil {
				if err := r.stopSandboxWorkload(ctx, workload); err != nil {
					return nil, err
				}
				continue
			}
			plan.activeWorkload = workload
		}
	}
	for _, volume := range volumes {
		if isPinnedVolumeStatus(volume.GetStatus()) {
			if plan.workspaceVolume != nil {
				return nil, fmt.Errorf("sandbox %s has multiple active workspace volumes", sandboxID.String())
			}
			plan.workspaceVolume = volume
		}
	}
	return plan, nil
}

func (r *Reconciler) listSandboxWorkloads(ctx context.Context, sandboxID string) ([]*runnersv1.Workload, error) {
	pageToken := ""
	var workloads []*runnersv1.Workload
	for {
		resp, err := r.runners.ListWorkloads(runnersContext(ctx), &runnersv1.ListWorkloadsRequest{
			PageSize:  activeWorkloadPageSize,
			PageToken: pageToken,
			Filter: &runnersv1.ListWorkloadsFilter{
				OwnerKindIn: []runnersv1.RuntimeOwnerKind{runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX},
				OwnerIdIn:   []string{sandboxID},
			},
		})
		if err != nil {
			return nil, fmt.Errorf("list sandbox workloads %s: %w", sandboxID, err)
		}
		workloads = append(workloads, resp.GetWorkloads()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return workloads, nil
		}
	}
}

func (r *Reconciler) listSandboxVolumes(ctx context.Context, sandboxID string) ([]*runnersv1.Volume, error) {
	pageToken := ""
	var volumes []*runnersv1.Volume
	for {
		resp, err := r.runners.ListVolumes(runnersContext(ctx), &runnersv1.ListVolumesRequest{
			PageSize:  activeVolumePageSize,
			PageToken: pageToken,
			Filter: &runnersv1.ListVolumesFilter{
				OwnerKindIn: []runnersv1.RuntimeOwnerKind{runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX},
				OwnerIdIn:   []string{sandboxID},
			},
		})
		if err != nil {
			return nil, fmt.Errorf("list sandbox volumes %s: %w", sandboxID, err)
		}
		volumes = append(volumes, resp.GetVolumes()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return volumes, nil
		}
	}
}

func (r *Reconciler) startSandboxWorkload(ctx context.Context, plan *sandboxWorkloadPlan) error {
	workspaceVolumeID := ""
	if plan.workspaceVolume != nil {
		workspaceVolumeID = plan.workspaceVolume.GetMeta().GetId()
	} else {
		workspaceVolumeID = uuid.NewString()
	}
	assembled, err := r.assembler.AssembleSandbox(ctx, plan.sandbox, workspaceVolumeID)
	if err != nil {
		return err
	}
	runnerID := strings.TrimSpace(assembled.RunnerID)
	if runnerID == "" {
		return fmt.Errorf("sandbox %s runner id missing", plan.sandboxID.String())
	}
	runner, enrolled, err := r.getRunnerIfEnrolled(ctx, runnerID)
	if err != nil {
		return err
	}
	if !enrolled {
		return fmt.Errorf("sandbox %s runner %s is not enrolled", plan.sandboxID.String(), runnerID)
	}
	if runner.GetMeta().GetId() == "" {
		return fmt.Errorf("sandbox %s runner meta missing", plan.sandboxID.String())
	}
	runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
	if err != nil {
		return fmt.Errorf("dial runner %s: %w", runnerID, err)
	}
	runnerCtx, err := runnerIdentityContext(ctx, assembled.OwnerID.String())
	if err != nil {
		return err
	}
	workloadID := uuid.NewString()
	request := assembled.Request
	request.WorkloadId = workloadID
	request.Main.Env = append(request.Main.Env, &runnerv1.EnvVar{Name: "WORKLOAD_ID", Value: workloadID})
	if request.AdditionalProperties == nil {
		request.AdditionalProperties = map[string]string{}
	}
	request.AdditionalProperties[assembler.LabelKeyPrefix+assembler.LabelWorkloadKey] = workloadID
	identity, err := r.createSandboxIdentity(ctx, plan.sandboxID, assembled.EnvironmentID, assembled.OwnerID, uuid.MustParse(workloadID), assembled.OrganizationID)
	if err != nil {
		return err
	}
	zitiIdentityID := identity.idPtr()
	if identity != nil {
		if err := attachZitiEnrollmentToken(request, identity.enrollmentJWT); err != nil {
			r.compensateIdentity(ctx, zitiIdentityID, "missing ziti enroll container")
			return err
		}
	}
	if plan.workspaceVolume == nil {
		if err := r.createSandboxWorkspaceRecord(runnerCtx, assembled, runnerID); err != nil {
			r.compensateIdentity(ctx, zitiIdentityID, "sandbox workspace record failure")
			return err
		}
	}
	if err := r.createSandboxWorkloadRecord(runnerCtx, workloadID, runnerID, assembled, zitiIdentityID); err != nil {
		r.markSandboxWorkspaceFailed(runnerCtx, plan.workspaceVolume, workspaceVolumeID)
		r.compensateIdentity(ctx, zitiIdentityID, "sandbox workload record failure")
		return err
	}
	resp, err := runnerClient.StartWorkload(runnerCtx, request)
	if err != nil {
		r.markWorkloadFailed(runnerCtx, workloadID, nil, runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED, err.Error(), nil)
		r.markSandboxWorkspaceFailed(runnerCtx, plan.workspaceVolume, workspaceVolumeID)
		r.compensateIdentity(ctx, zitiIdentityID, "sandbox start failure")
		return err
	}
	instanceID := normalizeRunnerWorkloadID(resp.GetId())
	containers := buildContainers(request, resp)
	if resp.GetStatus() == runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		failureMessage := failureSummary(resp.GetFailure())
		if instanceID != "" {
			if err := r.stopRunnerWorkload(runnerCtx, runnerClient, instanceID); err != nil {
				log.Printf("reconciler: stop sandbox workload %s after failure: %v", instanceID, err)
			}
		}
		r.markWorkloadFailed(runnerCtx, workloadID, stringPtr(instanceID), runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED, failureMessage, containers)
		r.markSandboxWorkspaceFailed(runnerCtx, plan.workspaceVolume, workspaceVolumeID)
		r.compensateIdentity(ctx, zitiIdentityID, "sandbox workload failure")
		return nil
	}
	if resp.GetId() != workloadID {
		if resp.GetId() != "" {
			if err := r.stopRunnerWorkload(runnerCtx, runnerClient, resp.GetId()); err != nil {
				log.Printf("reconciler: stop sandbox workload %s after id mismatch: %v", resp.GetId(), err)
			}
		}
		r.markWorkloadFailed(runnerCtx, workloadID, stringPtr(instanceID), runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED, "workload id mismatch", containers)
		r.markSandboxWorkspaceFailed(runnerCtx, plan.workspaceVolume, workspaceVolumeID)
		r.compensateIdentity(ctx, zitiIdentityID, "sandbox workload id mismatch")
		return nil
	}
	_, err = r.runners.UpdateWorkload(runnersContext(runnerCtx), &runnersv1.UpdateWorkloadRequest{
		Id:         workloadID,
		InstanceId: stringPtr(instanceID),
		Containers: containers,
	})
	return err
}

func (r *Reconciler) createSandboxWorkloadRecord(ctx context.Context, workloadID, runnerID string, assembled *assembler.SandboxAssembleResult, zitiIdentityID *string) error {
	status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING
	zitiIdentityValue := ""
	if zitiIdentityID != nil {
		zitiIdentityValue = *zitiIdentityID
	}
	_, err := r.runners.CreateWorkload(runnersContext(ctx), &runnersv1.CreateWorkloadRequest{
		Id:                     workloadID,
		RunnerId:               runnerID,
		OrganizationId:         assembled.OrganizationID,
		Status:                 status,
		ZitiIdentityId:         zitiIdentityValue,
		AllocatedCpuMillicores: assembled.AllocatedCPUMillicores,
		AllocatedRamBytes:      assembled.AllocatedRAMBytes,
		OwnerKind:              runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:                assembled.Request.GetAdditionalProperties()[assembler.LabelKeyPrefix+assembler.LabelSandboxID],
	})
	return err
}

func (r *Reconciler) createSandboxWorkspaceRecord(ctx context.Context, assembled *assembler.SandboxAssembleResult, runnerID string) error {
	_, err := r.runners.CreateVolume(runnersContext(ctx), &runnersv1.CreateVolumeRequest{
		Id:             assembled.WorkspaceVolumeID,
		RunnerId:       runnerID,
		OrganizationId: assembled.OrganizationID,
		SizeGb:         assembled.WorkspaceSizeGB,
		Status:         runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:        assembled.Request.GetAdditionalProperties()[assembler.LabelKeyPrefix+assembler.LabelSandboxID],
	})
	return err
}

func (r *Reconciler) createSandboxIdentity(ctx context.Context, sandboxID, environmentID, ownerID, workloadID uuid.UUID, organizationID string) (*identityInfo, error) {
	if r.zitiMgmt == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(
		"sandbox workload identity is blocked: agynio/api ziti_management.v1 has no CreateSandboxIdentity RPC for sandbox %s environment %s owner %s workload %s organization %s",
		sandboxID.String(),
		environmentID.String(),
		ownerID.String(),
		workloadID.String(),
		organizationID,
	)
}

func (r *Reconciler) stopSandboxWorkload(ctx context.Context, workload *runnersv1.Workload) error {
	if workload == nil || workload.GetMeta() == nil || workload.GetMeta().GetId() == "" {
		return fmt.Errorf("sandbox workload meta missing")
	}
	ownerID := strings.TrimSpace(workload.GetOwnerId())
	if ownerID == "" {
		ownerID = strings.TrimSpace(workload.GetAgentId())
	}
	runnerCtx, err := runnerIdentityContext(ctx, ownerID)
	if err != nil {
		return err
	}
	if err := r.stopWorkloadWithContext(runnerCtx, workload); err != nil {
		return err
	}
	return r.deleteIdentity(ctx, workload.GetZitiIdentityId())
}

func (r *Reconciler) terminateSandbox(ctx context.Context, plan *sandboxWorkloadPlan) error {
	if plan.activeWorkload != nil {
		if err := r.stopSandboxWorkload(ctx, plan.activeWorkload); err != nil {
			return err
		}
	}
	if err := r.deleteSandboxWorkspace(ctx, plan); err != nil {
		return err
	}
	_, err := r.agents.DeleteSandbox(ctx, &agentsv1.DeleteSandboxRequest{Id: plan.sandboxID.String()})
	return err
}

func (r *Reconciler) deleteSandboxWorkspace(ctx context.Context, plan *sandboxWorkloadPlan) error {
	volume := plan.workspaceVolume
	if volume == nil {
		return nil
	}
	ownerID := strings.TrimSpace(volume.GetOwnerId())
	if ownerID == "" {
		ownerID = strings.TrimSpace(volume.GetAgentId())
	}
	runnerCtx, err := runnerIdentityContext(ctx, ownerID)
	if err != nil {
		return err
	}
	if volume.GetRunnerId() != "" {
		runnerClient, err := r.runnerDialer.Dial(ctx, volume.GetRunnerId())
		if err != nil {
			return err
		}
		if _, err := runnerClient.RemoveVolume(runnerCtx, &runnerv1.RemoveVolumeRequest{VolumeName: volume.GetMeta().GetId(), Force: true}); err != nil {
			return err
		}
	}
	status := runnersv1.VolumeStatus_VOLUME_STATUS_DELETED
	_, err = r.runners.UpdateVolume(runnersContext(runnerCtx), &runnersv1.UpdateVolumeRequest{
		Id:        volume.GetMeta().GetId(),
		Status:    &status,
		RemovedAt: timestamppb.New(time.Now().UTC()),
	})
	return err
}

func (r *Reconciler) markSandboxWorkspaceFailed(ctx context.Context, existing *runnersv1.Volume, volumeID string) {
	if existing != nil {
		return
	}
	if volumeID == "" {
		return
	}
	status := runnersv1.VolumeStatus_VOLUME_STATUS_FAILED
	_, err := r.runners.UpdateVolume(runnersContext(ctx), &runnersv1.UpdateVolumeRequest{
		Id:        volumeID,
		Status:    &status,
		RemovedAt: timestamppb.New(time.Now().UTC()),
	})
	if err != nil {
		log.Printf("reconciler: update sandbox workspace %s to failed: %v", volumeID, err)
	}
}

func ttlExpired(sandbox *agentsv1.Sandbox, now time.Time) bool {
	meta := sandbox.GetMeta()
	if meta == nil || meta.GetCreatedAt() == nil {
		return false
	}
	ttl, err := time.ParseDuration(strings.TrimSpace(sandbox.GetTtl()))
	if err != nil || ttl <= 0 {
		return false
	}
	return !now.Before(meta.GetCreatedAt().AsTime().UTC().Add(ttl))
}

func sandboxIdle(sandbox *agentsv1.Sandbox, workload *runnersv1.Workload, now time.Time) bool {
	idleTimeout, err := time.ParseDuration(strings.TrimSpace(sandbox.GetIdleTimeout()))
	if err != nil || idleTimeout <= 0 {
		return false
	}
	activityAt, err := workloadActivityAt(workload)
	if err != nil {
		return false
	}
	return now.Sub(activityAt) > idleTimeout
}

func isActiveWorkloadStatus(status runnersv1.WorkloadStatus) bool {
	switch status {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		return true
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
		return false
	default:
		return false
	}
}
