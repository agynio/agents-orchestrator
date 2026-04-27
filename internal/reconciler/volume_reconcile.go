package reconciler

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/runnerdial"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const activeVolumePageSize int32 = 100
const workloadHistoryPageSize int32 = 100

type threadActivity struct {
	hasActive       bool
	latestRemovedAt *time.Time
}

type volumeTTLInfo struct {
	persistent bool
	ttl        *time.Duration
}

func (r *Reconciler) reconcileVolumes(ctx context.Context) error {
	if r.agents == nil {
		return fmt.Errorf("agents client not configured")
	}
	orgIdentities, err := r.agentIdentityByOrg(ctx)
	if err != nil {
		return err
	}
	tracked, err := r.listActiveVolumes(ctx, orgIdentities)
	if err != nil {
		return err
	}
	runnerIDs := map[string]struct{}{}
	volumesByRunner := make(map[string]map[string]*runnersv1.Volume)
	runnerIdentities := map[string]string{}
	for _, volume := range tracked {
		runnerID := volume.GetRunnerId()
		if runnerID == "" {
			log.Printf("reconciler: warn: volume %s missing runner id", volume.GetMeta().GetId())
			continue
		}
		identityID := strings.TrimSpace(volume.GetAgentId())
		if identityID == "" {
			return fmt.Errorf("volume %s missing agent id", volume.GetMeta().GetId())
		}
		volumeID := volume.GetMeta().GetId()
		if volumeID == "" {
			log.Printf("reconciler: warn: volume missing id")
			continue
		}
		runnerIDs[runnerID] = struct{}{}
		if volumesByRunner[runnerID] == nil {
			volumesByRunner[runnerID] = map[string]*runnersv1.Volume{}
		}
		volumesByRunner[runnerID][volumeID] = volume
		if _, ok := runnerIdentities[runnerID]; !ok {
			runnerIdentities[runnerID] = identityID
		}
	}
	runners, err := r.listRunnersByOrg(ctx, orgIdentities)
	if err != nil {
		return err
	}
	enrolledRunnerIDs := map[string]struct{}{}
	for _, runner := range runners {
		if runner == nil {
			continue
		}
		runnerID := runner.GetMeta().GetId()
		if runnerID == "" {
			continue
		}
		if runner.GetStatus() != runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED {
			continue
		}
		enrolledRunnerIDs[runnerID] = struct{}{}
		runnerIDs[runnerID] = struct{}{}
		if _, ok := runnerIdentities[runnerID]; ok {
			continue
		}
		orgID := strings.TrimSpace(runner.GetOrganizationId())
		if orgID == "" {
			return fmt.Errorf("runner %s organization id missing", runnerID)
		}
		identityID, ok := orgIdentities[orgID]
		if !ok {
			return fmt.Errorf("runner %s missing identity for org %s", runnerID, orgID)
		}
		runnerIdentities[runnerID] = identityID
	}

	degraded := newDegradeTracker()
	volumeInfoCache := map[string]volumeTTLInfo{}
	threadCache := map[string]threadActivity{}
	for runnerID := range runnerIDs {
		trackedVolumes := volumesByRunner[runnerID]
		identityID, ok := runnerIdentities[runnerID]
		if !ok {
			return fmt.Errorf("runner %s missing identity", runnerID)
		}
		if _, ok := enrolledRunnerIDs[runnerID]; !ok {
			for volumeID, volume := range trackedVolumes {
				volumeCtx, err := runnerIdentityContext(ctx, volume.GetAgentId())
				if err != nil {
					return err
				}
				if err := r.handleMissingRunnerVolume(volumeCtx, volume); err != nil {
					log.Printf("reconciler: warn: handle missing volume %s on unenrolled runner: %v", volumeID, err)
				}
				r.degradeThread(volumeCtx, volume.GetThreadId(), degradeReasonRunnerDeprovisioned, degraded)
			}
			continue
		}
		runnerCtx, err := runnerIdentityContext(ctx, identityID)
		if err != nil {
			return err
		}
		runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
		if err != nil {
			if runnerdial.IsNoTerminators(err) {
				for volumeID, volume := range trackedVolumes {
					volumeCtx, err := runnerIdentityContext(ctx, volume.GetAgentId())
					if err != nil {
						return err
					}
					if err := r.handleMissingRunnerVolume(volumeCtx, volume); err != nil {
						log.Printf("reconciler: warn: handle missing volume %s after runner dial failure: %v", volumeID, err)
					}
				}
				continue
			}
			log.Printf("reconciler: warn: dial runner %s for volume reconciliation: %v", runnerID, err)
			continue
		}
		resp, err := runnerClient.ListVolumes(runnerCtx, &runnerv1.ListVolumesRequest{})
		if err != nil {
			if runnerdial.IsNoTerminators(err) {
				for volumeID, volume := range trackedVolumes {
					volumeCtx, err := runnerIdentityContext(ctx, volume.GetAgentId())
					if err != nil {
						return err
					}
					if err := r.handleMissingRunnerVolume(volumeCtx, volume); err != nil {
						log.Printf("reconciler: warn: handle missing volume %s after runner list failure: %v", volumeID, err)
					}
				}
				continue
			}
			log.Printf("reconciler: warn: list volumes for runner %s: %v", runnerID, err)
			continue
		}
		runnerVolumes := make(map[string]*runnerv1.VolumeListItem)
		for _, item := range resp.GetVolumes() {
			if item == nil {
				continue
			}
			volumeKey := item.GetVolumeKey()
			if volumeKey == "" {
				log.Printf("reconciler: warn: runner %s volume missing volume_key", runnerID)
				continue
			}
			if _, ok := runnerVolumes[volumeKey]; ok {
				log.Printf("reconciler: warn: runner %s volume_key %s duplicated", runnerID, volumeKey)
				continue
			}
			runnerVolumes[volumeKey] = item
		}

		for volumeID, volume := range trackedVolumes {
			volumeCtx, err := runnerIdentityContext(ctx, volume.GetAgentId())
			if err != nil {
				return err
			}
			item, ok := runnerVolumes[volumeID]
			if !ok {
				if err := r.handleMissingRunnerVolume(volumeCtx, volume); err != nil {
					log.Printf("reconciler: warn: handle missing volume %s: %v", volumeID, err)
				}
				if volume.GetStatus() == runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
					r.degradeThread(volumeCtx, volume.GetThreadId(), degradeReasonVolumeLost, degraded)
				}
				continue
			}
			delete(runnerVolumes, volumeID)
			if err := r.handlePresentRunnerVolume(volumeCtx, runnerClient, volume, item, volumeInfoCache, threadCache); err != nil {
				log.Printf("reconciler: warn: handle volume %s on runner %s: %v", volumeID, runnerID, err)
			}
		}

		for _, item := range runnerVolumes {
			instanceID := item.GetInstanceId()
			if instanceID == "" {
				log.Printf("reconciler: warn: runner %s orphan volume missing instance id", runnerID)
				continue
			}
			if err := r.removeRunnerVolume(runnerCtx, runnerClient, instanceID); err != nil {
				log.Printf("reconciler: warn: remove orphan volume %s on runner %s: %v", instanceID, runnerID, err)
			}
		}
	}
	return nil
}

func (r *Reconciler) listActiveVolumes(ctx context.Context, orgIdentities map[string]string) ([]*runnersv1.Volume, error) {
	active := []*runnersv1.Volume{}
	if len(orgIdentities) == 0 {
		return active, nil
	}
	pageToken := ""
	statuses := []runnersv1.VolumeStatus{
		runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
		runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
		runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING,
	}
	for {
		resp, err := r.runners.ListVolumes(runnersContext(ctx), &runnersv1.ListVolumesRequest{
			PageSize:  activeVolumePageSize,
			PageToken: pageToken,
			Filter: &runnersv1.ListVolumesFilter{
				StatusIn: statuses,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("list volumes: %w", err)
		}
		for _, volume := range resp.GetVolumes() {
			if volume == nil {
				return nil, fmt.Errorf("volume is nil")
			}
			meta := volume.GetMeta()
			if meta == nil {
				return nil, fmt.Errorf("volume meta missing")
			}
			if meta.GetId() == "" {
				return nil, fmt.Errorf("volume meta id missing")
			}
			orgID := strings.TrimSpace(volume.GetOrganizationId())
			if orgID == "" {
				return nil, fmt.Errorf("volume %s organization id missing", meta.GetId())
			}
			parsedOrgID, err := uuidutil.ParseUUID(orgID, "volume.organization_id")
			if err != nil {
				return nil, err
			}
			if _, ok := orgIdentities[parsedOrgID.String()]; !ok {
				continue
			}
			active = append(active, volume)
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return active, nil
}

func (r *Reconciler) handleMissingRunnerVolume(ctx context.Context, volume *runnersv1.Volume) error {
	volumeID := volume.GetMeta().GetId()
	if volumeID == "" {
		return nil
	}
	switch volume.GetStatus() {
	case runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING:
		return nil
	case runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE:
		status := runnersv1.VolumeStatus_VOLUME_STATUS_FAILED
		_, err := r.runners.UpdateVolume(runnersContext(ctx), &runnersv1.UpdateVolumeRequest{
			Id:     volumeID,
			Status: &status,
		})
		return err
	case runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING:
		status := runnersv1.VolumeStatus_VOLUME_STATUS_DELETED
		_, err := r.runners.UpdateVolume(runnersContext(ctx), &runnersv1.UpdateVolumeRequest{
			Id:        volumeID,
			Status:    &status,
			RemovedAt: timestamppb.New(time.Now().UTC()),
		})
		return err
	default:
		return nil
	}
}

func (r *Reconciler) handlePresentRunnerVolume(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, volume *runnersv1.Volume, item *runnerv1.VolumeListItem, volumeInfoCache map[string]volumeTTLInfo, threadCache map[string]threadActivity) error {
	volumeID := volume.GetMeta().GetId()
	if volumeID == "" {
		return nil
	}
	instanceID := item.GetInstanceId()
	if instanceID == "" {
		return nil
	}
	switch volume.GetStatus() {
	case runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING:
		status := runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE
		_, err := r.runners.UpdateVolume(runnersContext(ctx), &runnersv1.UpdateVolumeRequest{
			Id:         volumeID,
			Status:     &status,
			InstanceId: stringPtr(instanceID),
		})
		return err
	case runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE:
		if volume.GetInstanceId() != instanceID {
			if _, err := r.runners.UpdateVolume(runnersContext(ctx), &runnersv1.UpdateVolumeRequest{
				Id:         volumeID,
				InstanceId: stringPtr(instanceID),
			}); err != nil {
				return err
			}
		}
		expired, err := r.volumeTTLExpired(ctx, volume, volumeInfoCache, threadCache)
		if err != nil {
			return err
		}
		if !expired {
			return nil
		}
		status := runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING
		if _, err := r.runners.UpdateVolume(runnersContext(ctx), &runnersv1.UpdateVolumeRequest{Id: volumeID, Status: &status}); err != nil {
			return err
		}
		return r.removeRunnerVolume(ctx, runnerClient, instanceID)
	case runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING:
		return r.removeRunnerVolume(ctx, runnerClient, instanceID)
	default:
		return nil
	}
}

func (r *Reconciler) removeRunnerVolume(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, instanceID string) error {
	_, err := runnerClient.RemoveVolume(ctx, &runnerv1.RemoveVolumeRequest{
		VolumeName: instanceID,
		Force:      true,
	})
	return err
}

func (r *Reconciler) volumeTTLExpired(ctx context.Context, volume *runnersv1.Volume, volumeInfoCache map[string]volumeTTLInfo, threadCache map[string]threadActivity) (bool, error) {
	volumeID := volume.GetVolumeId()
	if volumeID == "" {
		return false, fmt.Errorf("volume %s missing volume_id", volume.GetMeta().GetId())
	}
	info, err := r.volumeTTLInfo(ctx, volumeID, volumeInfoCache)
	if err != nil {
		return false, err
	}
	if !info.persistent || info.ttl == nil {
		return false, nil
	}
	threadID := volume.GetThreadId()
	if threadID == "" {
		return false, fmt.Errorf("volume %s missing thread_id", volume.GetMeta().GetId())
	}
	activity, err := r.threadActivity(ctx, threadID, threadCache)
	if err != nil {
		return false, err
	}
	if activity.hasActive || activity.latestRemovedAt == nil {
		return false, nil
	}
	if time.Since(*activity.latestRemovedAt) < *info.ttl {
		return false, nil
	}
	return true, nil
}

func (r *Reconciler) volumeTTLInfo(ctx context.Context, volumeID string, cache map[string]volumeTTLInfo) (volumeTTLInfo, error) {
	if cached, ok := cache[volumeID]; ok {
		return cached, nil
	}
	resp, err := r.agents.GetVolume(ctx, &agentsv1.GetVolumeRequest{Id: volumeID})
	if err != nil {
		return volumeTTLInfo{}, fmt.Errorf("get volume %s: %w", volumeID, err)
	}
	volume := resp.GetVolume()
	if volume == nil {
		return volumeTTLInfo{}, fmt.Errorf("volume %s missing", volumeID)
	}
	info := volumeTTLInfo{persistent: volume.GetPersistent()}
	if ttl := volume.GetTtl(); ttl != "" {
		parsed, err := parseVolumeTTL(ttl)
		if err != nil {
			return volumeTTLInfo{}, err
		}
		info.ttl = &parsed
	}
	cache[volumeID] = info
	return info, nil
}

func (r *Reconciler) threadActivity(ctx context.Context, threadID string, cache map[string]threadActivity) (threadActivity, error) {
	if cached, ok := cache[threadID]; ok {
		return cached, nil
	}
	workloads, err := r.listWorkloadsByThread(ctx, threadID, nil, nil, 0)
	if err != nil {
		return threadActivity{}, err
	}
	activity := threadActivity{}
	for _, workload := range workloads {
		switch workload.GetStatus() {
		case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
			activity.hasActive = true
		}
		removedAt := workload.GetRemovedAt()
		if removedAt == nil {
			continue
		}
		removedTime := removedAt.AsTime()
		if activity.latestRemovedAt == nil || removedTime.After(*activity.latestRemovedAt) {
			copy := removedTime
			activity.latestRemovedAt = &copy
		}
	}
	cache[threadID] = activity
	return activity, nil
}

func parseVolumeTTL(value string) (time.Duration, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("ttl is empty")
	}
	parsed, err := time.ParseDuration(trimmed)
	if err == nil {
		if parsed <= 0 {
			return 0, fmt.Errorf("ttl must be greater than 0")
		}
		return parsed, nil
	}
	if !strings.HasSuffix(trimmed, "d") {
		return 0, fmt.Errorf("parse ttl %q: %w", value, err)
	}
	dayValue := strings.TrimSuffix(trimmed, "d")
	floatValue, parseErr := strconv.ParseFloat(dayValue, 64)
	if parseErr != nil {
		return 0, fmt.Errorf("parse ttl %q: %w", value, parseErr)
	}
	if floatValue <= 0 {
		return 0, fmt.Errorf("ttl must be greater than 0")
	}
	return time.Duration(floatValue * float64(24*time.Hour)), nil
}
