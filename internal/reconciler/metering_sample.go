package reconciler

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	meteringv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/metering/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	meteringSamplePageSize int32 = 100
	meteringProducer             = "orchestrator"
	labelResource                = "resource"
	labelResourceID              = "resource_id"
	labelKind                    = "kind"
	labelThreadID                = "thread_id"
	labelAgentID                 = "agent_id"
	labelRunnerID                = "runner_id"
	labelIdentityID              = "identity_id"
	resourceWorkload             = "workload"
	resourceVolume               = "volume"
	kindRAM                      = "ram"
	kindStorage                  = "storage"
	unitCoreSecondsLabel         = "core_seconds"
	unitGBSecondsLabel           = "gb_seconds"
	microUnitValue         int64 = 1000000
	bytesPerGB             int64 = 1 << 30
)

func (r *Reconciler) runMeteringSampleLoop(ctx context.Context) {
	ticker := time.NewTicker(r.meteringSampleInterval)
	defer ticker.Stop()

	r.runMeteringSampleCycle(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runMeteringSampleCycle(ctx)
		}
	}
}

func (r *Reconciler) runMeteringSampleCycle(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	if err := r.sampleMetering(rctx, time.Now().UTC()); err != nil {
		log.Printf("reconciler: metering sample failed: %v", err)
	}
}

func (r *Reconciler) sampleMetering(ctx context.Context, now time.Time) error {
	orgIdentities, err := r.agentIdentityByOrg(ctx)
	if err != nil {
		return err
	}
	workloads, err := r.listPendingSampleWorkloads(ctx, orgIdentities)
	if err != nil {
		return err
	}
	volumes, err := r.listPendingSampleVolumes(ctx, orgIdentities)
	if err != nil {
		return err
	}
	if len(workloads) == 0 && len(volumes) == 0 {
		return nil
	}

	records := make([]*meteringv1.UsageRecord, 0, len(workloads)*2+len(volumes))
	workloadUpdates := make(map[string][]*runnersv1.SampledAtEntry)
	volumeUpdates := make(map[string][]*runnersv1.SampledAtEntry)

	for _, workload := range workloads {
		workloadRecords, update, err := sampleWorkloadMetering(workload, now)
		if err != nil {
			return err
		}
		if update != nil {
			meta := workload.GetMeta()
			if meta == nil {
				return fmt.Errorf("workload meta missing")
			}
			orgID := strings.TrimSpace(workload.GetOrganizationId())
			if orgID == "" {
				return fmt.Errorf("workload %s organization id missing", meta.GetId())
			}
			parsedOrgID, err := uuidutil.ParseUUID(orgID, "workload.organization_id")
			if err != nil {
				return err
			}
			identityID, ok := orgIdentities[parsedOrgID.String()]
			if !ok {
				return fmt.Errorf("workload %s missing identity for org %s", meta.GetId(), orgID)
			}
			workloadUpdates[identityID] = append(workloadUpdates[identityID], update)
		}
		records = append(records, workloadRecords...)
	}
	for _, volume := range volumes {
		volumeRecord, update, err := sampleVolumeMetering(volume, now)
		if err != nil {
			return err
		}
		if update != nil {
			meta := volume.GetMeta()
			if meta == nil {
				return fmt.Errorf("volume meta missing")
			}
			orgID := strings.TrimSpace(volume.GetOrganizationId())
			if orgID == "" {
				return fmt.Errorf("volume %s organization id missing", meta.GetId())
			}
			parsedOrgID, err := uuidutil.ParseUUID(orgID, "volume.organization_id")
			if err != nil {
				return err
			}
			identityID, ok := orgIdentities[parsedOrgID.String()]
			if !ok {
				return fmt.Errorf("volume %s missing identity for org %s", meta.GetId(), orgID)
			}
			volumeUpdates[identityID] = append(volumeUpdates[identityID], update)
		}
		if volumeRecord != nil {
			records = append(records, volumeRecord)
		}
	}

	if len(records) > 0 {
		if _, err := r.metering.Record(ctx, &meteringv1.RecordRequest{Records: records}); err != nil {
			return fmt.Errorf("record metering: %w", err)
		}
	}
	for identityID, entries := range workloadUpdates {
		if len(entries) == 0 {
			continue
		}
		runnerCtx, err := runnerIdentityContext(ctx, identityID)
		if err != nil {
			return err
		}
		if _, err := r.runners.BatchUpdateWorkloadSampledAt(runnerCtx, &runnersv1.BatchUpdateWorkloadSampledAtRequest{Entries: entries}); err != nil {
			return fmt.Errorf("update workloads sampled_at: %w", err)
		}
	}
	for identityID, entries := range volumeUpdates {
		if len(entries) == 0 {
			continue
		}
		runnerCtx, err := runnerIdentityContext(ctx, identityID)
		if err != nil {
			return err
		}
		if _, err := r.runners.BatchUpdateVolumeSampledAt(runnerCtx, &runnersv1.BatchUpdateVolumeSampledAtRequest{Entries: entries}); err != nil {
			return fmt.Errorf("update volumes sampled_at: %w", err)
		}
	}
	return nil
}

func (r *Reconciler) listPendingSampleWorkloads(ctx context.Context, orgIdentities map[string]string) ([]*runnersv1.Workload, error) {
	workloads := []*runnersv1.Workload{}
	if len(orgIdentities) == 0 {
		return workloads, nil
	}
	runnerCtx, err := r.clusterAdminRunnerContext(ctx)
	if err != nil {
		return nil, err
	}
	for orgID := range orgIdentities {
		orgIDCopy := orgID
		pageToken := ""
		for {
			resp, err := r.runners.ListWorkloads(runnerCtx, &runnersv1.ListWorkloadsRequest{
				PageSize:       meteringSamplePageSize,
				PageToken:      pageToken,
				OrganizationId: &orgIDCopy,
				PendingSample:  boolPtr(true),
			})
			if err != nil {
				return nil, fmt.Errorf("list workloads for metering: %w", err)
			}
			for _, workload := range resp.GetWorkloads() {
				if workload == nil {
					return nil, fmt.Errorf("workload is nil")
				}
				meta := workload.GetMeta()
				if meta == nil {
					return nil, fmt.Errorf("workload meta missing")
				}
				if meta.GetId() == "" {
					return nil, fmt.Errorf("workload meta id missing")
				}
				workloads = append(workloads, workload)
			}
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
	}
	return workloads, nil
}

func (r *Reconciler) listPendingSampleVolumes(ctx context.Context, orgIdentities map[string]string) ([]*runnersv1.Volume, error) {
	volumes := []*runnersv1.Volume{}
	if len(orgIdentities) == 0 {
		return volumes, nil
	}
	runnerCtx, err := r.clusterAdminRunnerContext(ctx)
	if err != nil {
		return nil, err
	}
	for orgID := range orgIdentities {
		orgIDCopy := orgID
		pageToken := ""
		for {
			resp, err := r.runners.ListVolumes(runnerCtx, &runnersv1.ListVolumesRequest{
				PageSize:       meteringSamplePageSize,
				PageToken:      pageToken,
				OrganizationId: &orgIDCopy,
				PendingSample:  boolPtr(true),
			})
			if err != nil {
				return nil, fmt.Errorf("list volumes for metering: %w", err)
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
				volumes = append(volumes, volume)
			}
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
	}
	return volumes, nil
}

func sampleWorkloadMetering(workload *runnersv1.Workload, now time.Time) ([]*meteringv1.UsageRecord, *runnersv1.SampledAtEntry, error) {
	meta := workload.GetMeta()
	if meta == nil || meta.GetId() == "" {
		return nil, nil, fmt.Errorf("workload meta missing")
	}
	workloadID := meta.GetId()
	intervalStart, intervalEnd, err := sampleWindow("workload", workloadID, meta.GetCreatedAt(), workload.GetLastMeteringSampledAt(), workload.GetRemovedAt(), now)
	if err != nil {
		return nil, nil, err
	}
	update := &runnersv1.SampledAtEntry{Id: workloadID, SampledAt: timestamppb.New(intervalEnd)}

	duration := intervalEnd.Sub(intervalStart)
	if duration <= 0 {
		return nil, update, nil
	}

	orgID := strings.TrimSpace(workload.GetOrganizationId())
	if orgID == "" {
		return nil, nil, fmt.Errorf("workload %s organization id missing", workloadID)
	}
	baseLabels, err := workloadLabels(workload, workloadID)
	if err != nil {
		return nil, nil, err
	}

	records := make([]*meteringv1.UsageRecord, 0, 2)
	cpuMillicores := int64(workload.GetAllocatedCpuMillicores())
	if cpuMillicores < 0 {
		return nil, nil, fmt.Errorf("workload %s allocated cpu millicores must be non-negative", workloadID)
	}
	if cpuMillicores > 0 {
		value, err := microCoreSeconds(cpuMillicores, duration)
		if err != nil {
			return nil, nil, fmt.Errorf("workload %s core seconds: %w", workloadID, err)
		}
		if value > 0 {
			records = append(records, &meteringv1.UsageRecord{
				OrgId:          orgID,
				IdempotencyKey: meteringKey(resourceWorkload, workloadID, unitCoreSecondsLabel, "", intervalEnd),
				Producer:       meteringProducer,
				Timestamp:      timestamppb.New(intervalEnd),
				Labels:         baseLabels,
				Unit:           meteringv1.Unit_UNIT_CORE_SECONDS,
				Value:          value,
			})
		}
	}
	ramBytes := workload.GetAllocatedRamBytes()
	if ramBytes < 0 {
		return nil, nil, fmt.Errorf("workload %s allocated ram bytes must be non-negative", workloadID)
	}
	if ramBytes > 0 {
		value, err := microGBSeconds(ramBytes, duration)
		if err != nil {
			return nil, nil, fmt.Errorf("workload %s ram gb seconds: %w", workloadID, err)
		}
		if value > 0 {
			labels := copyLabels(baseLabels)
			labels[labelKind] = kindRAM
			records = append(records, &meteringv1.UsageRecord{
				OrgId:          orgID,
				IdempotencyKey: meteringKey(resourceWorkload, workloadID, unitGBSecondsLabel, kindRAM, intervalEnd),
				Producer:       meteringProducer,
				Timestamp:      timestamppb.New(intervalEnd),
				Labels:         labels,
				Unit:           meteringv1.Unit_UNIT_GB_SECONDS,
				Value:          value,
			})
		}
	}
	return records, update, nil
}

func sampleVolumeMetering(volume *runnersv1.Volume, now time.Time) (*meteringv1.UsageRecord, *runnersv1.SampledAtEntry, error) {
	meta := volume.GetMeta()
	if meta == nil || meta.GetId() == "" {
		return nil, nil, fmt.Errorf("volume meta missing")
	}
	volumeID := meta.GetId()
	intervalStart, intervalEnd, err := sampleWindow("volume", volumeID, meta.GetCreatedAt(), volume.GetLastMeteringSampledAt(), volume.GetRemovedAt(), now)
	if err != nil {
		return nil, nil, err
	}
	update := &runnersv1.SampledAtEntry{Id: volumeID, SampledAt: timestamppb.New(intervalEnd)}

	duration := intervalEnd.Sub(intervalStart)
	if duration <= 0 {
		return nil, update, nil
	}

	orgID := strings.TrimSpace(volume.GetOrganizationId())
	if orgID == "" {
		return nil, nil, fmt.Errorf("volume %s organization id missing", volumeID)
	}
	labels, err := volumeLabels(volume, volumeID)
	if err != nil {
		return nil, nil, err
	}
	labels[labelKind] = kindStorage
	value, err := microGBSecondsFromSize(volume.GetSizeGb(), duration)
	if err != nil {
		return nil, nil, fmt.Errorf("volume %s storage gb seconds: %w", volumeID, err)
	}
	if value <= 0 {
		return nil, update, nil
	}
	return &meteringv1.UsageRecord{
		OrgId:          orgID,
		IdempotencyKey: meteringKey(resourceVolume, volumeID, unitGBSecondsLabel, kindStorage, intervalEnd),
		Producer:       meteringProducer,
		Timestamp:      timestamppb.New(intervalEnd),
		Labels:         labels,
		Unit:           meteringv1.Unit_UNIT_GB_SECONDS,
		Value:          value,
	}, update, nil
}

func sampleWindow(kind, id string, createdAt, lastSampledAt, removedAt *timestamppb.Timestamp, now time.Time) (time.Time, time.Time, error) {
	start := lastSampledAt
	if start == nil {
		start = createdAt
	}
	if start == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%s %s missing created_at", kind, id)
	}
	end := now
	if removedAt != nil {
		end = removedAt.AsTime().UTC()
	}
	return start.AsTime().UTC(), end, nil
}

func workloadLabels(workload *runnersv1.Workload, workloadID string) (map[string]string, error) {
	threadID := strings.TrimSpace(workload.GetThreadId())
	if threadID == "" {
		return nil, fmt.Errorf("workload %s thread id missing", workloadID)
	}
	agentID := strings.TrimSpace(workload.GetAgentId())
	if agentID == "" {
		return nil, fmt.Errorf("workload %s agent id missing", workloadID)
	}
	runnerID := strings.TrimSpace(workload.GetRunnerId())
	if runnerID == "" {
		return nil, fmt.Errorf("workload %s runner id missing", workloadID)
	}
	return map[string]string{
		labelResource:   resourceWorkload,
		labelResourceID: workloadID,
		labelThreadID:   threadID,
		labelAgentID:    agentID,
		labelRunnerID:   runnerID,
		labelIdentityID: agentID,
	}, nil
}

func volumeLabels(volume *runnersv1.Volume, volumeID string) (map[string]string, error) {
	threadID := strings.TrimSpace(volume.GetThreadId())
	if threadID == "" {
		return nil, fmt.Errorf("volume %s thread id missing", volumeID)
	}
	agentID := strings.TrimSpace(volume.GetAgentId())
	if agentID == "" {
		return nil, fmt.Errorf("volume %s agent id missing", volumeID)
	}
	runnerID := strings.TrimSpace(volume.GetRunnerId())
	if runnerID == "" {
		return nil, fmt.Errorf("volume %s runner id missing", volumeID)
	}
	return map[string]string{
		labelResource:   resourceVolume,
		labelResourceID: volumeID,
		labelThreadID:   threadID,
		labelAgentID:    agentID,
		labelRunnerID:   runnerID,
		labelIdentityID: agentID,
	}, nil
}

func microCoreSeconds(cpuMillicores int64, duration time.Duration) (int64, error) {
	nanos := duration.Nanoseconds()
	if nanos < 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	if cpuMillicores != 0 && nanos > math.MaxInt64/cpuMillicores {
		return 0, fmt.Errorf("core seconds overflow")
	}
	return cpuMillicores * nanos / microUnitValue, nil
}

func microGBSeconds(bytes int64, duration time.Duration) (int64, error) {
	nanos := duration.Nanoseconds()
	if nanos < 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	denominator := bytesPerGB * 1000
	if denominator <= 0 {
		return 0, fmt.Errorf("gb seconds denominator invalid")
	}
	if bytes == 0 || nanos == 0 {
		return 0, nil
	}
	numerator := big.NewInt(bytes)
	numerator.Mul(numerator, big.NewInt(nanos))
	numerator.Div(numerator, big.NewInt(denominator))
	if !numerator.IsInt64() {
		return 0, fmt.Errorf("gb seconds overflow")
	}
	return numerator.Int64(), nil
}

func microGBSecondsFromSize(sizeGB string, duration time.Duration) (int64, error) {
	trimmed := strings.TrimSpace(sizeGB)
	if trimmed == "" {
		return 0, fmt.Errorf("size_gb missing")
	}
	value, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size_gb %q: %w", trimmed, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("size_gb must be non-negative")
	}
	if value == 0 {
		return 0, nil
	}
	seconds := duration.Seconds()
	if seconds <= 0 {
		return 0, nil
	}
	if value > math.MaxInt64/seconds/float64(microUnitValue) {
		return 0, fmt.Errorf("gb seconds overflow")
	}
	return int64(math.Round(value * seconds * float64(microUnitValue))), nil
}

func meteringKey(resource, resourceID, unitLabel, kind string, intervalEnd time.Time) string {
	parts := []string{resource, resourceID, unitLabel, strconv.FormatInt(intervalEnd.UTC().UnixNano(), 10)}
	if kind != "" {
		parts = append(parts, kind)
	}
	return strings.Join(parts, ":")
}

func copyLabels(labels map[string]string) map[string]string {
	clone := make(map[string]string, len(labels))
	for key, value := range labels {
		clone[key] = value
	}
	return clone
}
