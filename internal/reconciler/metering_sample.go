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
	workloads, err := r.listPendingSampleWorkloads(ctx)
	if err != nil {
		return err
	}
	volumes, err := r.listPendingSampleVolumes(ctx)
	if err != nil {
		return err
	}
	if len(workloads) == 0 && len(volumes) == 0 {
		return nil
	}

	records := make([]*meteringv1.UsageRecord, 0, len(workloads)*2+len(volumes))
	workloadUpdates := make([]*runnersv1.SampledAtEntry, 0, len(workloads))
	volumeUpdates := make([]*runnersv1.SampledAtEntry, 0, len(volumes))

	for _, workload := range workloads {
		workloadRecords, update, err := sampleWorkloadMetering(workload, now)
		if err != nil {
			return err
		}
		if update != nil {
			workloadUpdates = append(workloadUpdates, update)
		}
		records = append(records, workloadRecords...)
	}
	for _, volume := range volumes {
		volumeRecord, update, err := sampleVolumeMetering(volume, now)
		if err != nil {
			return err
		}
		if update != nil {
			volumeUpdates = append(volumeUpdates, update)
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
	callCtx := ctx
	if len(workloadUpdates) > 0 || len(volumeUpdates) > 0 {
		callCtx = r.serviceContext(ctx)
	}
	if len(workloadUpdates) > 0 {
		if _, err := r.runners.BatchUpdateWorkloadSampledAt(callCtx, &runnersv1.BatchUpdateWorkloadSampledAtRequest{Entries: workloadUpdates}); err != nil {
			return fmt.Errorf("update workloads sampled_at: %w", err)
		}
	}
	if len(volumeUpdates) > 0 {
		if _, err := r.runners.BatchUpdateVolumeSampledAt(callCtx, &runnersv1.BatchUpdateVolumeSampledAtRequest{Entries: volumeUpdates}); err != nil {
			return fmt.Errorf("update volumes sampled_at: %w", err)
		}
	}
	return nil
}

func (r *Reconciler) listPendingSampleWorkloads(ctx context.Context) ([]*runnersv1.Workload, error) {
	workloads := []*runnersv1.Workload{}
	callCtx := r.serviceContext(ctx)
	pageToken := ""
	for {
		resp, err := r.runners.ListWorkloads(callCtx, &runnersv1.ListWorkloadsRequest{
			PageSize:      meteringSamplePageSize,
			PageToken:     pageToken,
			PendingSample: boolPtr(true),
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
	return workloads, nil
}

func (r *Reconciler) listPendingSampleVolumes(ctx context.Context) ([]*runnersv1.Volume, error) {
	volumes := []*runnersv1.Volume{}
	callCtx := r.serviceContext(ctx)
	pageToken := ""
	for {
		resp, err := r.runners.ListVolumes(callCtx, &runnersv1.ListVolumesRequest{
			PageSize:      meteringSamplePageSize,
			PageToken:     pageToken,
			PendingSample: boolPtr(true),
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
		value, err := byteSeconds(ramBytes, duration)
		if err != nil {
			return nil, nil, fmt.Errorf("workload %s ram seconds: %w", workloadID, err)
		}
		if value > 0 {
			labels := copyLabelMap(baseLabels)
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
	volumeSize := strings.TrimSpace(volume.GetSizeGb())
	if volumeSize == "" {
		return nil, nil, fmt.Errorf("volume %s size_gb missing", volumeID)
	}
	parsedSize, err := strconv.ParseFloat(volumeSize, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("volume %s parse size_gb: %w", volumeID, err)
	}
	if parsedSize <= 0 {
		return nil, nil, nil
	}
	baseLabels, err := volumeLabels(volume, volumeID)
	if err != nil {
		return nil, nil, err
	}
	baseLabels[labelKind] = kindStorage
	value, err := gigabyteSeconds(parsedSize, duration)
	if err != nil {
		return nil, nil, fmt.Errorf("volume %s storage seconds: %w", volumeID, err)
	}
	if value == 0 {
		return nil, update, nil
	}
	return &meteringv1.UsageRecord{
		OrgId:          orgID,
		IdempotencyKey: meteringKey(resourceVolume, volumeID, unitGBSecondsLabel, kindStorage, intervalEnd),
		Producer:       meteringProducer,
		Timestamp:      timestamppb.New(intervalEnd),
		Labels:         baseLabels,
		Unit:           meteringv1.Unit_UNIT_GB_SECONDS,
		Value:          value,
	}, update, nil
}

func sampleWindow(resource, resourceID string, createdAt, lastSampledAt, removedAt *timestamppb.Timestamp, now time.Time) (time.Time, time.Time, error) {
	if createdAt == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%s %s created_at missing", resource, resourceID)
	}
	createdTime := createdAt.AsTime().UTC()
	intervalStart := createdTime
	if lastSampledAt != nil {
		intervalStart = lastSampledAt.AsTime().UTC()
	}
	intervalEnd := now
	if removedAt != nil {
		intervalEnd = removedAt.AsTime().UTC()
	}
	if intervalEnd.Before(createdTime) {
		return time.Time{}, time.Time{}, fmt.Errorf("%s %s interval_end before created_at", resource, resourceID)
	}
	if intervalStart.Before(createdTime) {
		intervalStart = createdTime
	}
	if intervalStart.After(intervalEnd) {
		return time.Time{}, time.Time{}, fmt.Errorf("%s %s interval_start after interval_end", resource, resourceID)
	}
	return intervalStart, intervalEnd, nil
}

func workloadLabels(workload *runnersv1.Workload, workloadID string) (map[string]string, error) {
	labels := map[string]string{}
	labels[labelResource] = resourceWorkload
	labels[labelResourceID] = workloadID
	labels[labelThreadID] = workload.GetThreadId()
	labels[labelAgentID] = workload.GetAgentId()
	labels[labelRunnerID] = workload.GetRunnerId()
	labels[labelIdentityID] = workload.GetAgentId()
	return labels, nil
}

func volumeLabels(volume *runnersv1.Volume, volumeID string) (map[string]string, error) {
	labels := map[string]string{}
	labels[labelResource] = resourceVolume
	labels[labelResourceID] = volumeID
	labels[labelThreadID] = volume.GetThreadId()
	labels[labelAgentID] = volume.GetAgentId()
	labels[labelRunnerID] = volume.GetRunnerId()
	labels[labelIdentityID] = volume.GetAgentId()
	return labels, nil
}

func copyLabelMap(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return map[string]string{}
	}
	copyLabels := make(map[string]string, len(labels))
	for key, value := range labels {
		copyLabels[key] = value
	}
	return copyLabels
}

func microCoreSeconds(millicores int64, duration time.Duration) (int64, error) {
	if millicores < 0 {
		return 0, fmt.Errorf("millicores must be non-negative")
	}
	if duration <= 0 {
		return 0, nil
	}
	micros := big.NewInt(millicores)
	micros.Mul(micros, big.NewInt(int64(duration.Seconds())))
	if !micros.IsInt64() {
		return 0, fmt.Errorf("millicores duration overflows int64")
	}
	return micros.Int64(), nil
}

func byteSeconds(bytes int64, duration time.Duration) (int64, error) {
	if bytes < 0 {
		return 0, fmt.Errorf("bytes must be non-negative")
	}
	if duration <= 0 {
		return 0, nil
	}
	seconds := duration.Seconds()
	if seconds <= 0 {
		return 0, nil
	}
	value := float64(bytes) * seconds / float64(bytesPerGB)
	if value < 0 {
		return 0, fmt.Errorf("bytes seconds underflow")
	}
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("bytes seconds overflow")
	}
	return int64(value * microUnitValue), nil
}

func gigabyteSeconds(sizeGB float64, duration time.Duration) (int64, error) {
	if sizeGB < 0 {
		return 0, fmt.Errorf("sizeGB must be non-negative")
	}
	if duration <= 0 {
		return 0, nil
	}
	seconds := duration.Seconds()
	if seconds <= 0 {
		return 0, nil
	}
	value := sizeGB * seconds
	if value < 0 {
		return 0, fmt.Errorf("sizeGB seconds underflow")
	}
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("sizeGB seconds overflow")
	}
	return int64(value * microUnitValue), nil
}
