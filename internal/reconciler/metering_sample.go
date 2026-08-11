package reconciler

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
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
	labelSandboxID               = "sandbox_id"
	labelSandboxOwnerID          = "sandbox_owner_id"
	labelOwnerKind               = "owner_kind"
	labelRunnerID                = "runner_id"
	labelFlavor                  = "flavor"
	labelIdentityID              = "identity_id"
	labelAgentInstanceID         = "agent_instance_id"
	labelEnvironmentID           = "environment_id"
	resourceWorkload             = "workload"
	resourceVolume               = "volume"
	kindRAM                      = "ram"
	kindStorage                  = "storage"
	unitGBSecondsLabel           = "gb_seconds"
	unitFlavorSecondsLabel       = "flavor_seconds"
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
	agents, err := r.listAllAgents(ctx)
	if err != nil {
		return err
	}
	organizations, err := agentOrganizationsFrom(agents)
	if err != nil {
		return err
	}
	workloads, err := r.listPendingSampleWorkloads(ctx, organizations)
	if err != nil {
		return err
	}
	volumes, err := r.listPendingSampleVolumes(ctx, organizations)
	if err != nil {
		return err
	}
	if len(workloads) == 0 && len(volumes) == 0 {
		return nil
	}

	sources := labelSources{
		agentEnvironments: agentEnvironmentIDs(agents),
		sandboxes:         r.sandboxAttribution(ctx, workloads, volumes),
	}

	records := make([]*meteringv1.UsageRecord, 0, len(workloads)*2+len(volumes))
	workloadUpdates := make([]*runnersv1.SampledAtEntry, 0, len(workloads))
	volumeUpdates := make([]*runnersv1.SampledAtEntry, 0, len(volumes))

	for _, workload := range workloads {
		workloadRecords, update, err := sampleWorkloadMetering(workload, now, sources)
		if err != nil {
			return err
		}
		if update != nil {
			workloadUpdates = append(workloadUpdates, update)
		}
		records = append(records, workloadRecords...)
	}
	for _, volume := range volumes {
		volumeRecord, update, err := sampleVolumeMetering(volume, now, sources)
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
	if len(workloadUpdates) > 0 {
		if _, err := r.runners.BatchUpdateWorkloadSampledAt(ctx, &runnersv1.BatchUpdateWorkloadSampledAtRequest{Entries: workloadUpdates}); err != nil {
			return fmt.Errorf("update workloads sampled_at: %w", err)
		}
	}
	if len(volumeUpdates) > 0 {
		if _, err := r.runners.BatchUpdateVolumeSampledAt(ctx, &runnersv1.BatchUpdateVolumeSampledAtRequest{Entries: volumeUpdates}); err != nil {
			return fmt.Errorf("update volumes sampled_at: %w", err)
		}
	}
	return nil
}

func (r *Reconciler) listPendingSampleWorkloads(ctx context.Context, organizations map[string]struct{}) ([]*runnersv1.Workload, error) {
	workloads := []*runnersv1.Workload{}
	if len(organizations) == 0 {
		return workloads, nil
	}
	pageToken := ""
	for {
		resp, err := r.runners.ListWorkloads(ctx, &runnersv1.ListWorkloadsRequest{
			PageSize:  meteringSamplePageSize,
			PageToken: pageToken,
			Filter: &runnersv1.ListWorkloadsFilter{
				PendingSample: boolPtr(true),
			},
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
			orgID := strings.TrimSpace(workload.GetOrganizationId())
			if orgID == "" {
				return nil, fmt.Errorf("workload %s organization id missing", meta.GetId())
			}
			parsedOrgID, err := uuidutil.ParseUUID(orgID, "workload.organization_id")
			if err != nil {
				return nil, err
			}
			if _, ok := organizations[parsedOrgID.String()]; !ok {
				continue
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

func (r *Reconciler) listPendingSampleVolumes(ctx context.Context, organizations map[string]struct{}) ([]*runnersv1.Volume, error) {
	volumes := []*runnersv1.Volume{}
	if len(organizations) == 0 {
		return volumes, nil
	}
	pageToken := ""
	for {
		resp, err := r.runners.ListVolumes(ctx, &runnersv1.ListVolumesRequest{
			PageSize:  meteringSamplePageSize,
			PageToken: pageToken,
			Filter: &runnersv1.ListVolumesFilter{
				PendingSample: boolPtr(true),
			},
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
			orgID := strings.TrimSpace(volume.GetOrganizationId())
			if orgID == "" {
				return nil, fmt.Errorf("volume %s organization id missing", meta.GetId())
			}
			parsedOrgID, err := uuidutil.ParseUUID(orgID, "volume.organization_id")
			if err != nil {
				return nil, err
			}
			if _, ok := organizations[parsedOrgID.String()]; !ok {
				continue
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

// labelSources is what the sample cycle resolves once and every record it
// builds then reads: the environment behind each agent class, and the owner and
// environment behind each sandbox.
type labelSources struct {
	agentEnvironments map[string]string
	sandboxes         map[string]sandboxAttribution
}

type sandboxAttribution struct {
	ownerID       string
	environmentID string
}

// agentEnvironmentIDs maps each agent class to the environment it runs in.
// Resolved at sample time rather than read off the workload: the workload record
// does not carry one, so repointing a class moves its next samples, not its past
// ones.
func agentEnvironmentIDs(agents []*agentsv1.Agent) map[string]string {
	environments := make(map[string]string, len(agents))
	for _, agent := range agents {
		agentID := strings.TrimSpace(agent.GetMeta().GetId())
		environmentID := strings.TrimSpace(agent.GetEnvironmentId())
		if agentID == "" || environmentID == "" {
			continue
		}
		environments[agentID] = environmentID
	}
	return environments
}

// sandboxAttribution resolves what every sandbox with pending samples is
// attributed to: the user answerable for it and the environment it runs. Records
// are metered to the organization either way, so a sandbox that can no longer be
// resolved is labelled with neither rather than losing the whole sample cycle.
func (r *Reconciler) sandboxAttribution(ctx context.Context, workloads []*runnersv1.Workload, volumes []*runnersv1.Volume) map[string]sandboxAttribution {
	pending := map[string]struct{}{}
	for _, workload := range workloads {
		if workload.GetOwnerKind() != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
			continue
		}
		if sandboxID := strings.TrimSpace(workload.GetOwnerId()); sandboxID != "" {
			pending[sandboxID] = struct{}{}
		}
	}
	for _, volume := range volumes {
		if volume.GetOwnerKind() != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
			continue
		}
		if sandboxID := strings.TrimSpace(volume.GetOwnerId()); sandboxID != "" {
			pending[sandboxID] = struct{}{}
		}
	}
	if len(pending) == 0 {
		return nil
	}
	sandboxIDs := make([]string, 0, len(pending))
	for sandboxID := range pending {
		sandboxIDs = append(sandboxIDs, sandboxID)
	}
	sort.Strings(sandboxIDs)
	attribution := make(map[string]sandboxAttribution, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		resp, err := r.agents.GetSandbox(ctx, &agentsv1.GetSandboxRequest{Ref: &agentsv1.GetSandboxRequest_Id{Id: sandboxID}})
		if err != nil {
			log.Printf("reconciler: warn: get sandbox %s for metering labels: %v", sandboxID, err)
			continue
		}
		ownerID := strings.TrimSpace(resp.GetSandbox().GetOwnerId())
		if ownerID == "" {
			log.Printf("reconciler: warn: sandbox %s owner id missing for metering labels", sandboxID)
			continue
		}
		attribution[sandboxID] = sandboxAttribution{
			ownerID:       ownerID,
			environmentID: strings.TrimSpace(resp.GetSandbox().GetEnvironmentId()),
		}
	}
	return attribution
}

func sampleWorkloadMetering(workload *runnersv1.Workload, now time.Time, sources labelSources) ([]*meteringv1.UsageRecord, *runnersv1.SampledAtEntry, error) {
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
	baseLabels, err := workloadLabels(workload, workloadID, sources)
	if err != nil {
		return nil, nil, err
	}

	// Compute is billed by the flavor the workload occupies, for as long as it
	// occupies it. The flavor is read from the workload record rather than
	// re-resolved, so repointing an environment does not retroactively change
	// what a running workload bills.
	//
	// A workload with no flavor is one carrying an inline image and resources,
	// which is deprecated; it emits no compute record rather than keeping a
	// second billing shape alive.
	flavor := strings.TrimSpace(workload.GetFlavor())
	if flavor == "" {
		return nil, update, nil
	}
	value, err := microFlavorSeconds(duration)
	if err != nil {
		return nil, nil, fmt.Errorf("workload %s flavor seconds: %w", workloadID, err)
	}
	if value <= 0 {
		return nil, update, nil
	}
	labels := copyLabels(baseLabels)
	labels[labelFlavor] = flavor
	return []*meteringv1.UsageRecord{{
		OrgId:          orgID,
		IdempotencyKey: meteringKey(resourceWorkload, workloadID, unitFlavorSecondsLabel, "", intervalEnd),
		Producer:       meteringProducer,
		Timestamp:      timestamppb.New(intervalEnd),
		Labels:         labels,
		Unit:           meteringv1.Unit_UNIT_FLAVOR_SECONDS,
		Value:          value,
	}}, update, nil
}

func sampleVolumeMetering(volume *runnersv1.Volume, now time.Time, sources labelSources) (*meteringv1.UsageRecord, *runnersv1.SampledAtEntry, error) {
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
	labels, err := volumeLabels(volume, volumeID, sources)
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

func workloadLabels(workload *runnersv1.Workload, workloadID string, sources labelSources) (map[string]string, error) {
	runnerID := strings.TrimSpace(workload.GetRunnerId())
	if runnerID == "" {
		return nil, fmt.Errorf("workload %s runner id missing", workloadID)
	}
	labels := map[string]string{
		labelResource:   resourceWorkload,
		labelResourceID: workloadID,
		labelRunnerID:   runnerID,
	}
	if err := applyOwnerLabels(labels, workloadID, workload.GetOwnerKind(), workload.GetOwnerId(), workload.GetAgentId(), workload.GetThreadId(), sources); err != nil {
		return nil, err
	}
	return labels, nil
}

func volumeLabels(volume *runnersv1.Volume, volumeID string, sources labelSources) (map[string]string, error) {
	runnerID := strings.TrimSpace(volume.GetRunnerId())
	if runnerID == "" {
		return nil, fmt.Errorf("volume %s runner id missing", volumeID)
	}
	labels := map[string]string{
		labelResource:   resourceVolume,
		labelResourceID: volumeID,
		labelRunnerID:   runnerID,
	}
	if err := applyOwnerLabels(labels, volumeID, volume.GetOwnerKind(), volume.GetOwnerId(), volume.GetAgentId(), volume.GetThreadId(), sources); err != nil {
		return nil, err
	}
	return labels, nil
}

func applyOwnerLabels(labels map[string]string, resourceID string, ownerKind runnersv1.RuntimeOwnerKind, ownerID, agentID, threadID string, sources labelSources) error {
	switch ownerKind {
	case runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX:
		sandboxID := strings.TrimSpace(ownerID)
		if sandboxID == "" {
			return fmt.Errorf("resource %s sandbox owner id missing", resourceID)
		}
		labels[labelOwnerKind] = "sandbox"
		labels[labelSandboxID] = sandboxID
		labels[labelIdentityID] = sandboxID
		sandbox := sources.sandboxes[sandboxID]
		if sandbox.ownerID != "" {
			labels[labelSandboxOwnerID] = sandbox.ownerID
		}
		if sandbox.environmentID != "" {
			labels[labelEnvironmentID] = sandbox.environmentID
		}
	case runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_UNSPECIFIED,
		runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE:
		cleanAgentID := strings.TrimSpace(agentID)
		cleanInstanceID := strings.TrimSpace(ownerID)
		cleanThreadID := strings.TrimSpace(threadID)
		if cleanAgentID == "" {
			return fmt.Errorf("resource %s agent id missing", resourceID)
		}
		if cleanThreadID == "" {
			return fmt.Errorf("resource %s thread id missing", resourceID)
		}
		labels[labelOwnerKind] = "agent_instance"
		labels[labelThreadID] = cleanThreadID
		labels[labelAgentID] = cleanAgentID
		labels[labelIdentityID] = cleanAgentID
		// identity_id is the class here and the instance in the LLM Proxy's
		// records, so neither alone ranks a class and an instance on one axis.
		if cleanInstanceID != "" {
			labels[labelAgentInstanceID] = cleanInstanceID
		}
		if environmentID := sources.agentEnvironments[cleanAgentID]; environmentID != "" {
			labels[labelEnvironmentID] = environmentID
		}
	default:
		return fmt.Errorf("resource %s owner kind %s unsupported", resourceID, ownerKind.String())
	}
	return nil
}

// microFlavorSeconds is the interval duration in seconds, expressed in the same
// micro-units as every other value on the wire: one second of occupancy is one
// flavor-second.
func microFlavorSeconds(duration time.Duration) (int64, error) {
	nanos := duration.Nanoseconds()
	if nanos < 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	return nanos / (int64(time.Second) / microUnitValue), nil
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
