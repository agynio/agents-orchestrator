package reconciler

import (
	"context"
	"log"
	"time"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func workloadStatusPtr(status runnersv1.WorkloadStatus) *runnersv1.WorkloadStatus {
	return &status
}

func volumeStatusPtr(status runnersv1.VolumeStatus) *runnersv1.VolumeStatus {
	return &status
}

func stringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func boolPtr(value bool) *bool {
	return &value
}

func (r *Reconciler) markWorkloadFailed(ctx context.Context, workloadID string, instanceID *string) {
	status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
	req := &runnersv1.UpdateWorkloadRequest{
		Id:        workloadID,
		Status:    &status,
		RemovedAt: timestamppb.New(time.Now().UTC()),
	}
	if instanceID != nil && *instanceID != "" {
		req.InstanceId = instanceID
	}
	if _, err := r.runners.UpdateWorkload(ctx, req); err != nil {
		log.Printf("reconciler: update workload %s to failed: %v", workloadID, err)
	}
}

func (r *Reconciler) createWorkloadRecord(ctx context.Context, workloadID, runnerID string, target AgentThread, organizationID string, zitiIdentityID *string, allocatedCPUMillicores int32, allocatedRAMBytes int64) error {
	status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING
	zitiIdentityValue := ""
	if zitiIdentityID != nil {
		zitiIdentityValue = *zitiIdentityID
	}
	_, err := r.runners.CreateWorkload(ctx, &runnersv1.CreateWorkloadRequest{
		Id:                     workloadID,
		RunnerId:               runnerID,
		ThreadId:               target.ThreadID.String(),
		AgentId:                target.AgentID.String(),
		OrganizationId:         organizationID,
		Status:                 status,
		ZitiIdentityId:         zitiIdentityValue,
		AllocatedCpuMillicores: allocatedCPUMillicores,
		AllocatedRamBytes:      allocatedRAMBytes,
	})
	return err
}

func (r *Reconciler) createVolumeRecords(ctx context.Context, records []volumeRecord, runnerID string, target AgentThread, organizationID string) ([]volumeRecord, error) {
	if len(records) == 0 {
		return nil, nil
	}
	created := make([]volumeRecord, 0, len(records))
	for _, record := range records {
		if record.id == "" {
			return created, ErrInvalidVolumeRecord
		}
		if record.volumeID == "" {
			return created, ErrInvalidVolumeRecord
		}
		if record.sizeGB == "" {
			return created, ErrInvalidVolumeRecord
		}
		req := &runnersv1.CreateVolumeRequest{
			Id:             record.id,
			RunnerId:       runnerID,
			ThreadId:       target.ThreadID.String(),
			AgentId:        target.AgentID.String(),
			OrganizationId: organizationID,
			VolumeId:       record.volumeID,
			SizeGb:         record.sizeGB,
			Status:         runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
		}
		if _, err := r.runners.CreateVolume(ctx, req); err != nil {
			return created, err
		}
		created = append(created, record)
	}
	return created, nil
}

func (r *Reconciler) markVolumeRecordsFailed(ctx context.Context, records []volumeRecord) {
	if len(records) == 0 {
		return
	}
	status := runnersv1.VolumeStatus_VOLUME_STATUS_FAILED
	removedAt := timestamppb.New(time.Now().UTC())
	for _, record := range records {
		if record.id == "" {
			log.Printf("reconciler: volume record missing id")
			continue
		}
		_, err := r.runners.UpdateVolume(ctx, &runnersv1.UpdateVolumeRequest{
			Id:        record.id,
			Status:    &status,
			RemovedAt: removedAt,
		})
		if err != nil {
			log.Printf("reconciler: update volume %s to failed: %v", record.id, err)
		}
	}
}

var ErrInvalidVolumeRecord = errInvalidVolumeRecord{}

type errInvalidVolumeRecord struct{}

func (errInvalidVolumeRecord) Error() string {
	return "invalid volume record"
}
