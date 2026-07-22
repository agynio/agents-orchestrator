package reconciler

import (
	"context"
	"errors"
	"testing"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

func TestBuildVolumeRecordsUsesStablePersistentKey(t *testing.T) {
	threadID := uuid.New()
	volumeID := uuid.New()
	info := assembler.PersistentVolumeInfo{
		ID:     volumeID,
		Thread: threadID,
		Volume: &agentsv1.Volume{Size: "1Gi"},
		Spec:   &runnerv1.VolumeSpec{},
	}

	first, err := buildVolumeRecords([]assembler.PersistentVolumeInfo{info})
	if err != nil {
		t.Fatalf("build volume records: %v", err)
	}
	second, err := buildVolumeRecords([]assembler.PersistentVolumeInfo{info})
	if err != nil {
		t.Fatalf("build volume records again: %v", err)
	}

	expectedKey := uuid.NewSHA1(uuid.NameSpaceOID, []byte(threadID.String()+":"+volumeID.String())).String()
	if len(first) != 1 || first[0].id != expectedKey {
		t.Fatalf("expected first key %q, got %v", expectedKey, first)
	}
	if len(second) != 1 || second[0].id != expectedKey {
		t.Fatalf("expected second key %q, got %v", expectedKey, second)
	}
	if info.Spec.Labels[assembler.LabelVolumeKey] != expectedKey {
		t.Fatalf("expected volume label %q, got %q", expectedKey, info.Spec.Labels[assembler.LabelVolumeKey])
	}
}

func TestPrepareExistingVolumeRecordReactivatesFailedRetry(t *testing.T) {
	ctx := context.Background()
	recordID := uuid.NewString()
	threadID := uuid.NewString()
	agentID := uuid.NewString()
	runnerID := "runner-1"
	organizationID := uuid.NewString()
	volumeID := uuid.NewString()
	var updateReq *runnersv1.UpdateVolumeRequest
	runners := &fakeRunnersClient{
		getVolume: func(_ context.Context, req *runnersv1.GetVolumeRequest, _ ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
			if req.GetId() != recordID {
				return nil, errNotImplemented
			}
			return &runnersv1.GetVolumeResponse{Volume: &runnersv1.Volume{
				Meta:           &runnersv1.EntityMeta{Id: recordID},
				ThreadId:       threadID,
				AgentId:        agentID,
				RunnerId:       runnerID,
				VolumeId:       volumeID,
				OrganizationId: organizationID,
				Status:         runnersv1.VolumeStatus_VOLUME_STATUS_FAILED,
				OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
				OwnerId:        threadID,
			}}, nil
		},
		updateVolume: func(_ context.Context, req *runnersv1.UpdateVolumeRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error) {
			updateReq = req
			return &runnersv1.UpdateVolumeResponse{}, nil
		},
	}
	reconciler := &Reconciler{runners: runners}
	req := &runnersv1.CreateVolumeRequest{
		Id:             recordID,
		ThreadId:       threadID,
		AgentId:        agentID,
		RunnerId:       runnerID,
		VolumeId:       volumeID,
		OrganizationId: organizationID,
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:        threadID,
	}

	if err := reconciler.prepareExistingVolumeRecord(ctx, req); err != nil {
		t.Fatalf("prepare existing volume record: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected failed record to be updated")
	}
	if updateReq.GetId() != recordID {
		t.Fatalf("unexpected update id: %q", updateReq.GetId())
	}
	if updateReq.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
}

func TestPrepareExistingVolumeRecordRejectsConflictingRecord(t *testing.T) {
	ctx := context.Background()
	recordID := uuid.NewString()
	threadID := uuid.NewString()
	runners := &fakeRunnersClient{
		getVolume: func(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
			return &runnersv1.GetVolumeResponse{Volume: &runnersv1.Volume{
				Meta:           &runnersv1.EntityMeta{Id: recordID},
				ThreadId:       uuid.NewString(),
				AgentId:        uuid.NewString(),
				RunnerId:       "runner-1",
				VolumeId:       uuid.NewString(),
				OrganizationId: uuid.NewString(),
				Status:         runnersv1.VolumeStatus_VOLUME_STATUS_FAILED,
				OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
				OwnerId:        threadID,
			}}, nil
		},
		updateVolume: func(context.Context, *runnersv1.UpdateVolumeRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error) {
			return nil, errors.New("conflicting record must not be updated")
		},
	}
	reconciler := &Reconciler{runners: runners}
	req := &runnersv1.CreateVolumeRequest{
		Id:             recordID,
		ThreadId:       threadID,
		AgentId:        uuid.NewString(),
		RunnerId:       "runner-1",
		VolumeId:       uuid.NewString(),
		OrganizationId: uuid.NewString(),
		OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:        threadID,
	}

	if err := reconciler.prepareExistingVolumeRecord(ctx, req); err == nil {
		t.Fatal("expected conflicting existing record to fail")
	}
}
