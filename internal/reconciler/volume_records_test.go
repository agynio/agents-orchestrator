package reconciler

import (
	"testing"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/google/uuid"
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
