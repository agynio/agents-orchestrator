package runnerselect

import (
	"strings"
	"testing"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
)

func TestSelectReturnsErrorWhenNoRunners(t *testing.T) {
	if _, err := Select(nil, "org-1", nil, nil); err == nil {
		t.Fatal("expected error for empty runner list")
	}
}

func TestSelectFiltersByScopeStatusAndLabels(t *testing.T) {
	orgID := "org-1"
	runners := []*runnersv1.Runner{
		buildRunner("runner-a", orgID, runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, map[string]string{"region": "us"}, nil),
		buildRunner("runner-b", "org-2", runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, map[string]string{"region": "us"}, nil),
		buildRunner("runner-c", orgID, runnersv1.RunnerStatus_RUNNER_STATUS_PENDING, map[string]string{"region": "us"}, nil),
		buildRunner("runner-d", "", runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, map[string]string{"region": "eu"}, nil),
	}
	selected, err := Select(runners, orgID, map[string]string{"region": "us"}, nil)
	if err != nil {
		t.Fatalf("select runner: %v", err)
	}
	selectedID := selected.GetMeta().GetId()
	if selectedID != "runner-a" {
		t.Fatalf("expected runner-a, got %s", selectedID)
	}
}

func TestSelectAllowsClusterScopedRunner(t *testing.T) {
	orgID := "org-1"
	runners := []*runnersv1.Runner{
		buildRunner("runner-a", "", runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, nil, nil),
	}
	selected, err := Select(runners, orgID, nil, nil)
	if err != nil {
		t.Fatalf("select runner: %v", err)
	}
	if selected.GetMeta().GetId() != "runner-a" {
		t.Fatalf("expected runner-a, got %s", selected.GetMeta().GetId())
	}
}

func TestSelectErrorsWhenNoLabelsMatch(t *testing.T) {
	orgID := "org-1"
	runners := []*runnersv1.Runner{
		buildRunner("runner-a", orgID, runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, map[string]string{"tier": "cpu"}, nil),
	}
	if _, err := Select(runners, orgID, map[string]string{"tier": "gpu"}, nil); err == nil {
		t.Fatal("expected error when labels do not match")
	}
}

func TestSelectFiltersByCapabilities(t *testing.T) {
	orgID := "org-1"
	runners := []*runnersv1.Runner{
		buildRunner("runner-a", orgID, runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, nil, []string{"privileged", "dind"}),
	}
	selected, err := Select(runners, orgID, nil, []string{"privileged"})
	if err != nil {
		t.Fatalf("select runner: %v", err)
	}
	if selected.GetMeta().GetId() != "runner-a" {
		t.Fatalf("expected runner-a, got %s", selected.GetMeta().GetId())
	}
}

func TestSelectErrorsWhenCapabilitiesMissing(t *testing.T) {
	orgID := "org-1"
	runners := []*runnersv1.Runner{
		buildRunner("runner-a", orgID, runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, nil, []string{"privileged"}),
	}
	_, err := Select(runners, orgID, nil, []string{"dind"})
	if err == nil {
		t.Fatal("expected error when capabilities do not match")
	}
	message := err.Error()
	if !strings.Contains(message, "required capabilities") {
		t.Fatalf("expected required capabilities error, got %q", message)
	}
	if !strings.Contains(message, "runner-a") {
		t.Fatalf("expected missing capabilities to list runner, got %q", message)
	}
	if !strings.Contains(message, "dind") {
		t.Fatalf("expected missing capability in error, got %q", message)
	}
}

func buildRunner(id, orgID string, status runnersv1.RunnerStatus, labels map[string]string, capabilities []string) *runnersv1.Runner {
	var orgPtr *string
	if orgID != "" {
		orgPtr = &orgID
	}
	return &runnersv1.Runner{
		Meta:           &runnersv1.EntityMeta{Id: id},
		OrganizationId: orgPtr,
		Status:         status,
		Labels:         labels,
		Capabilities:   capabilities,
	}
}
