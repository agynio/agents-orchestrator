package runnerselect

import (
	"testing"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
)

func TestSelectReturnsErrorWhenNoRunners(t *testing.T) {
	if _, err := Select(nil, "org-1", nil); err == nil {
		t.Fatal("expected error for empty runner list")
	}
}

func TestSelectFiltersByScopeStatusAndLabels(t *testing.T) {
	orgID := "org-1"
	runners := []*runnersv1.Runner{
		buildRunner("runner-a", orgID, runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, map[string]string{"region": "us"}),
		buildRunner("runner-b", "org-2", runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, map[string]string{"region": "us"}),
		buildRunner("runner-c", orgID, runnersv1.RunnerStatus_RUNNER_STATUS_PENDING, map[string]string{"region": "us"}),
		buildRunner("runner-d", "", runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, map[string]string{"region": "eu"}),
	}
	selected, err := Select(runners, orgID, map[string]string{"region": "us"})
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
		buildRunner("runner-a", "", runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, nil),
	}
	selected, err := Select(runners, orgID, nil)
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
		buildRunner("runner-a", orgID, runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, map[string]string{"tier": "cpu"}),
	}
	if _, err := Select(runners, orgID, map[string]string{"tier": "gpu"}); err == nil {
		t.Fatal("expected error when labels do not match")
	}
}

func buildRunner(id, orgID string, status runnersv1.RunnerStatus, labels map[string]string) *runnersv1.Runner {
	var orgPtr *string
	if orgID != "" {
		orgPtr = &orgID
	}
	return &runnersv1.Runner{
		Meta:           &runnersv1.EntityMeta{Id: id},
		OrganizationId: orgPtr,
		Status:         status,
		Labels:         labels,
	}
}
