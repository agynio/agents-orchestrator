package runnerselect

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
)

func Select(runners []*runnersv1.Runner, organizationID string, agentRunnerLabels map[string]string) (*runnersv1.Runner, error) {
	if len(runners) == 0 {
		return nil, errors.New("no runners available")
	}
	eligible := make([]*runnersv1.Runner, 0, len(runners))
	for _, runner := range runners {
		if runner == nil {
			return nil, errors.New("runner entry is nil")
		}
		meta := runner.GetMeta()
		if meta == nil || meta.GetId() == "" {
			return nil, errors.New("runner missing id")
		}
		if !runnerInScope(runner, organizationID) {
			continue
		}
		if runner.GetStatus() != runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED {
			continue
		}
		if !labelsMatch(agentRunnerLabels, runner.GetLabels()) {
			continue
		}
		eligible = append(eligible, runner)
	}
	if len(eligible) == 0 {
		return nil, errors.New("no eligible runners found")
	}
	idx, err := rand.Int(rand.Reader, big.NewInt(int64(len(eligible))))
	if err != nil {
		return nil, fmt.Errorf("select runner: %w", err)
	}
	return eligible[idx.Int64()], nil
}

func runnerInScope(runner *runnersv1.Runner, organizationID string) bool {
	orgID := runner.GetOrganizationId()
	if orgID == "" {
		return true
	}
	return orgID == organizationID
}

func labelsMatch(required, provided map[string]string) bool {
	if len(required) == 0 {
		return true
	}
	if len(provided) == 0 {
		return false
	}
	for key, value := range required {
		if provided[key] != value {
			return false
		}
	}
	return true
}
