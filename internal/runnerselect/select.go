package runnerselect

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
)

func Select(runners []*runnersv1.Runner, organizationID string, agentRunnerLabels map[string]string, requiredCapabilities []string) (*runnersv1.Runner, error) {
	if len(runners) == 0 {
		return nil, errors.New("no runners available")
	}
	normalizedRequired := normalizeCapabilities(requiredCapabilities)
	eligible := make([]*runnersv1.Runner, 0, len(runners))
	missingByRunner := map[string][]string{}
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
		missing := missingCapabilities(normalizedRequired, runner.GetCapabilities())
		if len(missing) > 0 {
			missingByRunner[meta.GetId()] = missing
			continue
		}
		eligible = append(eligible, runner)
	}
	if len(eligible) == 0 {
		if len(normalizedRequired) > 0 && len(missingByRunner) > 0 {
			return nil, fmt.Errorf("no eligible runners found (required capabilities: %v, missing capabilities: %s)", normalizedRequired, formatMissingCapabilities(missingByRunner))
		}
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

func normalizeCapabilities(capabilities []string) []string {
	if len(capabilities) == 0 {
		return nil
	}
	unique := make(map[string]struct{}, len(capabilities))
	for _, capability := range capabilities {
		unique[capability] = struct{}{}
	}
	normalized := make([]string, 0, len(unique))
	for capability := range unique {
		normalized = append(normalized, capability)
	}
	sort.Strings(normalized)
	return normalized
}

func missingCapabilities(required, provided []string) []string {
	if len(required) == 0 {
		return nil
	}
	providedSet := make(map[string]struct{}, len(provided))
	for _, capability := range provided {
		providedSet[capability] = struct{}{}
	}
	missing := make([]string, 0, len(required))
	for _, capability := range required {
		if _, ok := providedSet[capability]; !ok {
			missing = append(missing, capability)
		}
	}
	return missing
}

func formatMissingCapabilities(missingByRunner map[string][]string) string {
	if len(missingByRunner) == 0 {
		return ""
	}
	runnerIDs := make([]string, 0, len(missingByRunner))
	for runnerID := range missingByRunner {
		runnerIDs = append(runnerIDs, runnerID)
	}
	sort.Strings(runnerIDs)
	parts := make([]string, 0, len(runnerIDs))
	for _, runnerID := range runnerIDs {
		parts = append(parts, fmt.Sprintf("%s:%v", runnerID, missingByRunner[runnerID]))
	}
	return strings.Join(parts, ", ")
}
