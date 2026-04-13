package reconciler

import (
	"strings"

	"github.com/google/uuid"
)

const runnerWorkloadPrefix = "workload-"

func normalizeRunnerWorkloadID(instanceID string) string {
	if !strings.HasPrefix(instanceID, runnerWorkloadPrefix) {
		return instanceID
	}
	trimmed := strings.TrimPrefix(instanceID, runnerWorkloadPrefix)
	if trimmed == "" {
		return instanceID
	}
	if _, err := uuid.Parse(trimmed); err != nil {
		return instanceID
	}
	return trimmed
}
