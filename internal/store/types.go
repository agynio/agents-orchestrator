package store

import (
	"time"

	"github.com/google/uuid"
)

type Workload struct {
	ID         uuid.UUID
	WorkloadID string
	AgentID    uuid.UUID
	ThreadID   uuid.UUID
	StartedAt  time.Time
}
