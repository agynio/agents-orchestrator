package store

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Workload struct {
	ID             uuid.UUID
	WorkloadID     string
	AgentID        uuid.UUID
	ThreadID       uuid.UUID
	ZitiIdentityID *string
	StartedAt      time.Time
}

type WorkloadStore interface {
	Insert(ctx context.Context, workloadID string, agentID, threadID uuid.UUID, zitiIdentityID *string) (*Workload, error)
	Delete(ctx context.Context, workloadID string) (*Workload, error)
	FindByWorkloadID(ctx context.Context, workloadID string) (*Workload, error)
	ListAll(ctx context.Context) ([]Workload, error)
}
