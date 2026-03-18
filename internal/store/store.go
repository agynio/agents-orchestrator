package store

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Insert(ctx context.Context, workloadID string, agentID, threadID uuid.UUID) (Workload, error) {
	var workload Workload
	err := s.pool.QueryRow(ctx, `INSERT INTO workloads (workload_id, agent_id, thread_id) VALUES ($1, $2, $3)
        RETURNING id, workload_id, agent_id, thread_id, started_at`, workloadID, agentID, threadID).Scan(
		&workload.ID,
		&workload.WorkloadID,
		&workload.AgentID,
		&workload.ThreadID,
		&workload.StartedAt,
	)
	if err != nil {
		return Workload{}, err
	}
	return workload, nil
}

func (s *Store) Delete(ctx context.Context, workloadID string) (bool, error) {
	cmd, err := s.pool.Exec(ctx, `DELETE FROM workloads WHERE workload_id = $1`, workloadID)
	if err != nil {
		return false, err
	}
	return cmd.RowsAffected() > 0, nil
}

func (s *Store) FindByAgentAndThread(ctx context.Context, agentID, threadID uuid.UUID) (*Workload, error) {
	var workload Workload
	err := s.pool.QueryRow(ctx, `SELECT id, workload_id, agent_id, thread_id, started_at FROM workloads WHERE agent_id = $1 AND thread_id = $2`, agentID, threadID).Scan(
		&workload.ID,
		&workload.WorkloadID,
		&workload.AgentID,
		&workload.ThreadID,
		&workload.StartedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &workload, nil
}

func (s *Store) ListAll(ctx context.Context) ([]Workload, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, workload_id, agent_id, thread_id, started_at FROM workloads ORDER BY started_at ASC, id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	workloads := []Workload{}
	for rows.Next() {
		var workload Workload
		if err := rows.Scan(&workload.ID, &workload.WorkloadID, &workload.AgentID, &workload.ThreadID, &workload.StartedAt); err != nil {
			return nil, err
		}
		workloads = append(workloads, workload)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return workloads, nil
}
