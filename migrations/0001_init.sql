CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE workloads (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workload_id TEXT NOT NULL UNIQUE,
    agent_id    UUID NOT NULL,
    thread_id   UUID NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_workloads_agent_id ON workloads (agent_id);
CREATE INDEX idx_workloads_thread_id ON workloads (thread_id);
