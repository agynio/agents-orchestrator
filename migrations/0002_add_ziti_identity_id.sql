ALTER TABLE workloads ADD COLUMN ziti_identity_id TEXT;
CREATE INDEX idx_workloads_ziti_identity_id ON workloads (ziti_identity_id);
