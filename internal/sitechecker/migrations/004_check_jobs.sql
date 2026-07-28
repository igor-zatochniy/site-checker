DROP INDEX IF EXISTS monitors_due_idx;
DROP INDEX IF EXISTS monitors_stale_pending_idx;

CREATE TABLE IF NOT EXISTS check_jobs (
    id TEXT PRIMARY KEY,
    monitor_id TEXT NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
    scheduled_for TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('scheduled', 'queued', 'processing', 'completed', 'failed', 'dead')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    available_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    processing_started_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS check_jobs_one_active_per_monitor_idx
    ON check_jobs (monitor_id)
    WHERE status IN ('scheduled', 'queued', 'processing', 'failed');

CREATE INDEX IF NOT EXISTS check_jobs_publishable_idx
    ON check_jobs (available_at, scheduled_for, id)
    WHERE status IN ('scheduled', 'failed');

CREATE INDEX IF NOT EXISTS check_jobs_processing_lease_idx
    ON check_jobs (lease_expires_at, id)
    WHERE status = 'processing';

CREATE INDEX IF NOT EXISTS check_jobs_queued_lease_idx
    ON check_jobs (published_at, id)
    WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS check_jobs_monitor_created_idx
    ON check_jobs (monitor_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS monitors_due_idx
    ON monitors (next_check_at, id)
    WHERE enabled = true;

COMMENT ON COLUMN monitors.pending IS
    'Deprecated compatibility column. check_jobs is the authoritative lifecycle store.';
COMMENT ON COLUMN monitors.pending_since IS
    'Deprecated compatibility column. check_jobs.lease_expires_at owns leases.';
COMMENT ON COLUMN monitors.pending_job_id IS
    'Deprecated compatibility column. check_jobs.id owns job identity.';
