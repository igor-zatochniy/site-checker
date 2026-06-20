CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS monitors (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds BETWEEN 30 AND 86400),
    timeout_seconds INTEGER NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 60),
    expected_status INTEGER NOT NULL CHECK (expected_status BETWEEN 100 AND 599),
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    enabled BOOLEAN NOT NULL,
    next_check_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_status_code INTEGER,
    last_latency_ms BIGINT,
    last_checked_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    pending BOOLEAN NOT NULL DEFAULT false,
    pending_since TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS monitors_due_idx
    ON monitors (next_check_at, id)
    WHERE enabled = true AND pending = false;

CREATE TABLE IF NOT EXISTS check_results (
    id TEXT PRIMARY KEY,
    job_id TEXT UNIQUE,
    monitor_id TEXT NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
    status_code INTEGER NOT NULL,
    latency_ms BIGINT NOT NULL,
    error TEXT NOT NULL DEFAULT '',
    success BOOLEAN NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS check_results_monitor_checked_idx
    ON check_results (monitor_id, checked_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS incidents (
    id TEXT PRIMARY KEY,
    monitor_id TEXT NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('open', 'resolved')),
    failure_count INTEGER NOT NULL DEFAULT 1,
    first_failure_at TIMESTAMPTZ NOT NULL,
    last_failure_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS incidents_one_open_per_monitor_idx
    ON incidents (monitor_id)
    WHERE status = 'open';

CREATE INDEX IF NOT EXISTS incidents_status_created_idx
    ON incidents (status, created_at DESC);
