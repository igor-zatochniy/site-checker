ALTER TABLE incidents
    ADD COLUMN IF NOT EXISTS last_alerted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS alert_count INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS alert_outbox (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    monitor_id TEXT NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL CHECK (event_type IN ('incident.failure')),
    payload JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'delivered', 'dead')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at TIMESTAMPTZ NOT NULL,
    lease_token TEXT,
    locked_until TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (
        (status = 'processing' AND lease_token IS NOT NULL AND locked_until IS NOT NULL)
        OR (status <> 'processing' AND lease_token IS NULL AND locked_until IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS alert_outbox_claim_idx
    ON alert_outbox (available_at, created_at, id)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS alert_outbox_stale_lease_idx
    ON alert_outbox (locked_until, id)
    WHERE status = 'processing';

CREATE INDEX IF NOT EXISTS alert_outbox_incident_idx
    ON alert_outbox (incident_id, created_at DESC);
