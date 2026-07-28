ALTER TABLE monitors
    ADD COLUMN IF NOT EXISTS pending_job_id TEXT;

CREATE INDEX IF NOT EXISTS monitors_stale_pending_idx
    ON monitors (updated_at, pending_since, id)
    WHERE enabled = true AND pending = true;
