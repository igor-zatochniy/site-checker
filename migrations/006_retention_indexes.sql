CREATE INDEX IF NOT EXISTS check_results_retention_idx
    ON check_results (checked_at, id);

CREATE INDEX IF NOT EXISTS check_jobs_terminal_retention_idx
    ON check_jobs (updated_at, id)
    WHERE status IN ('completed', 'dead');

CREATE INDEX IF NOT EXISTS alert_outbox_terminal_retention_idx
    ON alert_outbox (updated_at, id)
    WHERE status IN ('delivered', 'dead');

CREATE INDEX IF NOT EXISTS incidents_resolved_retention_idx
    ON incidents (resolved_at, id)
    WHERE status = 'resolved';
