ALTER TABLE check_results
    ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMPTZ;

UPDATE check_results AS result
SET recorded_at = COALESCE(
    (
        SELECT job.completed_at
        FROM check_jobs AS job
        WHERE job.id = result.job_id
            AND job.completed_at IS NOT NULL
    ),
    result.checked_at
)
WHERE result.recorded_at IS NULL;

ALTER TABLE check_results
    ALTER COLUMN recorded_at SET DEFAULT clock_timestamp(),
    ALTER COLUMN recorded_at SET NOT NULL;

DROP INDEX IF EXISTS check_results_monitor_checked_idx;
CREATE INDEX IF NOT EXISTS check_results_monitor_recorded_idx
    ON check_results (monitor_id, recorded_at DESC, id DESC);

DROP INDEX IF EXISTS check_results_retention_idx;
CREATE INDEX check_results_retention_idx
    ON check_results (recorded_at, id);
