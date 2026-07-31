ALTER TABLE check_jobs
    ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'scheduled',
    ADD COLUMN IF NOT EXISTS lease_token TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'check_jobs_kind_check'
          AND conrelid = 'check_jobs'::regclass
    ) THEN
        ALTER TABLE check_jobs
            ADD CONSTRAINT check_jobs_kind_check
            CHECK (kind IN ('scheduled', 'manual'));
    END IF;
END
$$;

COMMENT ON COLUMN check_jobs.kind IS
    'Job origin. Manual jobs do not advance the periodic monitor schedule.';
COMMENT ON COLUMN check_jobs.lease_token IS
    'Fencing token for the current processing owner; cleared outside processing.';
