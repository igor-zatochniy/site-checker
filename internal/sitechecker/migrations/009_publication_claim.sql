ALTER TABLE check_jobs
    ADD COLUMN IF NOT EXISTS publication_token TEXT;

COMMENT ON COLUMN check_jobs.publication_token IS
    'Fencing token for scheduler publication claims; rotated on every claim.';
