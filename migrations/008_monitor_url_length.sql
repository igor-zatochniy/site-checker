DO $$
DECLARE
    oversized_count BIGINT;
BEGIN
    SELECT count(*)
    INTO oversized_count
    FROM monitors
    WHERE char_length(url) > 8192;

    IF oversized_count > 0 THEN
        RAISE EXCEPTION
            'cannot enforce monitor URL length: % existing row(s) exceed 8192 characters',
            oversized_count
            USING
                ERRCODE = 'check_violation',
                HINT = 'Shorten or delete oversized monitor URLs before retrying migration 008_monitor_url_length.';
    END IF;
END
$$;

ALTER TABLE monitors
    ADD CONSTRAINT monitors_url_length_check
    CHECK (char_length(url) <= 8192);
