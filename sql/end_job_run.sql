CREATE OR REPLACE FUNCTION end_job_run(
    p_log_id INTEGER,
    p_status TEXT,
    p_ended_at TIMESTAMP,
    p_error_message TEXT DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    v_job_id INTEGER;
BEGIN
    -- Update the log entry
    UPDATE fts_job_log
    SET
        status = p_status,
        ended_at = p_ended_at,
        error_message = p_error_message
    WHERE id = p_log_id
    RETURNING job_id INTO v_job_id;

    -- Update the job master
    UPDATE fts_job_master
    SET
        last_run_end_at = p_ended_at,
        last_run_status = p_status,
        last_successful_at = CASE
            WHEN p_status = 'SUCCESS' THEN p_ended_at
            ELSE last_successful_at
        END
    WHERE id = v_job_id;
END;
$$ LANGUAGE plpgsql;
