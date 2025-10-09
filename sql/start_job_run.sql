CREATE OR REPLACE FUNCTION start_job_run(
    p_job_name TEXT,
    p_started_at TIMESTAMP,
    p_next_run_at TIMESTAMP
) RETURNS INTEGER AS $$
DECLARE
    v_job_id INTEGER;
    v_log_id INTEGER;
    v_scheduled_for TIMESTAMP;
BEGIN
    -- Get job ID and current next_run_at from master
    SELECT id, next_run_at INTO v_job_id, v_scheduled_for
    FROM fts_job_master
    WHERE name = p_job_name;

    IF v_job_id IS NULL THEN
        RAISE EXCEPTION 'Job not found: %', p_job_name;
    END IF;

    -- Insert into job log
    INSERT INTO fts_job_log (
        job_id,
        scheduled_for,
        status,
        started_at
    )
    VALUES (
        v_job_id,
        v_scheduled_for,      -- use existing next_run_at as scheduled_for
        'RUNNING',
        p_started_at
    )
    RETURNING id INTO v_log_id;

    -- Update job master
    UPDATE fts_job_master
    SET
        last_run_start_at = p_started_at,
        last_run_status = 'RUNNING',
        next_run_at = p_next_run_at
    WHERE id = v_job_id;

    RETURN v_log_id;
END;
$$ LANGUAGE plpgsql;
