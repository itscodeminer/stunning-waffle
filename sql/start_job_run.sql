CREATE OR REPLACE FUNCTION start_job_run(
    p_job_name TEXT,
    p_started_at TIMESTAMP,
    p_interval_value INTEGER,
    p_interval_unit TEXT
) RETURNS INTEGER AS $$
DECLARE
    v_job_id INTEGER;
    v_log_id INTEGER;
    v_current_next_run TIMESTAMP;
    v_new_next_run TIMESTAMP;
    v_interval_expr TEXT;
BEGIN
    -- Get job ID and current next_run_at
    SELECT id, next_run_at INTO v_job_id, v_current_next_run
    FROM fts_job_master
    WHERE name = p_job_name;

    IF v_job_id IS NULL THEN
        RAISE EXCEPTION 'Job not found: %', p_job_name;
    END IF;

    -- Construct interval expression like '15 minutes'
    v_interval_expr := p_interval_value || ' ' || p_interval_unit;

    -- Compute new next_run_at
    v_new_next_run := v_current_next_run + v_interval_expr::interval;

    -- Insert into job log
    INSERT INTO fts_job_log (
        job_id,
        scheduled_for,
        status,
        started_at
    )
    VALUES (
        v_job_id,
        v_current_next_run,
        'RUNNING',
        p_started_at
    )
    RETURNING id INTO v_log_id;

    -- Update job master
    UPDATE fts_job_master
    SET
        last_run_start_at = p_started_at,
        last_run_status = 'RUNNING',
        next_run_at = v_new_next_run
    WHERE id = v_job_id;

    RETURN v_log_id;
END;
$$ LANGUAGE plpgsql;
