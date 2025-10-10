CREATE OR REPLACE FUNCTION start_job_run(
    p_job_name TEXT,
    p_started_at TIMESTAMP
) RETURNS UUID AS $$
DECLARE
    v_job_id UUID;
    v_log_id UUID;
    v_created_at TIMESTAMP;
    v_interval_value INTEGER;
    v_interval_unit TEXT;
    v_run_count INTEGER;
    v_scheduled_for TIMESTAMP;
    v_next_run_at TIMESTAMP;
BEGIN
    -- 1. Get job configuration
    SELECT id, created_at, interval_value, interval_unit
    INTO v_job_id, v_created_at, v_interval_value, v_interval_unit
    FROM fts_job_master
    WHERE name = p_job_name;

    IF v_job_id IS NULL THEN
        RAISE EXCEPTION 'Job not found: %', p_job_name;
    END IF;

    -- 2. Calculate how many intervals have passed since created_at
    v_run_count := FLOOR(EXTRACT(EPOCH FROM p_started_at - v_created_at) / 
                         EXTRACT(EPOCH FROM (v_interval_value || ' ' || v_interval_unit)::interval))::int + 1;

    -- 3. Calculate scheduled_for (start of this run)
    v_scheduled_for := v_created_at + ((v_run_count - 1) * (v_interval_value || ' ' || v_interval_unit)::interval);

    -- 4. Calculate next_run_at (start of next run)
    v_next_run_at := v_created_at + (v_run_count * (v_interval_value || ' ' || v_interval_unit)::interval);

    -- 5. Insert job run log
    INSERT INTO fts_job_log (
        job_id,
        scheduled_for,
        status,
        started_at
    )
    VALUES (
        v_job_id,
        v_scheduled_for,
        'RUNNING',
        p_started_at
    )
    RETURNING id INTO v_log_id;

    -- 6. Update job master
    UPDATE fts_job_master
    SET
        last_run_start_at = p_started_at,
        last_run_status = 'RUNNING',
        next_run_at = v_next_run_at
    WHERE id = v_job_id;

    RETURN v_log_id;
END;
$$ LANGUAGE plpgsql;
