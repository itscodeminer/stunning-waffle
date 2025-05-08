CREATE OR REPLACE FUNCTION get_daily_summary(
    start_date DATE,
    end_date DATE,
    assoc_manager_tid VARCHAR DEFAULT NULL,
    manager_tid VARCHAR DEFAULT NULL
)
RETURNS TABLE (
    report_date DATE,
    full_name TEXT,
    tech_id TEXT,
    zone TEXT,
    team TEXT,
    open_srs INT,
    closed_srs INT,
    activity_count INT,
    time_task_count INT
) AS $$
BEGIN
    RETURN QUERY
    WITH date_series AS (
        SELECT generate_series(start_date, end_date, INTERVAL '1 day')::date AS report_date
    ),
    filtered_staff AS (
        SELECT
            id AS staff_id,
            emp_id,
            first_name || ' ' || last_name AS full_name,
            tech_id,
            zone,
            team
        FROM staff_master
        WHERE is_active = true
          AND (
              (assoc_manager_tid = get_daily_summary.assoc_manager_tid AND get_daily_summary.assoc_manager_tid IS NOT NULL)
              OR (manager_tid = get_daily_summary.manager_tid AND get_daily_summary.manager_tid IS NOT NULL)
              OR (get_daily_summary.assoc_manager_tid IS NULL AND get_daily_summary.manager_tid IS NULL)
          )
    ),
    date_emp_grid AS (
        SELECT
            d.report_date,
            s.emp_id,
            s.staff_id,
            s.full_name,
            s.tech_id,
            s.zone,
            s.team
        FROM date_series d
        CROSS JOIN filtered_staff s
    )
    SELECT
        g.report_date,
        g.full_name,
        g.tech_id,
        g.zone,
        g.team,

        COUNT(CASE WHEN sr.open_date::date = g.report_date THEN 1 END) AS open_srs,
        COUNT(CASE WHEN sr.closed_date::date = g.report_date THEN 1 END) AS closed_srs,

        COUNT(CASE WHEN pa.created_at::date = g.report_date AND pa.record_type = 'Activity' THEN 1 END) AS activity_count,
        COUNT(CASE WHEN pa.created_at::date = g.report_date AND pa.record_type = 'Time Task' THEN 1 END) AS time_task_count

    FROM date_emp_grid g
    LEFT JOIN siebel_srs sr ON sr.siebel_id = g.emp_id
        AND (sr.open_date::date = g.report_date OR sr.closed_date::date = g.report_date)

    LEFT JOIN project_activities pa ON pa.staff_id = g.staff_id
        AND pa.created_at::date = g.report_date
        AND pa.record_type IN ('Activity', 'Time Task')

    GROUP BY g.report_date, g.full_name, g.tech_id, g.zone, g.team
    ORDER BY g.report_date, g.full_name;
END;
$$ LANGUAGE plpgsql;
