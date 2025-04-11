CREATE OR REPLACE FUNCTION get_severity_summary()
RETURNS TABLE (
    tech_id TEXT,
    first_name TEXT,
    state TEXT,
    city TEXT,
    zone TEXT,
    team TEXT,
    closed_day DATE,
    bucket TEXT,
    count INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        sm.tech_id,
        sm.first_name,
        sm.state,
        sm.city,
        sm.zone,
        sm.team,
        s.closed_date::date AS closed_day,
        CASE 
            WHEN s.severity > 5 THEN '5+'
            ELSE s.severity::text
        END AS bucket,
        COUNT(*) AS count
    FROM siebel_srs s
    JOIN staff_master sm ON s.siebel_id = sm.emp_id
    WHERE s.closed_date::date BETWEEN date_trunc('month', CURRENT_DATE)::date AND CURRENT_DATE - 1
    GROUP BY 1,2,3,4,5,6,7,8,9
    ORDER BY sm.tech_id, closed_day, bucket;
END;
$$ LANGUAGE plpgsql;
