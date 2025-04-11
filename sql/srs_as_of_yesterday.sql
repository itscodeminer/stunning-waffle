CREATE OR REPLACE FUNCTION pivot_severity_summary_json()
RETURNS JSON AS $$
DECLARE
    start_date DATE := date_trunc('month', CURRENT_DATE)::date;
    end_date DATE := CURRENT_DATE - 1;
    d DATE;
    dyn_sql TEXT;
    result JSON;
    col_exprs TEXT := '';
BEGIN
    d := start_date;
    WHILE d <= end_date LOOP
        col_exprs := col_exprs || format(
            ', (
                SELECT string_agg(cnt || '' P'' || sev, '', '') 
                FROM (
                    SELECT 
                        CASE 
                            WHEN severity > 5 THEN ''5+'' 
                            ELSE severity::TEXT 
                        END AS sev,
                        COUNT(*) AS cnt
                    FROM siebel_srs s2
                    WHERE s2.siebel_id = s.siebel_id 
                      AND s2.closed_date::date = %L
                    GROUP BY CASE 
                                WHEN severity > 5 THEN ''5+'' 
                                ELSE severity::TEXT 
                             END
                    ORDER BY sev
                ) sub
            ) AS "%s"',
            d, d
        );
        d := d + 1;
    END LOOP;

    dyn_sql := format($fmt$
        SELECT 
            sm.tech_id,
            sm.first_name,
            sm.state,
            sm.city,
            sm.zone,
            sm.team,
            (
                SELECT COUNT(*) 
                FROM siebel_srs s2 
                WHERE s2.siebel_id = s.siebel_id 
                  AND s2.closed_date::date BETWEEN %L AND %L
            ) AS "Total SRs"
            %s
        FROM (
            SELECT DISTINCT siebel_id
            FROM siebel_srs
            WHERE closed_date::date BETWEEN %L AND %L
        ) s
        JOIN staff_master sm ON s.siebel_id = sm.emp_id
        ORDER BY sm.tech_id
    $fmt$, start_date, end_date, col_exprs, start_date, end_date);

    dyn_sql := format('SELECT json_agg(t) FROM (%s) t', dyn_sql);
    EXECUTE dyn_sql INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;
