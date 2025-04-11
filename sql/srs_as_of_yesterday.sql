CREATE OR REPLACE FUNCTION pivot_srs_by_date_json()
RETURNS JSON AS $$
DECLARE
    start_date DATE := date_trunc('month', CURRENT_DATE)::date;
    end_date DATE := CURRENT_DATE - 1;
    col_list TEXT := '';
    d DATE;
    dyn_sql TEXT;
    result JSON;
BEGIN
    -- Build one column per day up to yesterday
    d := start_date;
    WHILE d <= end_date LOOP
        col_list := col_list || format(
            ', COUNT(CASE WHEN closed_date::date = %L THEN 1 END) AS "%s"',
            d, d
        );
        d := d + 1;
    END LOOP;

    -- Build the dynamic SQL
    dyn_sql := format(
        'SELECT siebel_id%s FROM siebel_srs WHERE closed_date::date BETWEEN %L AND %L GROUP BY siebel_id ORDER BY siebel_id',
        col_list, start_date, end_date
    );

    -- Wrap it in JSON output
    dyn_sql := format('SELECT json_agg(t) FROM (%s) t', dyn_sql);

    -- Execute and get the result
    EXECUTE dyn_sql INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
