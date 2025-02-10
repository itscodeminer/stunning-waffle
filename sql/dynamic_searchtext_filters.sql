CREATE OR REPLACE FUNCTION search_all_emp(
    search_text TEXT DEFAULT NULL,  -- Optional search text
    pos_status_filter TEXT DEFAULT NULL,  -- Optional filter
    join_status_filter TEXT DEFAULT NULL,  -- Optional filter
    zone_filter TEXT DEFAULT NULL  -- Optional filter
) RETURNS TABLE (
    emp_id INT,
    emp_full_name TEXT,
    emp_manager_id INT,
    manager_name TEXT
) AS $$
DECLARE
    search_terms TEXT[];
    search_condition TEXT := '';
    filter_condition TEXT := 'TRUE';  -- Default to TRUE (no filter applied)
    sql_query TEXT;
    term TEXT;
    i INT := 1;
BEGIN
    -- 🔹 Handle search_text (if provided)
    IF search_text IS NOT NULL THEN
        search_terms := string_to_array(search_text, ' '); -- Split search text
        FOR i IN 1 .. array_length(search_terms, 1) LOOP
            term := search_terms[i];
            IF i > 1 THEN
                search_condition := search_condition || ' OR ';
            END IF
            search_condition := search_condition || format(
                '(nh.req_num ILIKE %L OR e.first_name ILIKE %L OR e.last_name ILIKE %L)',
                '%' || term || '%', '%' || term || '%', '%' || term || '%'
            );
        END LOOP;
    ELSE
        search_condition := 'TRUE';  -- No search filter
    END IF;

    -- 🔹 Apply filters only if values are provided
    IF pos_status_filter IS NOT NULL THEN
        filter_condition := filter_condition || format(' AND nh.req_status = %L', pos_status_filter);
    END IF;

    IF join_status_filter IS NOT NULL THEN
        filter_condition := filter_condition || format(' AND nh.hire_status = %L', join_status_filter);
    END IF;

    IF zone_filter IS NOT NULL THEN
        filter_condition := filter_condition || format(' AND nh.zone = %L', zone_filter);
    END IF;

    -- 🔹 Construct the SQL Query
    sql_query := format($SQL$
        WITH cte AS (
            SELECT nh.id AS emp_id, 
                   CONCAT(e.first_name, ' ', e.last_name) AS emp_full_name, 
                   nh.manager_id AS emp_manager_id
            FROM new_hires nh
            LEFT JOIN employees e ON e.id = nh.old_emp_id
            WHERE (%s) AND (%s)  -- Dynamic search + filters
        )
        SELECT cte.emp_id, cte.emp_full_name, cte.emp_manager_id, 
               CONCAT(m.first_name, ' ', m.last_name) AS manager_name
        FROM cte
        JOIN employees m ON m.id = cte.emp_manager_id;
    $SQL$, search_condition, filter_condition);

    -- 🔹 Execute dynamic SQL
    RETURN QUERY EXECUTE sql_query;
END;
$$ LANGUAGE plpgsql;
