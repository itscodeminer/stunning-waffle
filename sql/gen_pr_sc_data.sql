CREATE OR REPLACE PROCEDURE generate_project_execution_data(p_project_id INT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_project RECORD;
    v_next_cycle_index INT;
    v_next_start_date DATE;
    v_next_end_date DATE;
    v_scope_values TEXT[];
BEGIN
    -- Fetch project definition
    SELECT * INTO v_project
    FROM project_definition
    WHERE id = p_project_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Project ID % not found', p_project_id;
    END IF;

    -- Determine next cycle details
    IF v_project.recurrence = 'one-time' THEN
        v_next_cycle_index := 1;
        v_next_start_date := v_project.start_date;
        v_next_end_date := v_project.end_date;

        -- If already generated, skip
        IF EXISTS (
            SELECT 1 FROM project_execution_data
            WHERE project_id = p_project_id AND cycle_index = 1
        ) THEN
            RETURN;
        END IF;
    ELSE
        v_next_cycle_index := COALESCE(v_project.cycle_index, 0) + 1;
        v_next_start_date := COALESCE(v_project.cycle_end_date, v_project.start_date) + INTERVAL '1 day';

        CASE v_project.recurrence
            WHEN 'monthly' THEN
                v_next_end_date := v_next_start_date + INTERVAL '1 month' - INTERVAL '1 day';
            WHEN 'quarterly' THEN
                v_next_end_date := v_next_start_date + INTERVAL '3 months' - INTERVAL '1 day';
            WHEN 'half-yearly' THEN
                v_next_end_date := v_next_start_date + INTERVAL '6 months' - INTERVAL '1 day';
            WHEN 'yearly' THEN
                v_next_end_date := v_next_start_date + INTERVAL '1 year' - INTERVAL '1 day';
            ELSE
                RAISE EXCEPTION 'Invalid recurrence type: %', v_project.recurrence;
        END CASE;

        -- Skip if data already exists
        IF EXISTS (
            SELECT 1 FROM project_execution_data
            WHERE project_id = p_project_id AND cycle_index = v_next_cycle_index
        ) THEN
            RETURN;
        END IF;
    END IF;

    -- Clean up pending records (if re-triggered)
    DELETE FROM project_execution_data
    WHERE project_id = p_project_id AND cycle_index = v_next_cycle_index AND execution_status = 'pending';

    -- Resolve scope and generate skeleton records
    IF v_project.scope = 'Global' THEN
        INSERT INTO project_execution_data (project_id, site, cycle_index, cycle_start_date, cycle_end_date, is_on_project, execution_status)
        SELECT p_project_id, site, v_next_cycle_index, v_next_start_date, v_next_end_date, TRUE, 'pending'
        FROM site_master;

    ELSIF v_project.scope = 'Zone' THEN
        v_scope_values := string_to_array(v_project.scope_data, ',');
        INSERT INTO project_execution_data
        SELECT p_project_id, site, v_next_cycle_index, v_next_start_date, v_next_end_date, TRUE, 'pending'
        FROM site_master
        WHERE zone = ANY(v_scope_values);

    ELSIF v_project.scope = 'State' THEN
        v_scope_values := string_to_array(v_project.scope_data, ',');
        INSERT INTO project_execution_data
        SELECT p_project_id, site, v_next_cycle_index, v_next_start_date, v_next_end_date, TRUE, 'pending'
        FROM site_master
        WHERE state = ANY(v_scope_values);

    ELSIF v_project.scope = 'Store Type' THEN
        v_scope_values := string_to_array(v_project.scope_data, ',');
        INSERT INTO project_execution_data
        SELECT p_project_id, site, v_next_cycle_index, v_next_start_date, v_next_end_date, TRUE, 'pending'
        FROM site_master
        WHERE store_type = ANY(v_scope_values);

    ELSIF v_project.scope = 'Specified Sites' THEN
        v_scope_values := string_to_array(v_project.scope_data, ',');
        INSERT INTO project_execution_data
        SELECT p_project_id, unnest(v_scope_values), v_next_cycle_index, v_next_start_date, v_next_end_date, TRUE, 'pending';

    ELSE
        RAISE EXCEPTION 'Invalid scope: %', v_project.scope;
    END IF;

    -- Update project_definition with latest cycle info
    UPDATE project_definition
    SET cycle_index = v_next_cycle_index,
        cycle_start_date = v_next_start_date,
        cycle_end_date = v_next_end_date
    WHERE id = p_project_id;
END;
$$;
