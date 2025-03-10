CREATE OR REPLACE FUNCTION GetSeverityCounts(
    p_start_date DATE,
    p_end_date DATE,
    p_assoc_mgr_id INT,
    p_manager_id INT,
    p_aggregation_level TEXT
)
RETURNS TABLE (
    aggregation_date DATE,
    open_sev1 INT, open_sev2 INT, open_sev3 INT, open_sev4 INT, open_sev5 INT, open_sev6 INT, open_sev7 INT, open_sev8 INT, open_sev9 INT, open_sev10 INT, open_sev11 INT, open_sev12 INT, open_sev13 INT, open_sev14 INT, open_sev15 INT,
    close_sev1 INT, close_sev2 INT, close_sev3 INT, close_sev4 INT, close_sev5 INT, close_sev6 INT, close_sev7 INT, close_sev8 INT, close_sev9 INT, close_sev10 INT, close_sev11 INT, close_sev12 INT, close_sev13 INT, close_sev14 INT, close_sev15 INT
) AS
$$
BEGIN
    RETURN QUERY
    SELECT 
        -- Aggregating by open_date or close_date depending on the level
        CASE 
            WHEN p_aggregation_level = 'daily' THEN DATE_TRUNC('day', COALESCE(open_date, close_date))
            WHEN p_aggregation_level = 'weekly' THEN DATE_TRUNC('week', COALESCE(open_date, close_date))
            WHEN p_aggregation_level = 'monthly' THEN DATE_TRUNC('month', COALESCE(open_date, close_date))
            ELSE DATE_TRUNC('day', COALESCE(open_date, close_date))  -- default to daily if invalid aggregation level
        END AS aggregation_date,

        -- Open severity counts (1-15)
        COUNT(CASE WHEN severity = 1 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev1,
        COUNT(CASE WHEN severity = 2 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev2,
        COUNT(CASE WHEN severity = 3 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev3,
        COUNT(CASE WHEN severity = 4 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev4,
        COUNT(CASE WHEN severity = 5 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev5,
        COUNT(CASE WHEN severity = 6 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev6,
        COUNT(CASE WHEN severity = 7 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev7,
        COUNT(CASE WHEN severity = 8 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev8,
        COUNT(CASE WHEN severity = 9 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev9,
        COUNT(CASE WHEN severity = 10 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev10,
        COUNT(CASE WHEN severity = 11 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev11,
        COUNT(CASE WHEN severity = 12 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev12,
        COUNT(CASE WHEN severity = 13 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev13,
        COUNT(CASE WHEN severity = 14 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev14,
        COUNT(CASE WHEN severity = 15 AND open_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS open_sev15,

        -- Close severity counts (1-15)
        COUNT(CASE WHEN severity = 1 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev1,
        COUNT(CASE WHEN severity = 2 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev2,
        COUNT(CASE WHEN severity = 3 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev3,
        COUNT(CASE WHEN severity = 4 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev4,
        COUNT(CASE WHEN severity = 5 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev5,
        COUNT(CASE WHEN severity = 6 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev6,
        COUNT(CASE WHEN severity = 7 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev7,
        COUNT(CASE WHEN severity = 8 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev8,
        COUNT(CASE WHEN severity = 9 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev9,
        COUNT(CASE WHEN severity = 10 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev10,
        COUNT(CASE WHEN severity = 11 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev11,
        COUNT(CASE WHEN severity = 12 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev12,
        COUNT(CASE WHEN severity = 13 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev13,
        COUNT(CASE WHEN severity = 14 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev14,
        COUNT(CASE WHEN severity = 15 AND close_date BETWEEN p_start_date AND p_end_date THEN 1 END) AS close_sev15
    FROM 
        siebel_srs
    WHERE 
        -- Filters on open_date and close_date
        (open_date BETWEEN p_start_date AND p_end_date OR close_date BETWEEN p_start_date AND p_end_date)
        AND (p_assoc_mgr_id IS NULL OR assoc_manager_id = p_assoc_mgr_id)
        AND (p_manager_id IS NULL OR manager_id = p_manager_id)
    GROUP BY 
        aggregation_date
    ORDER BY 
        aggregation_date;
END;
$$
