SELECT 
    t.site,
    json_agg(
        jsonb_build_object(
            'field_name', meta.field_name,
            'field_display_name', meta.field_display_name,
            'value', (t.*)::jsonb -> meta.column_name,
            'is_track_time', false
        )
        ORDER BY meta.column_name
    ) AS field_info
INTO new_project_data_json
FROM project_data t
JOIN metadata_table meta ON TRUE
GROUP BY t.site;
