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
JOIN project_metadata meta ON TRUE
GROUP BY t.site;


-- validate
WITH exploded_json AS (
  SELECT
    np.site,
    (jsonb_array_elements(np.field_info)) AS field_obj
  FROM new_project_data_json np
),
field_map AS (
  SELECT
    meta.field_name,
    meta.column_name
  FROM metadata_table meta
)
SELECT
  ej.site,
  fm.column_name,
  (project_data.*)::jsonb -> fm.column_name AS original_value,
  ej.field_obj->'value' AS json_value,
  CASE
    WHEN ((project_data.*)::jsonb -> fm.column_name) IS DISTINCT FROM (ej.field_obj->'value') THEN 'MISMATCH'
    ELSE 'MATCH'
  END AS validation_result
FROM exploded_json ej
JOIN field_map fm ON ej.field_obj->>'field_name' = fm.field_name
JOIN project_data ON project_data.site = ej.site
ORDER BY ej.site, fm.column_name;
