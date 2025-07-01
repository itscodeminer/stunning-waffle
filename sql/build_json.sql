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
-- Flatten json and compare values
WITH json_expanded AS (
  SELECT
    t.site,
    t.unique_id,
    meta.column_name,
    to_jsonb(t) -> meta.column_name AS actual_value,
    jsonb_build_object(
      'field_name', meta.field_name,
      'field_display_name', meta.field_display_name,
      'value', to_jsonb(t) -> meta.column_name,
      'is_track_time', false
    ) AS json_field
  FROM project_data t
  JOIN project_metadata meta ON TRUE
)
SELECT *
FROM json_expanded
WHERE actual_value IS DISTINCT FROM json_field ->> 'value';
