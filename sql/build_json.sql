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

SELECT
  ped.tech_id,
  sm.time_zone,
  elem->>'value' AS original_value,
  to_char(
    (
      (elem->>'value')::timestamp
      AT TIME ZONE CASE sm.time_zone
        WHEN 'Eastern'  THEN 'America/New_York'
        WHEN 'Central'  THEN 'America/Chicago'
        WHEN 'Mountain' THEN 'America/Denver'
        WHEN 'Pacific'  THEN 'America/Los_Angeles'
        WHEN 'Hawaii'   THEN 'Pacific/Honolulu'
        ELSE 'UTC'
      END
    ) AT TIME ZONE 'UTC',  -- convert local to UTC
    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
  ) AS converted_value_utc_iso8601
FROM project_execution_data ped
JOIN staff_master sm ON ped.tech_id = sm.tech_id
CROSS JOIN LATERAL jsonb_array_elements(ped.project_data) AS elem
WHERE elem->>'field_name' = 'comm_rack_inspection_date';

  AND project_data @> '[{"field_name": "comm_rack_inspection_date"}]';

UPDATE project_execution_data ped
SET project_data = (
    SELECT jsonb_agg(
        CASE
            WHEN elem->>'field_name' = 'comm_rack_inspection_date'
                 AND elem->>'value' IS NOT NULL THEN
                jsonb_set(
                    elem,
                    '{value}',
                    to_jsonb(
                        to_char(
                            (
                                (elem->>'value')::timestamp
                                AT TIME ZONE CASE sm.time_zone
                                    WHEN 'Eastern'  THEN 'America/New_York'
                                    WHEN 'Central'  THEN 'America/Chicago'
                                    WHEN 'Mountain' THEN 'America/Denver'
                                    WHEN 'Pacific'  THEN 'America/Los_Angeles'
                                    WHEN 'Hawaii'   THEN 'Pacific/Honolulu'
                                    ELSE 'UTC'
                                END
                            ) AT TIME ZONE 'UTC',
                            'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                        )
                    )
                )
            ELSE
                elem
        END
    )
    FROM jsonb_array_elements(ped.project_data) AS elem
)
FROM staff_master sm
WHERE ped.staff_id = sm.id
  AND project_data @> '[{"field_name": "comm_rack_inspection_date"}]';
