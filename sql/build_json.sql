SELECT json_agg(
    jsonb_build_object(
        'field_name', meta.field_name,
        'field_display_name', meta.field_display_name,
        'value', row_data.value,
        'is_track_time', false
    )
) AS field_info
FROM (
    SELECT *
    FROM data_table
    WHERE id = 1  -- or any specific row
) t,
LATERAL jsonb_each_text(to_jsonb(t)) AS row_data(key, value)
JOIN metadata_table meta
  ON row_data.key = meta.column_name;
