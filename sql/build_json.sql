SELECT 
    t.site,
    json_agg(
        jsonb_build_object(
            'field_name', meta.field_name,
            'field_display_name', meta.field_display_name,
            'value', row_data.value,
            'is_track_time', false
        )
    ) AS field_info
FROM data_table t
LATERAL jsonb_each_text(to_jsonb(t)) AS row_data(key, value)
JOIN metadata_table meta
  ON row_data.key = meta.column_name
GROUP BY t.site, t.id;  -- include the primary key to separate each row
