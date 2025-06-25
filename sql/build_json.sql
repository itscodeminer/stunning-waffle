SELECT 
    t.site,
    t.id,  -- assuming `id` is the unique identifier for the row
    json_agg(
        jsonb_build_object(
            'field_name', meta.field_name,
            'field_display_name', meta.field_display_name,
            'value', kv.value,
            'is_track_time', false
        )
    ) AS field_info
FROM data_table t
JOIN LATERAL jsonb_each_text(to_jsonb(t)) AS kv(key, value) ON true
JOIN metadata_table meta ON kv.key = meta.column_name
GROUP BY t.site, t.id;
