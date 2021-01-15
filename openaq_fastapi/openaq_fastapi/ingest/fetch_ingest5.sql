-- Update any records that have changed

UPDATE sensor_nodes s SET
    site_name = COALESCE(t.site_name, s.site_name),
    source_name = COALESCE(t.source_name, s.source_name),
    city = COALESCE(t.city, s.city),
    country = COALESCE(t.country, s.country),
    ismobile = COALESCE(t.ismobile, s.ismobile),
    metadata = COALESCE(s.metadata, '{}'::jsonb) || t.metadata,
    geom = COALESCE(t.geom, s.geom)
FROM tempfetchdata_nodes t
WHERE t.sensor_nodes_id = s.sensor_nodes_id AND
(
    (s.geom IS NULL and t.geom IS NOT NULL)
OR

    ROW(
        t.sensor_nodes_id,
        t.ismobile,
        t.site_name,
        t.source_name,
        t.city,
        t.country,
        t.metadata
    ) IS DISTINCT FROM (
        s.sensor_nodes_id,
        s.ismobile,
        s.site_name,
        s.source_name,
        s.city,
        s.country,
        s.metadata
    )
)
;
