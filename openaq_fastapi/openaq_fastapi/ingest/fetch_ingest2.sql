-- Cleanup fields

UPDATE tempfetchdata_sensors t SET
geom = NULL WHERE st_x(geom) = 0 and st_y(geom) =0;

UPDATE tempfetchdata_sensors SET units  = 'µg/m³'
WHERE units IN ('µg/m��','��g/m³');

UPDATE tempfetchdata_sensors SET
node_metadata =
    jsonb_strip_nulls(
        COALESCE(data, '{}'::jsonb)
        ||
        jsonb_build_object(
            'source_type',
            'government',
            'origin',
            'openaq'
            )
    ),
sensor_metadata = jsonb_strip_nulls(jsonb_build_object(
    'data_averaging_period_seconds', avpd_value * 3600
    ))
;