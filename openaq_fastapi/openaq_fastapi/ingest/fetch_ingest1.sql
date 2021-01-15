CREATE TEMP TABLE IF NOT EXISTS tempfetchdata_sensors AS
WITH t AS (
SELECT DISTINCT
    location as site_name,
    unit as units,
    parameter as measurand,
    country,
    city,
    jsonb_merge_agg(data) as data,
    source_name,
    coords::geometry as geom,
    source_type,
    mobile as ismobile,
    avpd_unit,
    avpd_value,
    coords::geometry as cgeom,
    NULL::int as sensor_nodes_id,
    null::int as sensor_systems_id,
    null::int as measurands_id,
    null::int as sensors_id,
    null::jsonb as node_metadata,
    null::jsonb as sensor_metadata,
    array_agg(tfdid) as tfdids
FROM tempfetchdata
GROUP BY
    location,
    unit,
    parameter,
    country,
    city,
    coords,
    source_type,
    source_name,
    mobile,
    avpd_unit,
    avpd_value,
    sensor_nodes_id,
    sensor_systems_id,
    measurands_id,
    sensors_id,
    node_metadata,
    sensor_metadata
)
SELECT row_number() over () as tfsid, * FROM t;
CREATE INDEX ON tempfetchdata_sensors (tfsid);