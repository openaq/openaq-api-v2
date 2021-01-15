/*
CREATE TEMP TABLE tempfetchdata_nodes AS
SELECT * FROM (SELECT
    first_notnull(site_name) as site_name,
    first_notnull(source_name) as source_name,
    first_notnull(country) as country,
    first_notnull(city) as city,
    --jsonb_merge_agg(node_metadata) as metadata,
    first_notnull(ismobile) as ismobile,
    null::int as sensor_nodes_id,
    null::int as sensor_systems_id,
    st_centroid(st_collect(geom)) as geom,
    array_agg(tfsid) as tfsids
FROM tempfetchdata_sensors
WHERE geom IS NOT NULL
GROUP BY
    sensor_nodes_id,st_snaptogrid(geom, .0001)
) AS wgeom
UNION ALL
SELECT * FROM
(SELECT
    site_name,
    source_name,
    first_notnull(country) as country,
    first_notnull(city) as city,
    --jsonb_merge_agg(node_metadata) as metadata,
    first_notnull(ismobile) as ismobile,
    null::int as sensor_nodes_id,
    null::int as sensor_systems_id,
    null::geometry as geom,
    array_agg(tfsid) as tfsids
FROM tempfetchdata_sensors
WHERE geom IS NULL
AND site_name is not null
and source_name is not null
GROUP BY
    site_name, source_name, sensor_nodes_id
) as nogeom
;
*/

CREATE TEMP TABLE tempfetchdata_nodes AS
SELECT * FROM (SELECT
    site_name,
    source_name,
    country,
    city,
    node_metadata as metadata,
    ismobile,
    null::int as sensor_nodes_id,
    null::int as sensor_systems_id,
    st_centroid(st_collect(geom)) as geom,
    array_agg(tfsid) as tfsids
FROM tempfetchdata_sensors
WHERE geom IS NOT NULL
GROUP BY
    1,2,3,4,5,6,7,8,st_snaptogrid(geom, .0001)
) AS wgeom
UNION ALL
SELECT * FROM
(SELECT
    site_name,
    source_name,
    country,
    city,
    node_metadata as metadata,
    ismobile,
    null::int as sensor_nodes_id,
    null::int as sensor_systems_id,
    null::geometry as geom,
    array_agg(tfsid) as tfsids
FROM tempfetchdata_sensors
WHERE geom IS NULL
AND site_name is not null
and source_name is not null
GROUP BY
    1,2,3,4,5,6,7,8,9
) as nogeom
;
