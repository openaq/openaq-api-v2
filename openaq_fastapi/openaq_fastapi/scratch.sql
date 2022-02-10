\set offset 0
\set limit 20
\set rangestart '''2021-01-01'''
\set rangeend '''2022-01-01'''
\timing


WITH logs AS (
SELECT init_datetime
, CASE
  WHEN key~E'^realtime' THEN 'realtime'
  WHEN key~E'^lcs-etl-pipeline/measures' THEN 'pipeline'
  WHEN key~E'^lcs-etl-pipeline/station' THEN 'metadata'
  ELSE key
  END AS type
, fetchlogs_id
FROM fetchlogs
WHERE last_message~*'^error'
AND init_datetime > current_date - 100)
SELECT type
, init_datetime::date as day
, COUNT(1) as n
, AGE(now(), MAX(init_datetime)) as min_age
, AGE(now(), MIN(init_datetime)) as max_age
, MIN(fetchlogs_id) as fetchlogs_id
FROM logs
GROUP BY init_datetime::date, type
ORDER BY init_datetime::date
--LIMIT 10
;

WITH logs AS (
SELECT init_datetime
, CASE
  WHEN key~E'^realtime' THEN 'realtime'
  WHEN key~E'^lcs-etl-pipeline/measures' THEN 'pipeline'
  WHEN key~E'^lcs-etl-pipeline/station' THEN 'metadata'
  ELSE key
  END AS type
, fetchlogs_id
FROM fetchlogs
WHERE completed_datetime is null
--AND key ~* 'clarity'
AND init_datetime > current_date - 100)
SELECT type
, init_datetime::date as day
, COUNT(1) as n
, AGE(now(), MAX(init_datetime)) as min_age
, AGE(now(), MIN(init_datetime)) as max_age
, MIN(fetchlogs_id) as fetchlogs_id
FROM logs
GROUP BY type, init_datetime::date
ORDER BY type, init_datetime::date
--LIMIT 10
;

SELECT tbl
, COUNT(1) as n
, MIN(t) as first_datetime
, MAX(t) as last_datetime
FROM rejects
GROUP BY tbl;

SELECT r->>'ingest_id' as ingest_id
, COUNT(1) as n
, MIN(t) as first_datetime
, MAX(t) as last_datetime
, MIN(fetchlogs_id) as id
FROM rejects
WHERE tbl ~* 'meas'
GROUP BY r->>'ingest_id';

SELECT tbl
, r->>'ingest_id'
, COUNT(1) as n
, MIN(t) as first_datetime
, MAX(t) as last_datetime
FROM rejects
GROUP BY tbl, r->>'ingest_id'
LIMIT 200;

DELETE
FROM




CREATE OR REPLACE FUNCTION stale_version_sensors() RETURNS SETOF int AS $$
WITH sensors AS (
SELECT v.parent_sensors_id
, v.sensors_id
, v.version_date
, lc.sort_order
, row_number() OVER (
  PARTITION BY parent_sensors_id
  ORDER BY v.version_date DESC, lc.sort_order DESC
) as version_rank
FROM versions v
JOIN life_cycles lc USING (life_cycles_id))
SELECT sensors_id
FROM sensors
WHERE version_rank > 1;
$$ LANGUAGE SQL;

-- returns only sensors with versions
CREATE OR REPLACE FUNCTION latest_version_sensors() RETURNS SETOF int AS $$
WITH sensors AS (
SELECT v.parent_sensors_id
, v.sensors_id
, v.version_date
, lc.sort_order
, row_number() OVER (
  PARTITION BY parent_sensors_id
  ORDER BY v.version_date DESC, lc.sort_order DESC
) as version_rank
FROM versions v
JOIN life_cycles lc USING (life_cycles_id))
SELECT sensors_id
FROM sensors
WHERE version_rank = 1;
$$ LANGUAGE SQL;


--\i ../../../openaq-db/openaqdb/idempotent/views.sql


DROP FUNCTION source_sensors(text);
CREATE OR REPLACE FUNCTION source_sensors(src text)
RETURNS TABLE(
sensors_id int
, source_id text
, measurand text
, starts timestamptz
, ends timestamptz
, lowest numeric
, highest numeric
, total int
) AS $$
SELECT s.sensors_id
, s.source_id
, p.measurand
, MIN(datetime) as starts
, MAX(datetime) as ends
, MIN(value) as lowest
, MAX(value) as highest
, COUNT(1) as total
FROM measurements m
JOIN sensors s ON (s.sensors_id = m.sensors_id)
JOIN measurands p ON (s.measurands_id = p.measurands_id)
WHERE s.source_id ~* ('^'||src)
GROUP BY s.source_id, s.sensors_id, p.measurands_id
ORDER BY MAX(datetime) DESC
LIMIT 20;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION source_sensors_id(src text)
RETURNS SETOF int AS $$
SELECT s.sensors_id
FROM measurements m
JOIN sensors s ON (s.sensors_id = m.sensors_id)
WHERE s.source_id ~* ('^'||src)
GROUP BY s.sensors_id
LIMIT 20;
$$ LANGUAGE SQL;

\echo Looking at the sensors
SELECT sensors_id
, source_id
, measurands_id
FROM sensors
WHERE sensors_id IN (SELECT source_sensors_id('versioning'))
ORDER BY measurands_id;

\echo Total count of versioning measurements
SELECT COUNT(DISTINCT sensors_id) as sensors
, COUNT(1) as records
FROM measurements
WHERE sensors_id IN (SELECT source_sensors_id('versioning'));

\echo Check of measurements_fastapi_base
SELECT COUNT(DISTINCT sensors_id)
, COUNT(1) records
, COUNT(DISTINCT measurand) as measurands
FROM measurements_fastapi_base
WHERE sensors_id IN (SELECT source_sensors_id('versioning'));

\echo check of sensor_stats table
SELECT COUNT(DISTINCT sensors_id)
, sum(value_count)
, min(first_datetime)
, max(last_datetime)
FROM sensor_stats
WHERE sensors_id IN (SELECT source_sensors_id('versioning'));

\echo check of the rollups table
SELECT groups_id
, rollup
, COUNT(DISTINCT sensors_id) as sensors
, COUNT(1) as records
, SUM(value_sum) as value_sum
, SUM(value_count) as value_count
FROM rollups
WHERE sensors_id IN (SELECT source_sensors_id('versioning'))
GROUP BY groups_id
, rollup;

\echo count of sensors meeting the search criteria
SELECT COUNT(DISTINCT sensors_id)
, sum(value_count)
, min(first_datetime)
, max(last_datetime)
FROM sensor_stats
LEFT JOIN measurements_fastapi_base b USING (sensors_id, sensor_nodes_id)
--LEFT JOIN groups_sensors USING (sensors_id)
--LEFT JOIN groups_view b USING (groups_id, measurands_id)
WHERE sensors_id IN (SELECT source_sensors_id('versioning'));



\echo Measurements for those sensors
WITH data AS (
SELECT
  sensor_nodes_id as location_id
  , b.sensors_id
  , site_name as location
  , measurand as parameter
  , value
  , datetime
  -- , timezone
  -- , CASE WHEN lon is not null and lat is not null THEN
  --   json_build_object(
  --     'latitude',lat,
  --     'longitude', lon
  --   )
  --   WHEN b.geog is not null THEN
  --   json_build_object(
  --     'latitude', st_y(geog::geometry),
  --     'longitude', st_x(geog::geometry)
  --   )
  --   ELSE NULL END AS coordinates
  -- , units as unit
  -- , country
  -- , city
  , is_versioned
  , parent_sensors_id
  , is_latest
  , version_date
  , life_cycles_id
  , life_cycles_label
  , entity
  , "sensorType"
FROM measurements_analyses a
LEFT JOIN measurements_fastapi_base b USING (sensors_id)
WHERE sensors_id IN (SELECT source_sensors_id('versioning'))
AND is_latest
ORDER BY sensors_id, "datetime" desc)
SELECT *
FROM data
LIMIT 5;

-- WITH t AS (
--                 SELECT
--                     sensor_nodes_id as location_id,
--                     site_name as location,
--                     measurand as parameter,
--                     value,
--                     datetime,
--                     timezone,
--                     CASE WHEN lon is not null and lat is not null THEN
--                         json_build_object(
--                             'latitude',lat,
--                             'longitude', lon
--                             )
--                         WHEN b.geog is not null THEN
--                         json_build_object(
--                                 'latitude', st_y(geog::geometry),
--                                 'longitude', st_x(geog::geometry)
--                             )
--                         ELSE NULL END AS coordinates,
--                     units as unit,
--                     country,
--                     city,
--                     ismobile,
--                     is_analysis,
--                     entity, "sensorType"
--                 FROM measurements_analyses a
--                 LEFT JOIN measurements_fastapi_base b USING (sensors_id)
--                 WHERE  sensor_nodes_id not in (61485,61505,61506)
--                 AND datetime >= :rangestart::timestamptz
--                 AND datetime <= :rangeend::timestamptz
--                 ORDER BY "datetime" desc
--                 OFFSET :offset
--                 LIMIT :limit
--                 ), t1 AS (
--                     SELECT
--                         location_id as "locationId",
--                         location,
--                         parameter,
--                         value,
--                         json_build_object(
--                             'utc',
--                             format_timestamp(datetime, 'UTC'),
--                             'local',
--                             format_timestamp(datetime, timezone)
--                         ) as date,
--                         unit,
--                         coordinates,
--                         country,
--                         city,
--                         ismobile as "isMobile",
--                         is_analysis as "isAnalysis",
--                         entity, "sensorType"
--                     FROM t
--                 )
--                 SELECT 164957::bigint as count,
--                 row_to_json(t1) as json FROM t1;


-- SELECT v.parent_sensors_id
-- , v.sensors_id
-- , v.version_date
-- , lc.sort_order
-- FROM versions v
-- JOIN life_cycles lc USING (life_cycles_id);



\echo Checking versions functions
SELECT stale_version_sensors();
SELECT latest_version_sensors();

\echo Measurements table
SELECT *
FROM source_sensors('versioning')
ORDER BY measurand;

\echo Measurements fastapi base table
SELECT sensors_id
, site_name
, measurand
, attribution
, city
, is_versioned
, is_latest
FROM measurements_fastapi_base
WHERE sensors_id IN (SELECT source_sensors_id('versioning'));

\echo Rollup table
SELECT s.sensors_id
, s.source_id
, s.measurand
, COUNT(1)
, array_agg(DISTINCT rollup)
, MIN(s.total) as min_total
FROM rollups r
, source_sensors('version') s
WHERE r.sensors_id = s.sensors_id
GROUP BY s.sensors_id, s.source_id, s.measurand
ORDER BY s.measurand;

\echo Stats table
SELECT sensors_id
, city
--, is_versioned
--, is_latest
FROM sensor_stats
WHERE sensors_id IN (SELECT source_sensors_id('versioning'))
ORDER BY sensors_id;


-- SELECT s.sensors_id
-- , s.source_id
-- , p.measurand
-- , MIN(datetime) as starts
-- , MAX(datetime) as ends
-- , MIN(value) as lowest
-- , MAX(value) as highest
-- , COUNT(1) as value_count
-- FROM measurements m
-- JOIN sensors s ON (s.sensors_id = m.sensors_id)
-- JOIN measurands p ON (s.measurands_id = p.measurands_id)
-- WHERE s.sensors_id IN (87,477,273)
-- GROUP BY s.source_id, s.sensors_id, p.measurands_id
-- ORDER BY MAX(datetime) DESC;

-- SELECT sensors_id
-- , rollup
-- , value_count
-- , value_sum
-- FROM rollups
-- WHERE sensors_id IN (87,477,273);


\echo Exploring the first part of the measurements table

  SELECT
        sensors_id
        --, min(datetime) as first_datetime
        --, max(datetime) as last_datetime
        , count(*) as value_count
        , sum(value) as value_sum
        , last(value, datetime) as last_value
    FROM measurements
    JOIN groups_sensors USING (sensors_id)
    JOIN sensors USING (sensors_id)
    WHERE sensors_id IN (SELECT source_sensors_id('version'))
    -- WHERE sensors_id IN (87,477,273)
    GROUP BY 1
    ORDER BY sensors_id;

\echo Looking at the combination
WITH tmp AS (
     SELECT
        sensors_id
        , min(datetime) as first_datetime
        , max(datetime) as last_datetime
        , count(*) as value_count
        , sum(value) as value_sum
        , last(value, datetime) as last_value
    FROM measurements
    JOIN groups_sensors USING (sensors_id)
    JOIN sensors USING (sensors_id)
    WHERE sensors_id IN (SELECT source_sensors_id('version'))
    -- WHERE sensors_id IN (87,477,273)
    GROUP BY 1
    ORDER BY sensors_id)
    SELECT
        groups_id
        , measurands_id
        --, sensors_id
        , COUNT(1) as sensors_count
        , last(sensors_id, last_datetime) as sensors_id
        --, last(sensors_id) as sensors_id
        --, min(first_datetime) as first_datetime
        --, max(last_datetime) as last_datetime
        , sum(value_count) as value_count
        , sum(value_sum) as value_sum
        , last(last_value, last_datetime) as last_value
    FROM tmp
    JOIN groups_sensors USING (sensors_id)
    JOIN sensors USING (sensors_id)
    GROUP BY 1,2
    ;




-- WITH tmp AS (
--      SELECT
--         sensors_id,
--         min(datetime) as first_datetime,
--         max(datetime) as last_datetime,
--         count(*) as value_count,
--         sum(value) as value_sum,
--         last(value, datetime) as last_value
--     FROM measurements
--     --JOIN groups_sensors USING (sensors_id)
--     JOIN sensors USING (sensors_id)
--     WHERE sensors_id IN (SELECT source_sensors_id('version'))
--     GROUP BY 1
--     ORDER BY sensors_id)
--     SELECT
--         groups_id,
--         measurands_id,
--         --sensors_id,
--         last(sensors_id, last_datetime) as sensors_id,
--         min(first_datetime) as first_datetime,
--         max(last_datetime) as last_datetime,
--         sum(value_count) as value_count,
--         sum(value_sum) as value_sum,
--         last(last_value, last_datetime) as last_value
--     FROM tmp
--     JOIN groups_sensors USING (sensors_id)
--     JOIN sensors USING (sensors_id)
--     GROUP BY 1,2;

-- \echo Removing the
-- WITH tmp AS (
--      SELECT
--         sensors_id,
--         min(datetime) as first_datetime,
--         max(datetime) as last_datetime,
--         count(*) as value_count,
--         sum(value) as value_sum,
--         last(value, datetime) as last_value
--     FROM measurements
--     JOIN groups_sensors USING (sensors_id)
--     JOIN sensors USING (sensors_id)
--     WHERE sensors_id IN (SELECT source_sensors_id('version'))
--     GROUP BY 1
--     ORDER BY sensors_id)
--     SELECT
--         groups_id,
--         measurands_id,
--         sensors_id,
--         --last(sensors_id, last_datetime) as sensors_id,
--         min(first_datetime) as first_datetime,
--         max(last_datetime) as last_datetime,
--         sum(value_count) as value_count,
--         sum(value_sum) as value_sum,
--         last(last_value, last_datetime) as last_value
--     FROM tmp
--     JOIN groups_sensors USING (sensors_id)
--     JOIN sensors USING (sensors_id)
--     GROUP BY 1,2,3;


-- \echo List of sensors and groups for versions
-- SELECT sensors_id
-- , COUNT(1) as n_groups
-- , array_agg(subtitle) as groups
-- FROM groups_sensors
-- JOIN groups USING (groups_id)
-- WHERE sensors_id IN (SELECT source_sensors_id('version'))
-- GROUP BY sensors_id
-- HAVING COUNT(1) > 1
-- ORDER BY COUNT(1) DESC
-- LIMIT 10;

-- \echo Group details for versions
-- SELECT *
-- FROM groups
-- WHERE groups_id IN (
--       SELECT groups_id
--       FROM groups_sensors
--       WHERE sensors_id IN (
--             SELECT source_sensors_id('version')
--             )
--       );

-- DO $$
-- DECLARE
-- config jsonb;
-- BEGIN

-- SELECT jsonb_build_object(
-- 'start', MIN(datetime)
-- , 'end', MAX(datetime)
-- , 'total', COUNT(1)
-- ) INTO config
-- FROM measurements m
-- JOIN sensors s ON (s.sensors_id = m.sensors_id)
-- WHERE s.source_id ~* '^versioning';

-- CALL run_updates(null, config);

-- END $$

SELECT COUNT(1)
FROM sensors
WHERE sensors_id NOT IN (SELECT sensors_id FROM measurements);

SELECT COUNT(1)
FROM sensors
WHERE sensors_id NOT IN (SELECT sensors_id FROM rollups WHERE rollup = 'day');


-- locations
-- SELECT *
-- FROM locations_base_v2
-- WHERE name ~* 'jakarta';

SELECT COUNT(1)
FROM sensor_nodes
JOIN sensor_systems USING (sensor_nodes_id)
JOIN sensors USING (sensor_systems_id);

WITH base AS (
            SELECT
                measurands_id,
                date_trunc('hour', datetime) as hour,
                date_trunc('hour', datetime) as o,
                date_trunc('hour', datetime) as st,
                sensors_id as id,
                count(*) as measurement_count,
                round((sum(value)/count(*))::numeric, 4) as average
            FROM measurements
            JOIN sensor_stats_versioning USING (sensors_id)
            --LEFT JOIN sensors USING (sensors_id)
            --LEFT JOIN groups_sensors USING (sensors_id)
            --LEFT JOIN groups_view USING (groups_id, measurands_id)
            WHERE  TRUE
            --AND type = :spatial::text
            AND datetime>=:date_from::timestamptz
            AND datetime<=:date_to::timestamptz
            GROUP BY 1,2,3,4,5
            ORDER BY 4 desc
            OFFSET :offset
            LIMIT :limit
        )
        SELECT :count::bigint as count,
        (to_jsonb(base) ||
        parameter(measurands_id)) -
        '{o,st, measurands_id}'::text[]
        FROM base


WITH t AS (
    SELECT
        measurands_id as id,
        measurand as name,
        display as "displayName",
        coalesce(description, display) as description,
        units as "preferredUnit",
        is_core as "isCore",
        max_color_value as "maxColorValue"
    FROM measurands
    WHERE display is not null and is_core is not null
    ORDER BY "id" asc
    )
    SELECT count(*) OVER () as count,
    jsonb_strip_nulls(to_jsonb(t)) as json FROM t
    LIMIT 100
    OFFSET 0;
