-- Get sensor systems
DO $$
DECLARE
__process_start timestamptz := clock_timestamp();
__total_measurements int;
__updated_nodes int;
__inserted_nodes int;
__inserted_sensors int;
__inserted_measurements int;
__inserted_measurands int;
__rejected_nodes int;
__rejected_systems int;
__rejected_sensors int;
__rejected_measurements int;
__start_datetime timestamptz;
__end_datetime timestamptz;
__inserted_start_datetime timestamptz;
__inserted_end_datetime timestamptz;
__deleted_timescaledb int;
__deleted_future_measurements int;
__deleted_past_measurements int;
BEGIN

SELECT now() INTO __process_start;

---------------------------
-- File fetch_filter.sql --
---------------------------

-- Note: I am including this because it already existed
-- I am not sure why its here

WITH deletes AS (
  DELETE
  FROM tempfetchdata
  WHERE datetime <= (
    SELECT max(range_end)
    FROM timescaledb_information.chunks
    WHERE hypertable_name IN ('rollups', 'measurements')
    AND is_compressed
    )
  RETURNING 1)
SELECT COUNT(1) INTO __deleted_timescaledb
FROM deletes;

-- This makes sense though we should track in case its systemic
WITH deletes AS (
  DELETE
  FROM tempfetchdata
  WHERE datetime > now()
  RETURNING 1)
SELECT COUNT(1) INTO __deleted_future_measurements
FROM deletes;

-- this seems questionable, I dont want to pass data to this
-- process only to have some of it filtered out because its too old
WITH deletes AS (
  DELETE
  FROM tempfetchdata
  WHERE datetime < (SELECT max(datetime) - '2 days'::interval from tempfetchdata)
  RETURNING 1)
SELECT COUNT(1) INTO __deleted_past_measurements
FROM deletes;

----------------------------------

-- start with simple count
SELECT COUNT(1)
, MIN(datetime)
, MAX(datetime)
INTO __total_measurements
, __start_datetime
, __end_datetime
FROM tempfetchdata;

-- Now we start the old fetch_ingest#.sql files
-------------
-- File #1 --
-------------
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
SELECT row_number() over () as tfsid, *
FROM t;
CREATE INDEX ON tempfetchdata_sensors (tfsid);
-------------
-- File #2 --
-------------

-- Cleanup fields

UPDATE tempfetchdata_sensors t
SET geom = NULL
WHERE st_x(geom) = 0
AND st_y(geom) = 0;

UPDATE tempfetchdata_sensors
SET units  = 'µg/m³'
WHERE units IN ('µg/m��','��g/m³');

UPDATE tempfetchdata_sensors
SET node_metadata =
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

-------------
-- File #3 --
-------------

CREATE TEMP TABLE IF NOT EXISTS tempfetchdata_nodes AS
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

-------------
-- File #4 --
-------------

-- Lookup Node Ids

UPDATE tempfetchdata_nodes t
SET sensor_nodes_id = sn.sensor_nodes_id
FROM sensor_nodes sn
WHERE t.geom IS NOT NULL
AND st_dwithin(sn.geom, t.geom, .0001)
AND origin='OPENAQ';

UPDATE tempfetchdata_nodes t
SET sensor_nodes_id = sn.sensor_nodes_id
FROM sensor_nodes sn
WHERE t.sensor_nodes_id is null
AND t.site_name is not null
AND t.source_name is not null
AND t.site_name = sn.site_name
AND t.source_name=sn.source_name
AND origin='OPENAQ';

-------------
-- File #5 --
-------------

-- Update any records that have changed
WITH updates AS (
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
RETURNING 1)
SELECT COUNT(1) INTO __updated_nodes
FROM updates;

-------------
-- File #6 --
-------------

-- Create new nodes where they don't exist
WITH sn AS (
INSERT INTO sensor_nodes (
    site_name,
    metadata,
    geom,
    source_name,
    city,
    country,
    ismobile
)
SELECT
    site_name,
    metadata,
    geom,
    source_name,
    city,
    country,
    ismobile
FROM tempfetchdata_nodes t
WHERE t.sensor_nodes_id is NULL
RETURNING *
), inserted AS (
UPDATE tempfetchdata_nodes tf SET
 sensor_nodes_id = sn.sensor_nodes_id
FROM sn
WHERE tf.sensor_nodes_id is null
and row(tf.site_name, tf.geom, tf.source_name) is not distinct from row(sn.site_name, sn.geom, sn.source_name)
)
SELECT COUNT(1) INTO __inserted_nodes
FROM sn;

-------------
-- File #7 --
-------------

UPDATE tempfetchdata_nodes t
SET sensor_systems_id = ss.sensor_systems_id FROM
sensor_systems ss
WHERE t.sensor_nodes_id = ss.sensor_nodes_id;

-- Add any rows that did not get an id
-- into the rejects table and then delete
WITH inserts AS (
  INSERT INTO rejects
  SELECT clock_timestamp(), 'sensor_nodes', to_jsonb(tf)
  FROM tempfetchdata_nodes tf
  WHERE sensor_nodes_id IS NULL
  RETURNING 1)
SELECT COUNT(1) INTO __rejected_nodes
FROM inserts;

DELETE
FROM tempfetchdata_nodes
WHERE sensor_nodes_id IS NULL;

-- create sensor systems that don't exist
WITH ss AS (
INSERT INTO sensor_systems (sensor_nodes_id)
SELECT DISTINCT sensor_nodes_id FROM tempfetchdata_nodes t
WHERE t.sensor_systems_id is NULL AND t.sensor_nodes_id IS NOT NULL
RETURNING *
) UPDATE tempfetchdata_nodes tf
SET sensor_systems_id = ss.sensor_systems_id
FROM ss WHERE tf.sensor_nodes_id=ss.sensor_nodes_id
and tf.sensor_systems_id is null;

-- Add any rows that did not get an id
-- into the rejects table and then delete
WITH inserts AS (
  INSERT INTO rejects
  SELECT clock_timestamp(), 'sensor_systems', to_jsonb(tf)
  FROM tempfetchdata_nodes tf
  WHERE sensor_systems_id IS NULL
  RETURNING 1)
SELECT COUNT(1) INTO __rejected_systems
FROM inserts;

DELETE
FROM tempfetchdata_nodes
WHERE sensor_systems_id IS NULL;

-- merge sensor node / system ids back to sensors table
UPDATE tempfetchdata_sensors ts SET
    sensor_nodes_id = tn.sensor_nodes_id,
    sensor_systems_id = tn.sensor_systems_id
FROM
    tempfetchdata_nodes tn
WHERE
    ts.tfsid = ANY(tn.tfsids);


-- add any measurands that don't exist
UPDATE tempfetchdata_sensors t
SET measurands_id= m.measurands_id
FROM measurands m
WHERE t.measurand = m.measurand
AND t.units = m.units;

WITH inserts AS (
  INSERT INTO measurands (measurand, units)
  SELECT DISTINCT measurand, units FROM tempfetchdata_sensors t
  WHERE t.measurands_id is NULL
  RETURNING *
), m AS (
  UPDATE tempfetchdata_sensors tf
  SET measurands_id = inserts.measurands_id
  FROM inserts
  WHERE tf.measurand=inserts.measurand
  AND tf.units=inserts.units
  AND tf.measurands_id is null)
SELECT COUNT(1) INTO __inserted_measurands
FROM inserts;

-- get cleaned sensors table
CREATE TEMP TABLE IF NOT EXISTS tempfetchdata_sensors_clean AS
SELECT
    null::int as sensors_id,
    sensor_nodes_id,
    sensor_systems_id,
    measurands_id,
    jsonb_merge_agg(sensor_metadata) as metadata,
    array_merge_agg(tfdids) as tfdids
FROM tempfetchdata_sensors
GROUP BY 1,2,3,4;


-- get sensor id
UPDATE tempfetchdata_sensors_clean t
SET sensors_id = s.sensors_id
FROM sensors s
WHERE t.sensor_systems_id = s.sensor_systems_id
AND t.measurands_id = s.measurands_id
;

-- Add any rows that did not get an id
-- into the rejects table and then delete
WITH inserts AS (
  INSERT INTO rejects
  SELECT clock_timestamp()
  , 'systems'
  , to_jsonb(tf)
  FROM tempfetchdata_sensors_clean tf
  WHERE sensor_systems_id IS NULL
  OR measurands_id IS NULL
  RETURNING 1)
SELECT COUNT(1) INTO __rejected_systems
FROM inserts;

DELETE
FROM tempfetchdata_sensors_clean
WHERE sensor_systems_id IS NULL
OR measurands_id IS NULL;

-- add any sensors that don't exist
WITH s AS (
    INSERT INTO sensors (
        sensor_systems_id,
        measurands_id,
        metadata
    )
    SELECT
        sensor_systems_id,
        measurands_id,
        metadata
    FROM
        tempfetchdata_sensors_clean tf
    WHERE
        tf.sensors_id IS NULL
    RETURNING *
), u AS (UPDATE tempfetchdata_sensors_clean tfc
    SET
        sensors_id = s.sensors_id
    FROM s
    WHERE
        tfc.sensors_id IS NULL
        AND
        s.sensor_systems_id = tfc.sensor_systems_id
        AND
        s.measurands_id = tfc.measurands_id
) SELECT COUNT(1) INTO __inserted_sensors
FROM s;

UPDATE tempfetchdata t
SET sensors_id = ts.sensors_id
FROM tempfetchdata_sensors_clean ts
WHERE t.tfdid = ANY(ts.tfdids);

-- Add any rows that did not get an id into
-- the rejects table and then delete
WITH inserts AS (
  INSERT INTO rejects
  SELECT clock_timestamp()
  , 'sensors'
  , to_jsonb(tf)
  FROM tempfetchdata tf
  WHERE sensors_id IS NULL
  RETURNING 1)
SELECT COUNT(1) INTO __rejected_sensors
FROM inserts;

DELETE
FROM tempfetchdata
WHERE sensors_id IS NULL;

WITH inserts AS (
  INSERT INTO measurements (sensors_id, datetime, value)
  SELECT sensors_id
  , datetime
  , value
  FROM tempfetchdata
  ON CONFLICT DO NOTHING
  RETURNING datetime)
SELECT MIN(datetime)
, MAX(datetime)
, COUNT(1)
INTO __inserted_start_datetime
, __inserted_end_datetime
, __inserted_measurements
FROM inserts;

-- No longer going to manage the fetch log in this way
-- WITH updates AS (
--   UPDATE fetchlogs
--   SET completed_datetime = clock_timestamp()
--   , last_message = NULL -- reset any previous error
--   WHERE key IN (SELECT key FROM ingestfiles)
--   RETURNING 1)
-- SELECT COUNT(1) INTO __keys
-- FROM updates;


RAISE NOTICE 'total-measurements: %, deleted-timescaledb: %, deleted-future-measurements: %, deleteted-past-measurements: %, from: %, to: %, inserted-from: %, inserted-to: %, updated-nodes: %, inserted-measurements: %, inserted-measurands: %, inserted-nodes: %, rejected-nodes: %, rejected-systems: %, rejected-sensors: %, process-time-ms: %'
      , __total_measurements
      , __deleted_timescaledb
      , __deleted_future_measurements
      , __deleted_past_measurements
      , __start_datetime
      , __end_datetime
      , __inserted_start_datetime
      , __inserted_end_datetime
      , __updated_nodes
      , __inserted_measurements
      , __inserted_measurands
      , __inserted_nodes
      , __rejected_nodes
      , __rejected_systems
      , __rejected_sensors
      , 1000 * (extract(epoch FROM clock_timestamp() - __process_start));

END $$;

--SELECT min(datetime), max(datetime) FROM tempfetchdata;
