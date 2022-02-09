-- Get sensor systems
DO $$
DECLARE
__process_start timestamptz := clock_timestamp();
__inserted_nodes int;
__inserted_sensors int;
__rejected_nodes int;
__rejected_systems int;
__rejected_sensors int;
__rejected_measurands int;

BEGIN

--------------------------
-- lcs_ingest_nodes.sql --
--------------------------

DELETE
FROM ms_sensornodes
WHERE ms_sensornodes.ingest_id IS NULL;

DELETE
FROM ms_sensorsystems
WHERE ms_sensorsystems.ingest_id IS NULL
OR ingest_sensor_nodes_id IS NULL;

DELETE
FROM ms_sensors
WHERE ms_sensors.ingest_id IS NULL
OR ingest_sensor_systems_id IS NULL;

-- match the sensor nodes to get the sensor_nodes_id
UPDATE ms_sensornodes
SET sensor_nodes_id = sensor_nodes.sensor_nodes_id
FROM sensor_nodes
WHERE sensor_nodes.source_name = ms_sensornodes.source_name
AND sensor_nodes.source_id = ms_sensornodes.ingest_id;

-- And now we insert those into the sensor nodes table
-- we are gouping to deal with any duplicates that currently exist
WITH inserts AS (
INSERT INTO sensor_nodes (
  site_name
, source_name
, ismobile
, geom
, metadata
, source_id
)
SELECT site_name
, source_name
, ismobile
, geom
, metadata
, ingest_id
FROM ms_sensornodes
GROUP BY site_name, source_name, ismobile, geom, metadata, ingest_id
ON CONFLICT (source_name, source_id) DO UPDATE
SET
    site_name=coalesce(EXCLUDED.site_name,sensor_nodes.site_name),
    ismobile=coalesce(EXCLUDED.ismobile,sensor_nodes.ismobile),
    geom=coalesce(EXCLUDED.geom,sensor_nodes.geom),
    metadata=COALESCE(sensor_nodes.metadata, '{}') || COALESCE(EXCLUDED.metadata, '{}')
RETURNING 1)
SELECT COUNT(1) INTO __inserted_nodes
FROM inserts;

----------------------------
-- lcs_ingest_systems.sql --
----------------------------

-- fill in any new sensor_nodes_id
UPDATE ms_sensornodes
SET sensor_nodes_id = sensor_nodes.sensor_nodes_id
FROM sensor_nodes
WHERE ms_sensornodes.sensor_nodes_id is null
AND sensor_nodes.source_name = ms_sensornodes.source_name
AND sensor_nodes.source_id = ms_sensornodes.ingest_id;

-- log anything we were not able to get an id for
WITH r AS (
INSERT INTO rejects (t, tbl,r,fetchlogs_id)
SELECT now()
, 'ms_sensornodes-missing-nodes-id'
, to_jsonb(ms_sensornodes)
, fetchlogs_id
FROM ms_sensornodes
WHERE sensor_nodes_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_nodes
FROM r;


UPDATE ms_sensorsystems
SET sensor_nodes_id = ms_sensornodes.sensor_nodes_id
FROM ms_sensornodes
WHERE ms_sensorsystems.ingest_sensor_nodes_id = ms_sensornodes.ingest_id;

UPDATE ms_sensorsystems
SET sensor_systems_id = sensor_systems.sensor_systems_id
FROM sensor_systems
WHERE sensor_systems.sensor_nodes_id = ms_sensorsystems.sensor_nodes_id
AND sensor_systems.source_id = ms_sensorsystems.ingest_id;


-- log anything we were not able to get an id for
WITH r AS (
INSERT INTO rejects (t,tbl,r,fetchlogs_id)
SELECT now()
, 'ms_sensorsystems-missing-nodes-id'
,  to_jsonb(ms_sensorsystems)
,  fetchlogs_id
FROM ms_sensorsystems
WHERE sensor_nodes_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_systems
FROM r;

INSERT INTO sensor_systems (sensor_nodes_id, source_id, metadata)
SELECT sensor_nodes_id
, ingest_id
, metadata
FROM ms_sensorsystems
WHERE sensor_nodes_id IS NOT NULL
GROUP BY sensor_nodes_id, ingest_id, metadata
ON CONFLICT (sensor_nodes_id, source_id) DO UPDATE SET
    metadata=COALESCE(sensor_systems.metadata, '{}') || COALESCE(EXCLUDED.metadata, '{}');

----------------------------
-- lcs_ingest_sensors.sql --
----------------------------

-- Match the sensor system data
UPDATE ms_sensorsystems
SET sensor_systems_id = sensor_systems.sensor_systems_id
FROM sensor_systems
WHERE ms_sensorsystems.sensor_systems_id IS NULL
AND ms_sensorsystems.sensor_nodes_id=sensor_systems.sensor_nodes_id
AND ms_sensorsystems.ingest_id=sensor_systems.source_id
;

WITH r AS (
INSERT INTO rejects (t, tbl,r,fetchlogs_id)
SELECT
  now()
, 'ms_sensorsystems-missing-systems-id'
, to_jsonb(ms_sensorsystems)
, fetchlogs_id
FROM ms_sensorsystems
WHERE sensor_systems_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_systems
FROM r;

UPDATE ms_sensors
SET sensor_systems_id = ms_sensorsystems.sensor_systems_id
FROM ms_sensorsystems
WHERE ms_sensors.ingest_sensor_systems_id = ms_sensorsystems.ingest_id;

WITH r AS (
INSERT INTO rejects (t,tbl,r,fetchlogs_id)
SELECT
  now()
, 'ms_sensors-missing-systems-id'
, to_jsonb(ms_sensors)
, fetchlogs_id
FROM ms_sensors
WHERE sensor_systems_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_sensors
FROM r;


UPDATE ms_sensors
SET sensors_id = sensors.sensors_id
FROM sensors
WHERE sensors.sensor_systems_id=ms_sensors.sensor_systems_id
AND sensors.source_id = ms_sensors.ingest_id;

UPDATE ms_sensors
SET measurands_id = measurands.measurands_id
from measurands
WHERE ms_sensors.measurand=measurands.measurand
and ms_sensors.units=measurands.units;

-- Removed the following because it has the ids hard coded in
-- if we want to continue to filter these out we should do it at the fetcher
-------------------------------------------------------------------------------------------------------------
-- UPDATE ms_sensors                                                                                       --
-- SET measurands_id = 10                                                                                  --
-- WHERE ms_sensors.measurand='ozone'                                                                      --
-- AND ms_sensors.units='ppm';                                                                             --
--                                                                                                         --
-- UPDATE ms_sensors SET measurands_id = 126 WHERE measurands_id is null and ms_sensors.measurand='um010'; --
-- UPDATE ms_sensors SET measurands_id = 130 WHERE measurands_id is null and ms_sensors.measurand='um025'; --
-- UPDATE ms_sensors SET measurands_id = 135 WHERE measurands_id is null and ms_sensors.measurand='um100'; --
-- UPDATE ms_sensors SET measurands_id = 19  WHERE measurands_id is null and ms_sensors.measurand='pm1';   --
-- UPDATE ms_sensors SET measurands_id = 2   WHERE measurands_id is null and ms_sensors.measurand='pm25';  --
-- UPDATE ms_sensors SET measurands_id = 1   WHERE measurands_id is null and ms_sensors.measurand='pm10';  --
--                                                                                                         --
-- DELETE                                                                                                  --
-- FROM ms_sensors                                                                                         --
-- WHERE ingest_id ~* 'purple'                                                                             --
-- AND measurands_id is null                                                                               --
-- AND measurand in ('um003','um050','um005');                                                             --
-------------------------------------------------------------------------------------------------------------
WITH r AS (
INSERT INTO rejects (t, tbl,r,fetchlogs_id)
SELECT
 now()
, 'ms_sensors-missing-measurands-id'
, to_jsonb(ms_sensors)
, fetchlogs_id
FROM ms_sensors
WHERE measurands_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_measurands
FROM r;

WITH inserts AS (
INSERT INTO sensors (
  source_id
, sensor_systems_id
, measurands_id
, metadata)
SELECT ingest_id
, sensor_systems_id
, measurands_id
, metadata
FROM ms_sensors
WHERE measurands_id is not null
AND sensor_systems_id is not null
GROUP BY ingest_id
, sensor_systems_id
, measurands_id
, metadata
ON CONFLICT (sensor_systems_id, measurands_id, source_id) DO UPDATE
SET metadata = COALESCE(sensors.metadata, '{}') || COALESCE(EXCLUDED.metadata, '{}')
RETURNING 1)
SELECT COUNT(1) INTO __inserted_sensors
FROM inserts;


UPDATE ms_sensors
SET sensors_id = sensors.sensors_id
FROM sensors
WHERE sensors.sensor_systems_id=ms_sensors.sensor_systems_id
AND sensors.source_id = ms_sensors.ingest_id;

WITH r AS (
INSERT INTO rejects (t,tbl,r,fetchlogs_id)
SELECT
  now()
  , 'ms_sensors-missing-sensors-id'
  , to_jsonb(ms_sensors)
  , fetchlogs_id
FROM ms_sensors
WHERE sensors_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_sensors
FROM r;

------------------
-- Return stats --
------------------

RAISE NOTICE 'inserted-nodes: %, inserted-sensors: %, rejected-nodes: %, rejected-sensors: %, rejected-measurands: %, process-time-ms: %, source: lcs'
      , __inserted_nodes
      , __inserted_sensors
      , __rejected_nodes
      , __rejected_sensors
      , __rejected_measurands
      , 1000 * (extract(epoch FROM clock_timestamp() - __process_start));

END $$;
