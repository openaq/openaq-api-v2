
UPDATE ms_sensorsystems
SET sensor_systems_id = sensor_systems.sensor_systems_id
FROM sensor_systems
WHERE ms_sensorsystems.sensor_systems_id IS NULL
AND
ms_sensorsystems.sensor_nodes_id=sensor_systems.sensor_nodes_id
AND
ms_sensorsystems.ingest_id=sensor_systems.source_id
;

INSERT INTO rejects (t, tbl,r) SELECT
    now(),
    'ms_sensorsystems',
    to_jsonb(ms_sensorsystems)
FROM ms_sensorsystems WHERE sensor_systems_id IS NULL;

UPDATE ms_sensors
SET sensor_systems_id = ms_sensorsystems.sensor_systems_id
FROM ms_sensorsystems WHERE
ms_sensors.ingest_sensor_systems_id = ms_sensorsystems.ingest_id;

INSERT INTO rejects (t, tbl,r) SELECT
    now(),
    'ms_sensors',
    to_jsonb(ms_sensors)
FROM ms_sensors WHERE sensor_systems_id IS NULL;


UPDATE ms_sensors
SET sensors_id = sensors.sensors_id
FROM sensors
WHERE
sensors.sensor_systems_id=ms_sensors.sensor_systems_id
AND
sensors.source_id = ms_sensors.ingest_id;

SELECT count(*) from measurands;

/*
INSERT INTO measurands (measurand, units)
SELECT DISTINCT measurand, units FROM ms_sensors
ON CONFLICT DO NOTHING;

SELECT count(*) from measurands;
*/

UPDATE ms_sensors
SET measurands_id = measurands.measurands_id
from measurands
WHERE ms_sensors.measurand=measurands.measurand
and ms_sensors.units=measurands.units;

UPDATE ms_sensors
SET measurands_id = 10
WHERE
ms_sensors.measurand='ozone'
AND
ms_sensors.units='ppm';

UPDATE ms_sensors SET measurands_id = 126 WHERE measurands_id is null and ms_sensors.measurand='um010';
UPDATE ms_sensors SET measurands_id = 130 WHERE measurands_id is null and ms_sensors.measurand='um025';
UPDATE ms_sensors SET measurands_id = 135 WHERE measurands_id is null and ms_sensors.measurand='um100';
UPDATE ms_sensors SET measurands_id = 19  WHERE measurands_id is null and ms_sensors.measurand='pm1';
UPDATE ms_sensors SET measurands_id = 2   WHERE measurands_id is null and ms_sensors.measurand='pm25';
UPDATE ms_sensors SET measurands_id = 1   WHERE measurands_id is null and ms_sensors.measurand='pm10';

DELETE FROM ms_sensors WHERE ingest_id ~* 'purple' AND measurands_id is null AND measurand in ('um003','um050','um005');

INSERT INTO rejects (t, tbl,r) SELECT
    now(),
    'ms_sensors no measurand',
    to_jsonb(ms_sensors)
FROM ms_sensors WHERE measurands_id IS NULL;

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
ON CONFLICT (sensor_systems_id, measurands_id, source_id) DO
UPDATE SET
    metadata=sensors.metadata || EXCLUDED.metadata
;


SELECT notify('After sensors');


UPDATE ms_sensors
SET sensors_id = sensors.sensors_id
FROM sensors
WHERE
sensors.sensor_systems_id=ms_sensors.sensor_systems_id
AND
sensors.source_id = ms_sensors.ingest_id;


INSERT INTO rejects (tbl,r)
SELECT
    'ms_sensors',
    to_jsonb(ms_sensors)
FROM ms_sensors WHERE sensors_id IS NULL;
