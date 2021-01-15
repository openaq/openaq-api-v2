
DELETE FROM ms_sensornodes WHERE ms_sensornodes.ingest_id IS NULL;
DELETE FROM ms_sensorsystems WHERE ms_sensorsystems.ingest_id is null or ingest_sensor_nodes_id IS NULL;
DELETE FROM ms_sensors WHERE ms_sensors.ingest_id is null OR ingest_sensor_systems_id IS NULL;

UPDATE ms_sensornodes
SET sensor_nodes_id = sensor_nodes.sensor_nodes_id
FROM sensor_nodes
WHERE
sensor_nodes.source_name = ms_sensornodes.source_name
AND
sensor_nodes.source_id = ms_sensornodes.ingest_id;

INSERT INTO sensor_nodes (site_name, source_name, ismobile, geom, metadata, source_id)
SELECT site_name, source_name, ismobile, geom, metadata, ingest_id FROM
ms_sensornodes
ON CONFLICT (source_name, source_id) DO
UPDATE
    SET
    site_name=coalesce(EXCLUDED.site_name,sensor_nodes.site_name),
    ismobile=coalesce(EXCLUDED.ismobile,sensor_nodes.ismobile),
    geom=coalesce(EXCLUDED.geom,sensor_nodes.geom),
    metadata=sensor_nodes.metadata || EXCLUDED.metadata
;

-- fill in any new sensor_nodes_id
UPDATE ms_sensornodes
SET sensor_nodes_id = sensor_nodes.sensor_nodes_id
FROM sensor_nodes
WHERE
ms_sensornodes.sensor_nodes_id is null
AND
sensor_nodes.source_name = ms_sensornodes.source_name
AND
sensor_nodes.source_id = ms_sensornodes.ingest_id;

-- log anything we were not able to get an id for
INSERT INTO rejects (tbl,r) SELECT
    'ms_sensornodes',
    to_jsonb(ms_sensornodes)
FROM ms_sensornodes WHERE sensor_nodes_id IS NULL;


UPDATE ms_sensorsystems
SET sensor_nodes_id = ms_sensornodes.sensor_nodes_id
FROM ms_sensornodes WHERE
ms_sensorsystems.ingest_sensor_nodes_id = ms_sensornodes.ingest_id;

UPDATE ms_sensorsystems
SET sensor_systems_id = sensor_systems.sensor_systems_id
FROM sensor_systems
WHERE
sensor_systems.sensor_nodes_id = ms_sensorsystems.sensor_nodes_id
AND
sensor_systems.source_id = ms_sensorsystems.ingest_id;

-- log anything we were not able to get an id for
INSERT INTO rejects (tbl,r) SELECT
    'ms_sensorsystems',
    to_jsonb(ms_sensorsystems)
FROM ms_sensorsystems WHERE sensor_nodes_id IS NULL;

SELECT * FROM rejects;

INSERT INTO sensor_systems (sensor_nodes_id,source_id, metadata)
SELECT sensor_nodes_id, ingest_id, metadata
FROM ms_sensorsystems
ON CONFLICT (sensor_nodes_id, source_id)
DO
UPDATE SET
    metadata=sensor_systems.metadata || EXCLUDED.metadata
;

UPDATE ms_sensorsystems
SET sensor_systems_id = sensor_systems.sensor_systems_id
FROM sensor_systems
WHERE ms_sensorsystems.sensor_systems_id IS NULL
AND
ms_sensorsystems.sensor_nodes_id=sensor_systems.sensor_nodes_id
AND
ms_sensorsystems.ingest_id=sensor_systems.source_id
;

INSERT INTO rejects (tbl,r) SELECT
    'ms_sensorsystems',
    to_jsonb(ms_sensorsystems)
FROM ms_sensorsystems WHERE sensor_systems_id IS NULL;

UPDATE ms_sensors
SET sensor_systems_id = ms_sensorsystems.sensor_systems_id
FROM ms_sensorsystems WHERE
ms_sensors.ingest_sensor_systems_id = ms_sensorsystems.ingest_id;

INSERT INTO rejects (tbl,r) SELECT
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

UPDATE ms_sensors SET measurands_id =
measurands.measurands_id from measurands WHERE
ms_sensors.measurand=measurands.measurand and
ms_sensors.units=measurands.units;

UPDATE ms_sensors
SET measurands_id = 10
WHERE
ms_sensors.measurand='ozone'
AND
ms_sensors.units='ppm';

INSERT INTO rejects (tbl,r) SELECT
    'ms_sensors no measurand',
    to_jsonb(ms_sensors)
FROM ms_sensors WHERE measurands_id IS NULL;

INSERT INTO sensors (source_id, sensor_systems_id, measurands_id, metadata)
SELECT ingest_id, sensor_systems_id, measurands_id, metadata
FROM ms_sensors where measurands_id is not null and sensor_systems_id is not null
ON CONFLICT (sensor_systems_id, measurands_id, source_id) DO
UPDATE SET
    metadata=sensors.metadata || EXCLUDED.metadata
;

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
