
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
INSERT INTO rejects (t, tbl,r) SELECT
    now(),
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
INSERT INTO rejects (t, tbl,r) SELECT
    now(),
    'ms_sensorsystems',
    to_jsonb(ms_sensorsystems)
FROM ms_sensorsystems WHERE sensor_nodes_id IS NULL;

SELECT notify('immediately before insert on systems');

INSERT INTO sensor_systems (sensor_nodes_id, source_id, metadata)
SELECT sensor_nodes_id, ingest_id, metadata
FROM ms_sensorsystems
WHERE sensor_nodes_id IS NOT NULL
ON CONFLICT (sensor_nodes_id, source_id)
DO
UPDATE SET
    metadata=sensor_systems.metadata || EXCLUDED.metadata
;

SELECT notify('After systems');
