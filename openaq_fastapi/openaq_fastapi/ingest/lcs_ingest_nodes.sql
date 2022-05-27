DELETE FROM ms_sensornodes WHERE ms_sensornodes.ingest_id IS NULL;
DELETE FROM ms_sensorsystems WHERE ms_sensorsystems.ingest_id is null or ingest_sensor_nodes_id IS NULL;
DELETE FROM ms_sensors WHERE ms_sensors.ingest_id is null OR ingest_sensor_systems_id IS NULL;

SELECT notify('After Deletes');

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

SELECT notify('After nodes');
