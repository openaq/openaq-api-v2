-- Get sensor systems


UPDATE tempfetchdata_nodes t
SET sensor_systems_id = ss.sensor_systems_id FROM
sensor_systems ss
WHERE t.sensor_nodes_id = ss.sensor_nodes_id;

-- Add any rows that did not get an id
-- into the rejects table and then delete
INSERT INTO rejects
SELECT clock_timestamp(), 'sensor_nodes', to_jsonb(tf) FROM
tempfetchdata_nodes tf WHERE sensor_nodes_id IS NULL;
DELETE FROM tempfetchdata_nodes WHERE sensor_nodes_id IS NULL;

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
INSERT INTO rejects
SELECT clock_timestamp(), 'sensor_systems', to_jsonb(tf) FROM
tempfetchdata_nodes tf WHERE sensor_systems_id IS NULL;
DELETE FROM tempfetchdata_nodes WHERE sensor_systems_id IS NULL;

-- merge sensor node / system ids back to sensors table
UPDATE tempfetchdata_sensors ts SET
    sensor_nodes_id = tn.sensor_nodes_id,
    sensor_systems_id = tn.sensor_systems_id
FROM
    tempfetchdata_nodes tn
WHERE
    ts.tfsid = ANY(tn.tfsids);


-- add any measurands that don't exist
UPDATE tempfetchdata_sensors t SET measurands_id= m.measurands_id FROM
measurands m
WHERE t.measurand = m.measurand AND t.units = m.units;

WITH m AS (
INSERT INTO measurands (measurand, units)
SELECT DISTINCT measurand, units FROM tempfetchdata_sensors t
WHERE t.measurands_id is NULL
RETURNING *
) UPDATE tempfetchdata_sensors tf SET measurands_id = m.measurands_id
FROM m WHERE tf.measurand=m.measurand
and tf.units=m.units and tf.measurands_id is null;

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
INSERT INTO rejects
SELECT clock_timestamp()
, 'sensors'
, to_jsonb(tf)
FROM tempfetchdata_sensors_clean tf
WHERE sensor_systems_id IS NULL
OR measurands_id IS NULL;

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
) UPDATE tempfetchdata_sensors_clean tfc
    SET
        sensors_id = s.sensors_id
    FROM s
    WHERE
        tfc.sensors_id IS NULL
        AND
        s.sensor_systems_id = tfc.sensor_systems_id
        AND
        s.measurands_id = tfc.measurands_id
;

UPDATE tempfetchdata t
SET sensors_id = ts.sensors_id
FROM tempfetchdata_sensors_clean ts
WHERE t.tfdid = ANY(ts.tfdids);

-- Add any rows that did not get an id into
-- the rejects table and then delete
INSERT INTO rejects
SELECT clock_timestamp()
, 'sensors'
, to_jsonb(tf)
FROM tempfetchdata tf
WHERE sensors_id IS NULL;

DELETE
FROM tempfetchdata
WHERE sensors_id IS NULL;

INSERT INTO measurements (sensors_id, datetime, value)
SELECT sensors_id, datetime, value
FROM tempfetchdata
ON CONFLICT DO NOTHING;


UPDATE fetchlogs
SET completed_datetime=clock_timestamp()
, last_message = NULL -- reset any previous error
WHERE key IN (SELECT key FROM ingestfiles);

SELECT min(datetime), max(datetime) FROM tempfetchdata;
