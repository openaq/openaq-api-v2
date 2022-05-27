-- Get sensor systems
DO $$
DECLARE
__process_start timestamptz := clock_timestamp();
__inserted_measurements int;
__rejected_measurements int;
__exported_days int;
BEGIN

DELETE
FROM meas
WHERE ingest_id IS NULL
OR datetime is NULL
OR value IS NULL;

DELETE
FROM meas
WHERE datetime < '2018-01-01'::timestamptz
OR datetime>now();

DELETE
FROM rejects
WHERE fetchlogs_id IN (SELECT fetchlogs_id FROM meas)
AND tbl ~* '^meas';

UPDATE meas
SET sensors_id=s.sensors_id
FROM sensors s
WHERE s.source_id=ingest_id;

WITH r AS (
INSERT INTO rejects (t,tbl,r,fetchlogs_id)
SELECT
    current_timestamp
    , 'meas-missing-sensors-id'
    , to_jsonb(meas)
    , fetchlogs_id
FROM meas
WHERE sensors_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_measurements
FROM r;


DELETE
FROM meas
WHERE sensors_id IS NULL;

-- --Some fake data to make it easier to test this section
-- TRUNCATE meas;
-- INSERT INTO meas (ingest_id, sensors_id, value, datetime)
-- SELECT 'fake-ingest'
-- , (SELECT sensors_id FROM sensors ORDER BY random() LIMIT 1)
-- , -99
-- , generate_series(now() - '3day'::interval, current_date, '1hour'::interval);


WITH m AS (
INSERT INTO measurements (
    sensors_id,
    datetime,
    value,
    lon,
    lat
) SELECT
    DISTINCT
    sensors_id,
    datetime,
    value,
    lon,
    lat
FROM meas
WHERE sensors_id IS NOT NULL
ON CONFLICT DO NOTHING
RETURNING 1)
SELECT COUNT(1) INTO __inserted_measurements
FROM m;

-- Update the export queue/logs to export these records
-- wrap it in a block just in case the database does not have this module installed
-- we subtract the second because the data is assumed to be time ending
WITH e AS (
INSERT INTO open_data_export_logs (sensor_nodes_id, day, records, measurands, modified_on)
SELECT sn.sensor_nodes_id
, ((m.datetime - '1sec'::interval) AT TIME ZONE (COALESCE(sn.metadata->>'timezone', 'UTC'))::text)::date as day
, COUNT(1)
, COUNT(DISTINCT p.measurands_id)
, MAX(now())
FROM meas m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN measurands p ON (s.measurands_id = p.measurands_id)
JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
JOIN sensor_nodes sn ON (ss.sensor_nodes_id = sn.sensor_nodes_id)
GROUP BY sn.sensor_nodes_id
, ((m.datetime - '1sec'::interval) AT TIME ZONE (COALESCE(sn.metadata->>'timezone', 'UTC'))::text)::date
ON CONFLICT (sensor_nodes_id, day) DO UPDATE
SET records = EXCLUDED.records
, measurands = EXCLUDED.measurands
, modified_on = EXCLUDED.modified_on
RETURNING 1)
SELECT COUNT(1) INTO __exported_days
FROM e;

RAISE NOTICE 'inserted-measurements: %, rejected-measurements: %, exported-sensor-days: %, process-time-ms: %, source: lcs'
      , __inserted_measurements
      , __rejected_measurements
      , __exported_days
      , 1000 * (extract(epoch FROM clock_timestamp() - __process_start));

EXCEPTION WHEN OTHERS THEN
 RAISE NOTICE 'Failed to export to logs: %', SQLERRM
 USING HINT = 'Make sure that the open data module is installed';

END $$;
