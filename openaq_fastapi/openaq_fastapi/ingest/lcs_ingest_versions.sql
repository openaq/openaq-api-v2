DO $$
DECLARE
reject_count int;
insert_count int;
parent_match_count int;
sensor_match_count int;
parameter_match_count int;
BEGIN


-- Do stuff

-- First we try to find a matching sensor
WITH m AS (
UPDATE ms_versioning
    SET sensors_id=s.sensors_id
    FROM sensors s
    WHERE s.source_id=sensor_id
    RETURNING 1)
SELECT COUNT(1) INTO sensor_match_count
FROM m;

-- the parent sensor is the root sensor that these data
-- are a version of
WITH m AS (
UPDATE ms_versioning
    SET parent_sensors_id=s.sensors_id
    FROM sensors s
    WHERE s.source_id=parent_sensor_id
    RETURNING 1)
SELECT COUNT(1) INTO parent_match_count
FROM m;

-- WITH m AS (
-- UPDATE ms_versioning
--     SET measurands_id = measurands.measurands_id
--     FROM measurands
--     WHERE ms_sensors.measurand = measurands.measurand
--     AND ms_sensors.units=measurands.units
--     RETURNING 1)
-- SELECT COUNT(1) INTO parameter_match_count
-- FROM m;




END $$;
