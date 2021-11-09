DO $$
DECLARE
reject_count int;
insert_count int;
parent_match_count int;
sensor_match_count int;
parameter_match_count int;
life_cycle_match_count int;
BEGIN


-- Do stuff

-- First we try to find a matching sensor
WITH m AS (
UPDATE ms_versions
    SET sensors_id=s.sensors_id
    FROM sensors s
    WHERE s.source_id=sensor_id
    RETURNING 1)
SELECT COUNT(1) INTO sensor_match_count
FROM m;

-- the parent sensor is the root sensor that these data
-- are a version of
WITH m AS (
UPDATE ms_versions
    SET parent_sensors_id=s.sensors_id
    FROM sensors s
    WHERE s.source_id=parent_sensor_id
    RETURNING 1)
SELECT COUNT(1) INTO parent_match_count
FROM m;


WITH m AS (
UPDATE ms_versions
    SET life_cycles_id=l.life_cycles_id
    FROM life_cycles l
    WHERE l.short_code=life_cycle_id
    RETURNING 1)
SELECT COUNT(1) INTO life_cycle_match_count
FROM m;


-- -- have to deal with the duplication parameter names issue
-- -- the filter should deal with the duplicates but to be save
-- -- we are going to group as well
-- WITH m AS (
-- UPDATE ms_versions
--     SET measurands_id = m1.measurands_id
--     FROM (
-- 	SELECT MIN(measurands_id) as measurands_id
--     	, measurand
--     	FROM measurands
--     	WHERE units IN ('ppm','ppb')
--     	GROUP BY measurand
-- 	) as m1
--     WHERE ms_versions.parameter = m1.measurand
--     RETURNING 1)
-- SELECT COUNT(1) INTO parameter_match_count
-- FROM m;

RAISE NOTICE 'Matched % sensors, % parents, and % life cycles of % versions'
, sensor_match_count
, parent_match_count
, life_cycle_match_count
, (SELECT COUNT(1) FROM ms_versions);

INSERT INTO versions (
 life_cycles_id
 , sensors_id
 , parent_sensors_id
 , readme
 , version_date
 , metadata
 ) SELECT
 life_cycles_id
 , sensors_id
 , parent_sensors_id
 , readme
 , version_id::date -- will need something to check this
 , metadata
 FROM ms_versions
 WHERE life_cycles_id IS NOT NULL
 AND sensors_id IS NOT NULL
 AND parent_sensors_id IS NOT NULL
 AND version_id IS NOT NULL
  ;



END $$;
