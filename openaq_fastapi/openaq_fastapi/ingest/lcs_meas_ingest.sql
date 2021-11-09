do $$
DECLARE
reject_count int;
insert_count int;
match_count int;
message text;
BEGIN

DELETE FROM meas WHERE ingest_id IS NULL OR datetime is NULL or value IS NULL;
DELETE FROM meas WHERE datetime < '2018-01-01'::timestamptz or datetime>now();

RAISE NOTICE '% records are in the meas table', (SELECT COUNT(1) FROM meas);

WITH m AS (
UPDATE meas
    SET
    sensors_id=s.sensors_id
    FROM sensors s
    WHERE
    s.source_id=ingest_id
    RETURNING 1)
SELECT COUNT(1) INTO match_count
FROM m;

RAISE NOTICE '% records were matched using the source & ingest ids', match_count;

WITH r AS (
INSERT INTO rejects (tbl,r, reason) SELECT
    'meas',
    to_jsonb(meas),
    'SENSOR_MISSING'
FROM meas
WHERE sensors_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO reject_count
FROM r;

RAISE NOTICE '% records were rejected due to missing sensor', reject_count;


DELETE FROM meas WHERE sensors_id IS NULL;

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
FROM
    meas
WHERE
    sensors_id IS NOT NULL
ON CONFLICT DO NOTHING
RETURNING 1)
SELECT COUNT(1)
FROM m INTO insert_count;

RAISE NOTICE '% records were inserted', insert_count;

IF reject_count > 0 THEN
   RAISE NOTICE 'explain here';
END IF;

END $$
