DELETE FROM meas WHERE ingest_id IS NULL OR datetime is NULL or value IS NULL;
DELETE FROM meas WHERE datetime < '2018-01-01'::timestamptz or datetime>now();
UPDATE meas
    SET
    sensors_id=s.sensors_id
    FROM sensors s
    WHERE
    s.source_id=ingest_id;

INSERT INTO rejects (tbl,r) SELECT
    'meas',
    to_jsonb(meas)
FROM meas WHERE sensors_id IS NULL;



DELETE FROM meas WHERE sensors_id IS NULL;

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
ON CONFLICT DO NOTHING;