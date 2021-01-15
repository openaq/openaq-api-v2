CREATE TEMP TABLE meas (
    ingest_id text,
    sensors_id int,
    value float,
    datetime timestamptz,
    lon float,
    lat float
);
CREATE TEMP TABLE keys (key text, last_modified timestamptz);