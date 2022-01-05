CREATE TEMP TABLE IF NOT EXISTS ms_sensornodes (
    sensor_nodes_id int,
    ingest_id text,
    site_name text,
    source_name text,
    city text,
    country text,
    ismobile boolean,
    geom geometry,
    metadata jsonb
);

CREATE TEMP TABLE IF NOT EXISTS ms_sensorsystems (
    sensor_systems_id int,
    ingest_id text,
    ingest_sensor_nodes_id text,
    sensor_nodes_id int,
    metadata jsonb
);


CREATE TEMP TABLE IF NOT EXISTS ms_sensors (
    ingest_id text,
    sensors_id int,
    sensor_systems_id int,
    ingest_sensor_systems_id text,
    measurand text,
    units text,
    measurands_id int,
    metadata jsonb
);

CREATE TEMP TABLE keys (key text, last_modified timestamptz);
