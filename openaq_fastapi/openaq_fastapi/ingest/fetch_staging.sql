CREATE TEMP TABLE IF NOT EXISTS tempfetchdata (
    location text,
    value float,
    unit text,
    parameter text,
    country text,
    city text,
    data jsonb,
    source_name text,
    datetime timestamptz,
    coords geography,
    source_type text,
    mobile boolean,
    avpd_unit text,
    avpd_value float,
    tfdid int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sensors_id int
);

CREATE TEMP TABLE ingestfiles(
    key text
);