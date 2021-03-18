
CREATE TABLE IF NOT EXISTS measurands (
    measurands_id int generated always as identity primary key,
    measurand text not null,
    units text not null,
    display text,
    description text,
    is_core bool,
    max_color_value float,
    unique (measurand, units)
);