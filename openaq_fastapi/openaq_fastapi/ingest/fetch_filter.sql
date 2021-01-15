DELETE FROM tempfetchdata
WHERE
datetime <= (
    SELECT max(range_end)
    FROM timescaledb_information.chunks
    WHERE
        hypertable_name IN ('rollups', 'measurements')
        AND is_compressed
);
DELETE FROM tempfetchdata WHERE datetime > now();
DELETE FROM tempfetchdata WHERE datetime < (SELECT max(datetime) - '2 days'::interval from tempfetchdata)
;
SELECT min(datetime), max(datetime) FROM tempfetchdata;