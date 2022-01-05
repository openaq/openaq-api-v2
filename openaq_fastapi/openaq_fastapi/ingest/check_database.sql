do $$
DECLARE
sn int;
BEGIN

SELECT COUNT(1) INTO sn
FROM sensor_nodes;
RAISE NOTICE 'node count: %', sn;

SELECT COUNT(1) INTO sn
FROM sensor_systems;
RAISE NOTICE 'system count: %', sn;

SELECT COUNT(1) INTO sn
FROM measurands;
RAISE NOTICE 'parameter count: %', sn;

SELECT COUNT(1) INTO sn
FROM sensors;
RAISE NOTICE 'sensor count: %', sn;

SELECT COUNT(1) INTO sn
FROM measurements;
RAISE NOTICE 'measurement count: %', sn;

SELECT COUNT(1) INTO sn
FROM versions;
RAISE NOTICE 'versions count: %', sn;

SELECT COUNT(1) INTO sn
FROM rejects;
RAISE NOTICE 'rejects count: %', sn;

--SELECT COUNT(1) INTO sn
--FROM meas;
--RAISE NOTICE 'meas count: %', sn;

SELECT COUNT(1) INTO sn
FROM analyses;
RAISE NOTICE 'analyses count: %', sn;

end $$;
