-- Lookup Node Ids

UPDATE tempfetchdata_nodes t
SET sensor_nodes_id = sn.sensor_nodes_id FROM
sensor_nodes sn
WHERE t.geom is not null
AND st_dwithin(sn.geom, t.geom, .0001)
AND origin='OPENAQ';

UPDATE tempfetchdata_nodes t
SET sensor_nodes_id = sn.sensor_nodes_id FROM
sensor_nodes sn
WHERE
t.sensor_nodes_id is null AND
t.site_name is not null
and t.source_name is not null
and t.site_name = sn.site_name
and t.source_name=sn.source_name
and origin='OPENAQ';