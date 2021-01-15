-- Create new nodes where they don't exist
WITH sn AS (
INSERT INTO sensor_nodes (
    site_name,
    metadata,
    geom,
    source_name,
    city,
    country,
    ismobile
)
SELECT
    site_name,
    metadata,
    geom,
    source_name,
    city,
    country,
    ismobile
FROM tempfetchdata_nodes t
WHERE t.sensor_nodes_id is NULL
RETURNING *
)
UPDATE tempfetchdata_nodes tf SET sensor_nodes_id = sn.sensor_nodes_id
FROM sn WHERE tf.sensor_nodes_id is null
and row(tf.site_name, tf.geom, tf.source_name) is not distinct
from row(sn.site_name, sn.geom, sn.source_name);