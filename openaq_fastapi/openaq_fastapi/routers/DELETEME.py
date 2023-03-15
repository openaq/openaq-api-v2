results = [
{
"url": "http://agaar.mn/aqdata/stationlist?period=h&language=en",
"adapter": "agaar_mn",
"name": "Agaar.mn",
"city": "Ulaanbaatar",
"country": "MN",
"description": "",
"sourceURL": "http://agaar.mn/index",
"resolution": "1 hr",
"contacts": [
"info@openaq.org"
],
"active": false
},

 SELECT 	
--        sn.metadata->'attribution'->0->>'url' as url
        , data->>'adapter' as adapter

	      *
        FROM 
            sensors_rollup sr
        JOIN 
            sensors s USING (sensors_id)
        JOIN
            sensor_systems ss USING (sensor_systems_id)
        JOIN
            sensor_nodes sn USING (sensor_nodes_id)
        JOIN 
            countries c USING (countries_id)
        JOIN
            measurands m USING (measurands_id)
        JOIN 
            providers p USING (providers_id)
        JOIN 
            sources_from_openaq sfoaq USING (id)
        WHERE
        c.iso IS NOT NULL
		Limit 5

        SELECT metadata||jsonb_build_object('active', p.is_active, 'adapter', a.name) 
        FROM providers p
        JOIN adapters a ON (p.adapters_id = a.adapters_id) 