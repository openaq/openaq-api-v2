import logging
from typing import List

import jq
from fastapi import APIRouter, Depends, Query
from pydantic.typing import Union
from enum import Enum

from ..models.responses import (
    LatestResponse,
    LatestResponseV1,
    LocationsResponse,
    LocationsResponseV1,
    converter,
)
from ..db import DB
from ..models.queries import (
    APIBase,
    City,
    Country,
    Geo,
    # HasGeo,
    Location,
    Measurands,
    Sort,
    EntityTypes,
    SensorTypes,
)

logger = logging.getLogger("locations")

router = APIRouter(
    include_in_schema=True
)


class LocationsOrder(str, Enum):
    city = "city"
    country = "country"
    location = "location"
    sourceName = "sourceName"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"
    count = "count"
    random = "random"
    distance = "distance"


class Locations(
        Location,
        City,
        Country,
        Geo,
        Measurands,
      #  HasGeo,
        APIBase
):
    order_by: LocationsOrder = Query(
        "lastUpdated",
        description="Order by a field",
    )
    sort: Union[Sort, None] = Query(
        "desc", description="Sort Direction e.g. sort=desc", example="desc"
    )
    isMobile: Union[bool, None] = Query(
        None, description="Location is mobile e.g. ?isMobile=true", example="true"
    )
    isAnalysis: Union[bool, None] = Query(
        None,
        description=(
            "Data is the product of a previous "
            "analysis/aggregation and not raw measurements "
            "e.g. ?isAnalysis=true "
        ),
        example="true",
    )
    sourceName: Union[List[str], None] = Query(
        None,
        description="Name of the data source e.g. ?sourceName=Houston%20Mobile",
        example="Houston%20Mobile",
    )
    entity: Union[EntityTypes, None] = Query(
        None,
        description="Source entity type. e.g. ?entity=government",
        example="government",
    )
    sensorType: Union[SensorTypes, None] = Query(
        None,
        description="Type of Sensor e.g. ?sensorType=reference%20grade",
        example="reference%20grade",
    )
    modelName: Union[List[str], None] = Query(
        None, description="Model Name of Sensor e.g. ?modelName=AE33", example="AE33"
    )
    manufacturerName: Union[List[str], None] = Query(
        None,
        description="Manufacturer of Sensor e.g. ?manufacturer=Ecotech",
        example="Ecotech",
    )
    dumpRaw: Union[bool, None] = False

    def order(self):
        stm = self.order_by
        if stm == "location":
            stm = "name"
        elif stm == "distance":
            stm = "st_distance(st_makepoint(:lon,:lat)::geography, geog)"
        elif stm == "count":
            stm = "measurements"

        if stm == "random":
            stm = " random() "
        elif self.order_by != "distance":
            stm = f'"{stm}"'

        return f"{stm} {self.sort} nulls last"

    def where(self):
        wheres = []

        for f, v in self:
            if v is not None:
                if f == "project":
                    if all(isinstance(x, int) for x in self.project):
                        wheres.append("groups_id = ANY(:project)")
                    else:
                        wheres.append("name = ANY(:project)")
                elif f == "location":
                    if all(isinstance(x, int) for x in v):
                        wheres.append(" l.id = ANY(:location) ")
                    else:
                        wheres.append(" name = ANY(:location) ")
                elif f == "country":
                    wheres.append(" country = ANY(:country) ")
                elif f == "city":
                    wheres.append(" city = ANY(:city) ")
                elif f == "parameter_id":
                    wheres.append(
                        """
                        parameters @> jsonb_build_array(jsonb_build_object('id', :parameter_id::int))
                        """
                    )
                elif f == "parameter":
                    if all(isinstance(x, int) for x in v):
                        wheres.append(
                            """
                            parameters @> ANY(
                                jsonb_array_query('parameterId',:parameter::int[])
                                )
                            """
                        )
                    else:
                        wheres.append(
                            """
                            parameters @> ANY(
                                jsonb_array_query('parameter',:parameter::text[])
                                )
                            """
                        )
                elif f == "sourceName":
                    wheres.append(
                        """
                        sources @> ANY(
                            jsonb_array_query('name',:source_name::text[])
                            ||
                            jsonb_array_query('id',:source_name::text[])
                            )
                        """
                    )
                elif f == "entity":
                    wheres.append(
                        """
                        entity = :entity
                        """
                    )
                elif f == "sensorType":
                    wheres.append(
                        """
                        "sensorType" = :sensor_type
                        """
                    )
                elif f == "modelName":
                    wheres.append(
                        """
                        manufacturers @> ANY(
                            jsonb_array_query('modelName',:model_name::text[])
                            )
                        """
                    )
                elif f == "manufacturerName":
                    wheres.append(
                        """
                        manufacturers @> ANY(
                            jsonb_array_query('manufacturerName',:manufacturer_name::text[])
                            )
                        """
                    )
                elif f == "isMobile":
                    wheres.append(f' "isMobile" = {bool(v)} ')
                elif f == "isAnalysis":
                    wheres.append(f' "isAnalysis" = {bool(v)} ')
                elif f == "unit":
                    wheres.append(
                        """
                            parameters @> ANY(
                                jsonb_array_query('unit',:unit::text[])
                                )
                            """
                    )
        wheres.append(self.where_geo())
        if self.order_by == "random":
            wheres.append("\"lastUpdated\" > now() - '2 weeks'::interval")
        wheres = [w for w in wheres if w is not None]
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get(
    "/v2/locations/{location_id}",
    response_model=LocationsResponse,
    summary="Get a location by ID",
    description="Provides a location by location ID",
    tags=["v2"],
)
@router.get(
    "/v2/locations",
    response_model=LocationsResponse,
    summary="Get locations",
    description="Provides a list of locations",
    tags=["v2"],
)
async def locations_get(
    db: DB = Depends(),
    locations: Locations = Depends(Locations.depends()),
):

    qparams = locations.params()

    hidejson = "rawData,"
    if locations.dumpRaw:
        hidejson = ""

    # row_number is required to make sure that the order is
    # preserved after the to_jsonb aggregating, which resorts by id
    # the order by inside row_number ensures that the right sort
    # method is used to determine the row number
    q = f"""
-----------------------------
WITH nodes_instruments AS (
-----------------------------
  SELECT sn.sensor_nodes_id as id
  , jsonb_agg(DISTINCT jsonb_build_object(
     'modelName', i.label
     , 'manufacturerName', mc.full_name
  )) as manufacturers
  FROM sensor_nodes sn
  JOIN sensor_systems ss USING (sensor_nodes_id)
  JOIN instruments i USING (instruments_id)
  JOIN entities mc ON (mc.entities_id = i.manufacturer_entities_id)
  GROUP BY sn.sensor_nodes_id
-------------------------------
), nodes_measurements_count AS (
-------------------------------
  SELECT sn.sensor_nodes_id as id
  , SUM(value_count) as measurements  
  , jsonb_agg(jsonb_build_object(
    'id', m.measurands_id
    , 'parameter', m.measurand
    , 'parameterId', m.measurands_id
    , 'unit', m.units
    , 'displayName', m.measurand||' '||m.units
    , 'count', sl.value_count
    , 'average', sl.value_avg
    , 'lastValue', sl.value_latest
    , 'firstUpdated', sl.datetime_first
    , 'lastUpdated', sl.datetime_last    
    )) as parameters
  FROM sensor_nodes sn
  JOIN sensor_systems ss USING (sensor_nodes_id)
  JOIN sensors s USING (sensor_systems_id)
  JOIN sensors_rollup sl USING (sensors_id)
  JOIN measurands m USING (measurands_id)
  GROUP BY sensor_nodes_id)
--------------------------
SELECT l.id
    , name
    , ismobile as "isMobile"
    , ismonitor as "isMonitor"    
    , city
    , country->>'code' as country
    , datetime_first->>'utc' as "firstUpdated"
    , datetime_last->>'utc' as "lastUpdated"    
    , coordinates
    , sensors
    , timezone
    , bbox(geom) as bounds
    , i.manufacturers
    , COALESCE(s.measurements, 0) as measurements
    , s.parameters
    , COUNT(1) OVER() as found
    FROM locations_view_cached l
    JOIN nodes_instruments i ON (l.id = i.id)
    LEFT JOIN nodes_measurements_count s ON (l.id = s.id)    
    WHERE {locations.where()}
    ORDER BY {locations.order()}
    LIMIT :limit
    OFFSET :offset
    """
    output = await db.fetchPage(q, qparams)
    return output


@router.get(
    "/v2/latest/{location_id}",
    response_model=LatestResponse,
    summary="Get latest measurements by location ID",
    description="Provides latest measurements for a locations by location ID",
    tags=["v2"],
)
@router.get(
    "/v2/latest",
    response_model=LatestResponse,
    summary="Get latest measurements",
    description="Provides a list of locations with latest measurements",
    tags=["v2"],
)
async def latest_get(
    db: DB = Depends(),
    locations: Locations = Depends(Locations.depends()),
):

    data = await locations_get(db, locations)
    meta = data.meta
    res = data.results

    # dprint(res)
    latest_jq = jq.compile(
        """
        .[] |
            {
                location: .name,
                city: .city,
                country: .country,
                coordinates: .coordinates,
                measurements: [
                    .parameters[] | {
                        parameter: .parameter,
                        value: .lastValue,
                        lastUpdated: .lastUpdated,
                        unit: .unit
                    }
                ]
            }

        """
    )

    ret = latest_jq.input(res).all()
    return LatestResponse(meta=meta, results=ret)


async def v1_base(
    db: DB = Depends(),
    locations: Locations = Depends(Locations.depends()),
):
    locations.entity = "government"
    qparams = locations.params()

    q = f"""
        WITH t1 AS (
            SELECT *,
            parameters as measurements,
            json->'pvals'->'site_names' as locations,
            json->'pvals'->'cities' as cities,
            'government' as "sourceType",
            ARRAY['government'] as "sourceTypes",
            json->'pvals'->'source_names' as "sourceNames",
            json->'source_name' as "sourceName",
            json->'sensor_systems' as systems,
            row_number() over () as row
            FROM locations_base_v2
            WHERE
            {locations.where()}
            ORDER BY {locations.order()}
            LIMIT :limit
            OFFSET :offset
        ),
        nodes AS (
            SELECT count(distinct id) as nodes
            FROM locations_base_v2
            WHERE
            {locations.where()}
        ),
        t2 AS (
        SELECT
        row,
        jsonb_strip_nulls(
            to_jsonb(t1) - '{{json,geog, row}}'::text[]
        ) as json
        FROM t1 group by row, t1, json
        )
        SELECT nodes as count, json
        FROM t2, nodes
        ORDER BY row

        ;
        """

    data = await db.fetchOpenAQResult(q, qparams)
    return data


@router.get(
    "/v1/latest/{location_id}",
    response_model=LatestResponseV1,
    summary="Get latest measurements by location ID",
    tags=["v1"],
)
@router.get(
    "/v1/latest",
    response_model=LatestResponseV1,
    summary="Get latest measurements",
    tags=["v1"],
)
async def latest_v1_get(
    db: DB = Depends(),
    locations: Locations = Depends(Locations.depends()),
):

    found = 0
    locations.entity = "government"
    order_by = locations.order()
    qparams = locations.params()

    sql = f"""
  -- start with getting locations with limit
  WITH loc AS (
    SELECT id
    , name as location
    , city
    , country
    , json->>'source_name' as source_name
    , json_build_object(
    'value', (json->'sensor_systems'->0->'sensors'->0->>'data_averaging_period_seconds')::int
    , 'unit', 'seconds'
    ) as "averagingPeriod"
    , parameters
    , case WHEN "isMobile" then null else coordinates end as coordinates
    FROM locations_base_v2
    WHERE
    {locations.where()}
    ORDER BY {locations.order()}
    LIMIT :limit
    OFFSET :offset
  -- but also count locations without the limit
  ), nodes AS (
    SELECT count(distinct id) as n
    FROM locations_base_v2
    WHERE
    {locations.where()}
  -- and then reshape the parameters data
  ), meas AS (
    SELECT loc.id
    , json_agg(jsonb_build_object(
    'parameter', m.parameter
    , 'unit', m.unit
    , 'value', m."lastValue"
    , 'lastUpdated', m."lastUpdated"
    , 'sourceName', loc.source_name
    , 'averagingPeriod', loc."averagingPeriod"
    )) as measurements
    FROM loc, jsonb_to_recordset(loc.parameters) as m(parameter text, "lastValue" float, "lastUpdated" timestamptz, unit text)
    GROUP BY loc.id)
  -- and finally we return it all
SELECT loc.location
, loc.country
, loc.city
, loc.coordinates
, COALESCE(meas.measurements, '[]') as measurements
, (SELECT n FROM nodes) as n
FROM loc
JOIN meas ON (meas.id = loc.id)
    """
    data = await db.fetch(sql, qparams)
    if len(data):
        found = data[0][5]

    return LatestResponseV1(
        meta={"found": found, "page": qparams["page"], "limit": qparams["limit"]},
        results=data,
    )


@router.get(
    "/v1/locations/{location_id}",
    response_model=LocationsResponseV1,
    summary="Get location by ID",
    tags=["v1"],
)
@router.get(
    "/v1/locations",
    response_model=LocationsResponseV1,
    summary="Get locations",
    tags=["v1"],
)
async def locationsv1_get(
    db: DB = Depends(),
    locations: Locations = Depends(Locations.depends()),
):
    data = await v1_base(db, locations)
    meta = data.meta
    res = data.results
    if len(res) == 0:
        return data
    latest_jq = jq.compile(
        """
        .[] |
            {
                id: .id,
                country: .country,
                city: .city,
                cities: .cities,
                location: .name,
                locations: .locations,
                sourceName: .sourceName,
                sourceNames: .sourceNames,
                sourceType: .sourceType,
                sourceTypes: .sourceTypes,
                coordinates: .coordinates,
                firstUpdated: .firstUpdated,
                lastUpdated: .lastUpdated,
                parameters : [ .parameters[].parameter ],
                countsByMeasurement: [
                    .parameters[] | {
                        parameter: .parameter,
                        count: .count
                    }
                ],
                count: .parameters| map(.count) | add
            }

        """
    )

    return converter(meta, res, latest_jq)
