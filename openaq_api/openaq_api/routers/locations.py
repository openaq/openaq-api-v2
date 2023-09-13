import logging
from enum import StrEnum
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query

from ..db import DB
from ..models.queries import (
    APIBase,
    City,
    Country,
    EntityTypes,
    Geo,
    Location,
    LocationPath,
    Measurands,
    SensorTypes,
    Sort,
)
from ..models.responses import (
    LatestResponse,
    LatestResponseV1,
    LocationsResponse,
    LocationsResponseV1,
)

logger = logging.getLogger("locations")

router = APIRouter(include_in_schema=True)


class LocationsOrder(StrEnum):
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
    APIBase,
):
    order_by: LocationsOrder = Query(
        "lastUpdated",
        description="Order by a field",
    )
    sort: Sort | None = Query(
        "desc", description="Sort Direction e.g. sort=desc", examples=["desc"]
    )
    isMobile: bool | None = Query(
        None, description="Location is mobile e.g. ?isMobile=true", examples=["true"]
    )
    isAnalysis: bool | None = Query(
        None,
        description=(
            "Data is the product of a previous "
            "analysis/aggregation and not raw measurements "
            "e.g. ?isAnalysis=true "
        ),
        examples=["true"],
    )
    sourceName: list[str] | None = Query(
        None,
        description="Name of the data source e.g. ?sourceName=Houston%20Mobile",
        examples=["Houston%20Mobile"],
    )
    entity: EntityTypes | None = Query(
        None,
        description="Source entity type. e.g. ?entity=government",
        examples=["government"],
    )
    sensorType: SensorTypes | None = Query(
        None,
        description="Type of Sensor e.g. ?sensorType=reference%20grade",
        examples=["reference%20grade"],
    )
    modelName: list[str] | None = Query(
        None, description="Model Name of Sensor e.g. ?modelName=AE33", examples=["AE33"]
    )
    manufacturerName: list[str] | None = Query(
        None,
        description="Manufacturer of Sensor e.g. ?manufacturer=Ecotech",
        examples=["Ecotech"],
    )
    dumpRaw: bool | None = Query(False)

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
                        wheres.append(" l.name = ANY(:location) ")
                elif f == "country":
                    wheres.append(" country->>'code' = ANY(:country) ")
                elif f == "country_id":
                    wheres.append(" (country->>'id')::int = :country_id ")
                elif f == "city":
                    wheres.append(" city = ANY(:city) ")
                elif f == "parameter_id":
                    wheres.append(
                        """
                        :parameter_id::int = ANY(parameter_ids)
                        """
                    )
                elif f == "parameter":
                    if all(isinstance(x, int) for x in v):
                        wheres.append(
                            """
                            parameter_ids @> :parameter::int[]
                            """
                        )
                    else:
                        wheres.append(
                            """
                            s.parameters @> :parameter::text[]
                            """
                        )
                elif f == "sourceName":
                    wheres.append(
                        """
                        provider->>'name' = ANY(:source_name::text[])
                        """
                    )
                elif f == "entity":
                    wheres.append(
                        """
                        owner->>'type' ~* :entity
                        """
                    )
                elif f == "sensorType":
                    if v == "reference grade":
                        wheres.append("ismonitor")
                    elif v == "low-cost sensor":
                        wheres.append("NOT ismonitor")
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
                        l.manufacturers @> :manufacturer_name::text[]
                        """
                    )
                elif f == "isMobile":
                    wheres.append(f" ismobile = {bool(v)} ")
                elif f == "isAnalysis":
                    wheres.append(f' "is_analysis" = {bool(v)} ')
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


class LocationQuery(LocationPath, APIBase):
    location_id: int = Path(..., description="The ID of the location")

    def where(self) -> str:
        return "l.id = :location_id"


@router.get(
    "/v2/locations/{location_id}",
    response_model=LocationsResponse,
    summary="Get a location by ID",
    description="Provides a location by location ID",
    tags=["v2"],
)
async def get_v2_location_by_id(
    locations: Annotated[LocationQuery, Depends(LocationQuery.depends())],
    db: DB = Depends(),
):
    qparams = locations.params()

    # row_number is required to make sure that the order is
    # preserved after the to_jsonb aggregating, which resorts by id
    # the order by inside row_number ensures that the right sort
    # method is used to determine the row number
    q = f"""
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
    , m.manufacturers
    , COALESCE(s.total_count, 0) as measurements
    , s.measurements as parameters
    , provider->>'name' as "sourceName"
    , COUNT(1) OVER() as found
    FROM locations_view_cached l
    LEFT JOIN locations_manufacturers_cached m ON (m.id = l.id)
    LEFT JOIN locations_latest_measurements_cached s ON (l.id = s.id)
    WHERE {locations.where()}
    LIMIT :limit
    OFFSET :offset;
    """
    output = await db.fetchPage(q, qparams)
    return output


@router.get(
    "/v2/locations",
    response_model=LocationsResponse,
    summary="Get locations",
    description="Provides a list of locations",
    tags=["v2"],
)
async def locations_get(
    locations: Annotated[Locations, Depends(Locations.depends())],
    db: DB = Depends(),
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
    , m.manufacturers
    , COALESCE(s.total_count, 0) as measurements
    , s.measurements as parameters
    , provider->>'name' as "sourceName"
    , COUNT(1) OVER() as found
    FROM locations_view_cached l
    LEFT JOIN locations_manufacturers_cached m ON (m.id = l.id)
    LEFT JOIN locations_latest_measurements_cached s ON (l.id = s.id)
    WHERE {locations.where()}
    ORDER BY {locations.order()}
    LIMIT :limit
    OFFSET :offset;
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
async def get_v2_latest_by_id(
    locations: Annotated[LocationQuery, Depends(LocationQuery.depends())],
    db: DB = Depends(),
):
    qparams = locations.params()

    q = f"""
     SELECT l.id
    , name as location
    , city
    , country->>'code' as country
    , coordinates
    , datetime_first->>'utc' as "firstUpdated"
    , datetime_last->>'utc' as "lastUpdated"
    , s.measurements
    , COUNT(1) OVER() as found
    FROM locations_view_cached l
    JOIN locations_latest_measurements_cached s ON (l.id = s.id)
    WHERE {locations.where()}
    LIMIT :limit
    OFFSET :offset
    """
    output = await db.fetchPage(q, qparams)
    return output


@router.get(
    "/v2/latest",
    response_model=LatestResponse,
    summary="Get latest measurements",
    description="Provides a list of locations with latest measurements",
    tags=["v2"],
)
async def latest_get(
    locations: Annotated[Locations, Depends(Locations)],
    db: DB = Depends(),
):
    qparams = locations.params()

    q = f"""
     SELECT l.id
    , name as location
    , city
    , country->>'code' as country
    , coordinates
    , datetime_first->>'utc' as "firstUpdated"
    , datetime_last->>'utc' as "lastUpdated"
    , s.measurements
    , COUNT(1) OVER() as found
    FROM locations_view_cached l
    JOIN locations_latest_measurements_cached s ON (l.id = s.id)
    WHERE {locations.where()}
    ORDER BY {locations.order()}
    LIMIT :limit
    OFFSET :offset
    """
    output = await db.fetchPage(q, qparams)
    return output


@router.get(
    "/v1/latest/{location_id}",
    response_model=LatestResponseV1,
    summary="Get latest measurements by location ID",
    tags=["v1"],
)
async def get_v1_latest_by_id(
    locations: LocationQuery = Depends(LocationQuery.depends()),
    db: DB = Depends(),
):
    qparams = locations.params()

    q = f"""
-----------------------------
WITH locations AS (
-----------------------------
SELECT l.id
    , name as location
    , city
    , country->>'code' as country
    , coordinates
    , datetime_first->>'utc' as "firstUpdated"
    , datetime_last->>'utc' as "lastUpdated"
    , s.measurements
    FROM locations_view_cached l
    JOIN locations_latest_measurements_cached s ON (l.id = s.id)
    WHERE {locations.where()}
    LIMIT :limit
    OFFSET :offset
-------------------------------
), locations_count AS (
   SELECT COUNT(1) as found
   FROM locations_view_cached l
   JOIN locations_latest_measurements_cached s ON (l.id = s.id)
   WHERE {locations.where()}
-------------------------------
)
--------------------------
  SELECT l.*
  , m.measurements
  , c.found
  FROM locations_count c
  , locations l
  JOIN locations_latest_measurements_cached m USING (id);
    """
    output = await db.fetchPage(q, qparams)
    return output


@router.get(
    "/v1/latest",
    response_model=LatestResponseV1,
    summary="Get latest measurements",
    tags=["v1"],
)
async def latest_v1_get(
    locations: Annotated[Locations, Depends(Locations.depends())],
    db: DB = Depends(),
):
    locations.entity = "government"
    qparams = locations.params()

    q = f"""
-----------------------------
WITH locations AS (
-----------------------------
SELECT l.id
    , name as location
    , city
    , country->>'code' as country
    , coordinates
    , datetime_first->>'utc' as "firstUpdated"
    , datetime_last->>'utc' as "lastUpdated"
    , s.measurements
    FROM locations_view_cached l
    JOIN locations_latest_measurements_cached s ON (l.id = s.id)
    WHERE {locations.where()}
    ORDER BY {locations.order()}
    LIMIT :limit
    OFFSET :offset
-------------------------------
), locations_count AS (
   SELECT COUNT(1) as found
   FROM locations_view_cached l
   JOIN locations_latest_measurements_cached s ON (l.id = s.id)
   WHERE {locations.where()}
-------------------------------
)
--------------------------
  SELECT l.*
  , m.measurements
  , c.found
  FROM locations_count c
  , locations l
  JOIN locations_latest_measurements_cached m USING (id);
    """
    output = await db.fetchPage(q, qparams)
    return output


@router.get(
    "/v1/locations/{location_id}",
    response_model=LocationsResponseV1,
    summary="Get location by ID",
    tags=["v1"],
)
async def get_v1_locations_by_id(
    locations: Annotated[LocationQuery, Depends(LocationQuery.depends())],
    db: DB = Depends(),
):
    qparams = locations.params()
    q = f"""
WITH locations AS (
SELECT l.id
    , name as location
    , ARRAY[name] as locations
    , city
    , ARRAY[city] as cities
    , provider->>'name' as "sourceName"
    , ARRAY[provider->>'name'] as "sourceNames"
    , country->>'code' as country
    , datetime_first->>'utc' as "firstUpdated"
    , datetime_last->>'utc' as "lastUpdated"
    , coordinates
    , CASE owner->>'type'
         WHEN 'Governmental Organization' THEN 'government'
         WHEN 'Research Organization' THEN 'research'
         WHEN 'Community Organization' THEN 'community'
         ELSE 'n/a'
        END as "sourceType"
    , CASE owner->>'type'
         WHEN 'Governmental Organization' THEN '{{government}}'::text[]
         WHEN 'Research Organization' THEN '{{research}}'::text[]
         WHEN 'Community Organization' THEN '{{community}}'::text[]
         ELSE '{{na}}'::text[]
        END as "sourceTypes"
    , s.parameters
    , s.counts as "countsByMeasurement"
    , s.total_count as count
    , COUNT(1) OVER() as found
    FROM locations_view_cached l
    LEFT JOIN locations_latest_measurements_cached s ON (l.id = s.id)
    WHERE {locations.where()}
    LIMIT :limit
    OFFSET :offset
-----------------------------
), nodes_instruments AS (
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
)
--------------------------
SELECT l.*
    FROM locations l
    JOIN nodes_instruments i ON (l.id = i.id)
    """
    output = await db.fetchPage(q, qparams)
    return output


@router.get(
    "/v1/locations",
    response_model=LocationsResponseV1,
    summary="Get locations",
    tags=["v1"],
)
async def locationsv1_get(
    locations: Annotated[Locations, Depends(Locations.depends())],
    db: DB = Depends(),
):
    locations.entity = "government"
    qparams = locations.params()

    q = f"""
WITH locations AS (
SELECT l.id
    , name as location
    , ARRAY[name] as locations
    , city
    , ARRAY[city] as cities
    , provider->>'name' as "sourceName"
    , ARRAY[provider->>'name'] as "sourceNames"
    , country->>'code' as country
    , datetime_first->>'utc' as "firstUpdated"
    , datetime_last->>'utc' as "lastUpdated"
    , coordinates
    , CASE owner->>'type'
         WHEN 'Governmental Organization' THEN 'government'
         WHEN 'Research Organization' THEN 'research'
         WHEN 'Community Organization' THEN 'community'
         ELSE 'n/a'
        END as "sourceType"
    , CASE owner->>'type'
         WHEN 'Governmental Organization' THEN '{{government}}'::text[]
         WHEN 'Research Organization' THEN '{{research}}'::text[]
         WHEN 'Community Organization' THEN '{{community}}'::text[]
         ELSE '{{na}}'::text[]
        END as "sourceTypes"
    , s.parameters
    , s.counts as "countsByMeasurement"
    , s.total_count as count
    , COUNT(1) OVER() as found
    FROM locations_view_cached l
    LEFT JOIN locations_latest_measurements_cached s ON (l.id = s.id)
    WHERE {locations.where()}
    ORDER BY {locations.order()}
    LIMIT :limit
    OFFSET :offset
-----------------------------
), nodes_instruments AS (
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
)
--------------------------
SELECT l.*
    FROM locations l
    JOIN nodes_instruments i ON (l.id = i.id)
    """
    output = await db.fetchPage(q, qparams)
    return output
