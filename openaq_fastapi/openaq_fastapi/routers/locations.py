import logging
from typing import List

import jq
from fastapi import APIRouter, Depends, Query
from pydantic.typing import Optional
from enum import Enum
from ..db import DB
from ..models.queries import (
    APIBase,
    City,
    Country,
    Geo,
    HasGeo,
    Location,
    Measurands,
    Sort,
    EntityTypes,
    SensorTypes,
)

from openaq_fastapi.models.responses import OpenAQResult, converter

logger = logging.getLogger("locations")

router = APIRouter()


class LocationsOrder(str, Enum):
    city = "city"
    country = "country"
    location = "location"
    sourceName = "sourceName"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"
    count = "count"
    random = "random"


class Locations(Location, City, Country, Geo, Measurands, HasGeo, APIBase):
    order_by: LocationsOrder = Query(
        "lastUpdated", description="Order by a field"
    )
    sort: Optional[Sort] = Query("desc", description="Sort Direction")
    isMobile: Optional[bool] = Query(None, description="Location is mobile")
    isAnalysis: Optional[bool] = Query(
        None,
        description=(
            "Data is the product of a previous "
            "analysis/aggregation and not raw measurements"
        ),
    )
    sourceName: Optional[List[str]] = Query(
        None, description="Name of the data source"
    )
    entity: Optional[EntityTypes] = Query(
        None, description="Source entity type."
    )
    sensorType: Optional[SensorTypes] = Query(
        None, description="Type of Sensor"
    )
    modelName: Optional[List[str]] = Query(
        None, description="Model Name of Sensor"
    )
    manufacturerName: Optional[List[str]] = Query(
        None, description="Manufacturer of Sensor"
    )
    dumpRaw: Optional[bool] = False

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
                        wheres.append(" id = ANY(:location) ")
                    else:
                        wheres.append(" name = ANY(:location) ")
                elif f == "country":
                    wheres.append(" country = ANY(:country) ")
                elif f == "city":
                    wheres.append(" city = ANY(:city) ")
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
        wheres.append(" id not in (61485,61505,61506) ")
        wheres = [w for w in wheres if w is not None]
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get(
    "/v2/locations/{location_id}", response_model=OpenAQResult, tags=["v2"]
)
@router.get("/v2/locations", response_model=OpenAQResult, tags=["v2"])
async def locations_get(
    db: DB = Depends(),
    locations: Locations = Depends(Locations.depends()),
):

    order_by = locations.order_by
    if order_by == "location":
        order_by = "name"
    elif order_by == "count":
        order_by = "measurements"

    if order_by == "random":
        order_by = " random() "
        lastupdateq = """
            AND "lastUpdated" > now() - '2 weeks'::interval AND entity='government'
            """
    else:
        order_by = f'"{order_by}"'
        lastupdateq = ""

    qparams = locations.params()

    hidejson = "rawData,"
    if locations.dumpRaw:
        hidejson = ""

    q = f"""
        WITH t1 AS (
            SELECT
                id,
                name,
                "sensorType",
                entity,
                "isMobile",
                "isAnalysis",
                city,
                country,
                sources,
                manufacturers,
                case WHEN "isMobile" then null else coordinates end as coordinates,
                measurements,
                "firstUpdated",
                "lastUpdated",
                json as "rawData",
                geog,
                bounds,
                parameters,
                row_number() over () as row
            FROM locations_base_v2
            WHERE
            {locations.where()}
            {lastupdateq}
            ORDER BY {order_by} {locations.sort} nulls last
            LIMIT :limit
            OFFSET :offset
        ),
        nodes AS (
            SELECT count(distinct id) as nodes
            FROM locations_base_v2
            WHERE
            {locations.where()}
            {lastupdateq}
        ),
        t2 AS (
        SELECT
        row,
        jsonb_strip_nulls(
            to_jsonb(t1) - '{{{hidejson}source_name,geog, row}}'::text[]
        ) as json
        FROM t1 group by row, t1, json
        )
        SELECT nodes as count, json
        FROM t2, nodes
        ORDER BY row

        ;
        """

    logger.debug(f"**** {qparams}")

    output = await db.fetchOpenAQResult(q, qparams)

    return output


@router.get(
    "/v2/latest/{location_id}", response_model=OpenAQResult, tags=["v2"]
)
@router.get("/v2/latest", response_model=OpenAQResult, tags=["v2"])
async def latest_get(
    db: DB = Depends(), locations: Locations = Depends(Locations.depends()),
):
    data = await locations_get(db, locations)
    meta = data.meta
    res = data.results
    if len(res) == 0:
        return data

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
                        unit: .unit,
                        isVersioned: .isVersioned,
                        isLatest: .isLatest,
                        versionDate: .versionDate,
                        parentSensorsId: .parentSensorsId,
                        lifeCycle: .lifeCycle
                    }
                ]
            }

        """
    )

    ret = latest_jq.input(res).all()
    return OpenAQResult(meta=meta, results=ret)


async def v1_base(
    db: DB = Depends(), locations: Locations = Depends(Locations.depends()),
):
    locations.entity = "government"

    order_by = locations.order_by
    if order_by == "location":
        order_by = "name"
    elif order_by == "count":
        order_by = "measurements"

    if order_by == "random":
        order_by = " random() "
        lastupdateq = """
            AND "lastUpdated" > now() - '2 weeks'::interval
            """
    else:
        order_by = f'"{order_by}"'
        lastupdateq = ""

    qparams = locations.params()

    q = f"""
        WITH t1 AS (
            SELECT *,
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
            {lastupdateq}
            ORDER BY {order_by} {locations.sort} nulls last
            LIMIT :limit
            OFFSET :offset
        ),
        nodes AS (
            SELECT count(distinct id) as nodes
            FROM locations_base_v2

            WHERE
            {locations.where()}
            {lastupdateq}
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

    logger.debug(f"**** {qparams}")

    data = await db.fetchOpenAQResult(q, qparams)
    return data


@router.get(
    "/v1/latest/{location_id}", response_model=OpenAQResult, tags=["v1"]
)
@router.get("/v1/latest", response_model=OpenAQResult, tags=["v1"])
async def latest_v1_get(
    db: DB = Depends(), locations: Locations = Depends(Locations.depends()),
):
    data = await v1_base(db, locations)
    meta = data.meta
    res = data.results
    if len(res) == 0:
        return data

    latest_jq = jq.compile(
        """
        .[] | . as $loc |
            {
                location: .name,
                city: .city,
                country: .country,
                coordinates: .coordinates,
                measurements: [
                    .parameters[] |
                    . as $p | {
                        parameter: .parameter,
                        value: .lastValue,
                        lastUpdated: .lastUpdated,
                        unit: .unit,
                        sourceName: $loc.sourceName,
                        averagingPeriod: {
                            value: $loc.systems[].sensors[]
                                | select(.sensors_id==$p.id)
                                | .data_averaging_period_seconds,
                            unit: "seconds"
                            }
                    }
                ]
            }

        """
    )

    return converter(meta, res, latest_jq)


@router.get(
    "/v1/locations/{location_id}", response_model=OpenAQResult, tags=["v1"]
)
@router.get("/v1/locations", response_model=OpenAQResult, tags=["v1"])
async def locationsv1_get(
    db: DB = Depends(), locations: Locations = Depends(Locations.depends()),
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
