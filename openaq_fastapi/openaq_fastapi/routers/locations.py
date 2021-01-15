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
    SourceTypes,
)

from openaq_fastapi.models.responses import (
    OpenAQResult,
)

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

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
    sourceName: Optional[List[str]] = Query(
        None, description="Name of the data source"
    )
    entity: Optional[List[EntityTypes]] = Query(
        None, description="Source entity type."
    )
    sensorType: Optional[List[SourceTypes]] = Query(
        None, description="Type of Sensor"
    )
    modelName: Optional[List[str]] = Query(
        None, description="Model Name of Sensor"
    )
    manufacturerName: Optional[List[str]] = Query(
        None, description="Manufacturer of Sensor"
    )

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
                        entity = ANY(:entity)
                        """
                    )
                elif f == "sensorType":
                    wheres.append(
                        """
                        "sensorType" = ANY(:sensor_type)
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
                elif f == "unit":
                    wheres.append(
                        """
                            parameters @> ANY(
                                jsonb_array_query('unit',:unit::text[])
                                )
                            """
                    )
        wheres.append(self.where_geo())
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
            AND "lastUpdated" > now() - '2 weeks'::interval
            """
    else:
        order_by = f'"{order_by}"'
        lastupdateq = ""

    qparams = locations.params()

    q = f"""
        WITH t1 AS (
            SELECT *, row_number() over () as row
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
            to_jsonb(t1) - '{{json,source_name,geog, row}}'::text[]
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
    "/v1/latest/{location_id}", response_model=OpenAQResult, tags=["v1"]
)
@router.get(
    "/v2/latest/{location_id}", response_model=OpenAQResult, tags=["v2"]
)
@router.get("/v1/latest", response_model=OpenAQResult, tags=["v1"])
@router.get("/v2/latest", response_model=OpenAQResult, tags=["v2"])
async def latest_get(
    db: DB = Depends(),
    locations: Locations = Depends(Locations.depends()),
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
                        parameter: .measurand,
                        value: .lastValue,
                        lastUpdated: .lastUpdated,
                        unit: .unit
                    }
                ]
            }

        """
    )

    ret = latest_jq.input(res).all()
    return OpenAQResult(meta=meta, results=ret)


@router.get(
    "/v1/locations/{location_id}", response_model=OpenAQResult, tags=["v1"]
)
@router.get("/v1/locations", response_model=OpenAQResult, tags=["v1"])
async def locationsv1_get(
    db: DB = Depends(),
    locations: Locations = Depends(Locations.depends()),
):
    data = await locations_get(db, locations)
    meta = data.meta
    res = data.results

    latest_jq = jq.compile(
        """
        .[] |
            {
                id: .id,
                country: .country,
                city: .city,
                location: .name,
                soureName: .source_name,
                sourceType: .sources[0].name,
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

    ret = latest_jq.input(res).all()
    return OpenAQResult(meta=meta, results=ret)
