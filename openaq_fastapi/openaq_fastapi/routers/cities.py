import logging
from enum import Enum
from typing import Union

from fastapi import APIRouter, Depends, Query
from openaq_fastapi.models.responses import CitiesResponse, CitiesResponseV1
import jq
from ..db import DB
from ..models.queries import APIBase, City, Country

logger = logging.getLogger("cities")

router = APIRouter()


class CitiesOrder(str, Enum):
    city = "city"
    country = "country"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"


class Cities(City, Country, APIBase):
    order_by: CitiesOrder = Query(
        "city", description="Order by a field e.g. ?order_by=city", example="city"
    )
    entity: Union[str, None] = None

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                if f == "city":
                    wheres.append(
                        """
                        city = ANY(:city)
                        """
                    )
                elif f == "country":
                    wheres.append(
                        """
                        code = ANY(:country)
                        """
                    )
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get(
    "/v2/cities",
    response_model=CitiesResponse,
    summary="Get cities",
    description="Provides a list of cities supported by the platform",
    tags=["v2"],
)
async def cities_get(db: DB = Depends(), cities: Cities = Depends(Cities.depends())):
    order_by = cities.order_by
    if cities.order_by == "lastUpdated":
        order_by = "8"
    elif cities.order_by == "firstUpdated":
        order_by = "7"
    elif cities.order_by == "country":
        order_by = "code"
    elif cities.order_by == "count":
        order_by = "count"
    elif cities.order_by == "city":
        order_by = "city"
    elif cities.order_by == "locations":
        order_by = "locations"
    q = f"""
    WITH t AS (
    SELECT
    count(*) over () as citiescount,
        code as country,
        city,
        count,
        locations,
        "firstUpdated",
        "lastUpdated",
        parameters
    FROM city_stats
    WHERE
    {cities.where()}
    and city is not null
    ORDER BY {order_by} {cities.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT citiescount as count, to_jsonb(t)-'{{citiescount}}'::text[] as json FROM t
    """
    params = cities.params()
    output = await db.fetchOpenAQResult(q, params)

    return output


@router.get(
    "/v1/cities",
    response_model=CitiesResponseV1,
    tags=["v1"],
    summary="Get cities",
    description="Provides a list of cities supported by the platform",
)
async def cities_getv1(
    db: DB = Depends(),
    cities: Cities = Depends(Cities.depends()),
):
    order_by = cities.order_by
    if cities.order_by == "lastUpdated":
        order_by = "8"
    elif cities.order_by == "firstUpdated":
        order_by = "7"
    elif cities.order_by == "country":
        order_by = "code"
    elif cities.order_by == "count":
        order_by = "count"
    elif cities.order_by == "city":
        order_by = "city"
    elif cities.order_by == "locations":
        order_by = "locations"
    q = f"""
        SELECT 
            count(*) over () as citiescount
            , c.iso AS country
            , sn.city AS "name"
            , sn.city AS city
            , SUM(sr.value_count) AS "count"
            , COUNT(DISTINCT sn.sensor_nodes_id) AS locations
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
        WHERE
        {cities.where()}
        and city is not null
        GROUP BY c.iso, sn.city
        ORDER BY {order_by} {cities.sort}
        OFFSET :offset
        LIMIT :limit
    """
    params = cities.params()
    output = await db.fetchPage(q, params)

    return output
