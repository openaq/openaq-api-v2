import logging
from enum import StrEnum
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from openaq_api.models.responses import CitiesResponse, CitiesResponseV1

from ..db import DB
from ..models.queries import APIBase, City, Country

logger = logging.getLogger("cities")

router = APIRouter(deprecated=True)


class CitiesOrder(StrEnum):
    city = "city"
    country = "country"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"


class CitiesOrderV1(StrEnum):
    city = "city"
    country = "country"
    location = "locations"
    count = "count"


class CitiesV1(APIBase):
    order_by: CitiesOrderV1 = Query(
        "city", description="Order by a field e.g. ?order_by=city", examples=["city"]
    )

    country: list[str] | None = Query(
        None,
        min_length=2,
        max_length=2,
        description="Limit results by a certain country using two letter country code. e.g. ?country=US or ?country=US&country=MX",
        examples=["US"],
    )

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                if f == "country":
                    wheres.append(
                        """
                        country = ANY(:country)
                        """
                    )
        return " TRUE "


class Cities(City, Country, APIBase):
    order_by: CitiesOrder = Query(
        "city", description="Order by a field e.g. ?order_by=city", examples=["city"]
    )
    entity: str | None = Query(None)

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
                        country = ANY(:country)
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
async def cities_get(
    cities: Annotated[Cities, Depends(Cities.depends())], db: DB = Depends()
):
    order_by = cities.order_by
    if cities.order_by == "lastUpdated":
        order_by = "8"
    elif cities.order_by == "firstUpdated":
        order_by = "7"
    elif cities.order_by == "country":
        order_by = "country"
    elif cities.order_by == "count":
        order_by = "count"
    elif cities.order_by == "city":
        order_by = "city"
    elif cities.order_by == "locations":
        order_by = "locations"
    q = f"""
        SELECT
            count(*) over () as citiescount,
            c.iso AS country
            , sn.city AS city
            , SUM(sr.value_count) AS "count"
            , COUNT(DISTINCT sn.sensor_nodes_id) AS locations
            , MIN(sr.datetime_first)::TEXT AS first_updated
            , MAX(sr.datetime_last)::TEXT AS last_updated
            , array_agg(DISTINCT m.measurand) AS parameters
            , COUNT(1) OVER() as found
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
        WHERE
        {cities.where()}
        AND city is not null
        AND s.is_public
        GROUP BY c.iso, sn.city
        ORDER BY {order_by} {cities.sort}
        OFFSET :offset
        LIMIT :limit
    """
    params = cities.params()
    output = await db.fetchPage(q, params)

    return output


@router.get(
    "/v1/cities",
    response_model=CitiesResponseV1,
    tags=["v1"],
    summary="Get cities",
    description="Provides a list of cities supported by the platform",
)
async def cities_getv1(
    cities: Annotated[CitiesV1, Depends(CitiesV1.depends())], db: DB = Depends()
):
    order_by = cities.order_by
    if cities.order_by == "country":
        order_by = "country"
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
            , COUNT(1) OVER() as found
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
        AND city is not null
        AND s.is_public
        GROUP BY c.iso, sn.city
        ORDER BY {order_by} {cities.sort}
        OFFSET :offset
        LIMIT :limit
    """
    params = cities.params()
    output = await db.fetchPage(q, params)

    return output
