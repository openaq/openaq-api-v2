import logging
from enum import StrEnum
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from openaq_api.models.responses import CountriesResponse, CountriesResponseV1

from ..db import DB
from ..models.queries import APIBase, Country, CountryByPath

logger = logging.getLogger("countries")

router = APIRouter()


class CountriesOrder(StrEnum):
    code = "code"
    name = "name"
    locations = "locations"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"
    parameters = "parameters"
    count = "count"
    cities = "cities"
    sources = "sources"


class CountriesOrderV1(StrEnum):
    code = "code"
    count = "count"
    locations = "locations"
    cities = "cities"
    name = "name"


class CountriesV1(APIBase):
    order_by: CountriesOrderV1 = Query(
        "code", description="Order by a field e.g. ?order_by=code", examples=["code"]
    )
    limit: int = Query(
        100,
        description="Limit the number of results returned. e.g. limit=100 will return up to 100 results",
        examples=["100"],
    )

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                if f == "country":
                    wheres.append(
                        """
                        c.iso = ANY(:country)
                        """
                    )
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


class Countries(Country, APIBase):
    order_by: CountriesOrder = Query(
        "name", description="Order by a field e.g. ?order_by=name", examples=["name"]
    )
    limit: int = Query(
        100,
        description="Limit the number of results returned. e.g. limit=100 will return up to 100 results",
        examples=["100"],
    )

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                if f == "country":
                    wheres.append(
                        """
                        c.iso = ANY(:country)
                        """
                    )
                elif f == "country_id":
                    wheres.append(
                        """
                        c.countries_id = :country_id
                        """
                    )
        if len(wheres) > 0:
            return (" OR ").join(wheres)
        return " TRUE "


class CountriesPath(CountryByPath, APIBase):
    order_by: CountriesOrder = Query(
        "name", description="Order by a field e.g. ?order_by=name", examples=["name"]
    )
    limit: int = Query(
        100,
        description="Limit the number of results returned. e.g. limit=100 will return up to 100 results",
        examples=["100"],
    )


@router.get(
    "/v1/countries/{country_id}",
    response_model=CountriesResponse,
    summary="Get country by ID",
    description="Provides a single country by country ID",
    tags=["v1"],
)
@router.get(
    "/v2/countries/{country_id}",
    response_model=CountriesResponse,
    summary="Get country by ID",
    description="Provides a single country by country ID",
    tags=["v2"],
)
async def countries_by_path(
    countries: Annotated[CountriesPath, Depends(CountriesPath.depends())],
    db: DB = Depends(),
):
    q = f"""
        SELECT
        c.iso AS code
        , c.countries_id AS country_id
        , c.name
        , COUNT(DISTINCT sn.sensor_nodes_id) AS locations
        , MIN(sr.datetime_first)::TEXT AS first_updated
        , MAX(sr.datetime_last)::TEXT AS last_updated
        , array_agg(DISTINCT m.measurand) AS parameters
        , SUM(sr.value_count) AS "count"
        , count (DISTINCT sn.city) AS cities
        , count (DISTINCT p.source_name) AS sources
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
        WHERE
        c.iso IS NOT NULL
        AND c.countries_id = :country_id
        GROUP BY code, c.name, c.countries_id
        OFFSET :offset
        LIMIT :limit
        """

    params = countries.params()
    params["country_id"] = countries.country_id
    output = await db.fetchPage(q, params)

    return output


@router.get(
    "/v2/countries",
    response_model=CountriesResponse,
    summary="Get countries",
    description="Provides a list of countries",
    tags=["v2"],
)
async def countries_get(
    countries: Annotated[Countries, Depends(Countries)],
    db: DB = Depends(),
):
    order_by = countries.order_by
    if countries.order_by == "lastUpdated":
        order_by = "8"
    elif countries.order_by == "code":
        order_by = "code"
    elif countries.order_by == "firstUpdated":
        order_by = "7"
    elif countries.order_by == "country":
        order_by = "country"
    elif countries.order_by == "count":
        order_by = "count"
    elif countries.order_by == "locations":
        order_by = "locations"
    elif countries.order_by == "name":
        order_by = "name"

    q = f"""
        SELECT
        c.iso AS code
        , c.name
        , COUNT(DISTINCT sn.sensor_nodes_id) AS locations
        , MIN(sr.datetime_first)::TEXT AS first_updated
        , MAX(sr.datetime_last)::TEXT AS last_updated
        , array_agg(DISTINCT m.measurand) AS parameters
        , SUM(sr.value_count) AS "count"
        , count (DISTINCT sn.city) AS cities
        , count (DISTINCT p.source_name) AS sources
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
        WHERE
        {countries.where()}
        AND c.iso IS NOT NULL
        GROUP BY code, c.name
        ORDER BY {order_by} {countries.sort}
        OFFSET :offset
        LIMIT :limit
        """

    params = countries.params()
    output = await db.fetchPage(q, params)

    return output


@router.get(
    "/v1/countries",
    response_model=CountriesResponseV1,
    summary="Get countries",
    description="Providecs a list of countries",
    tags=["v1"],
)
async def countries_getv1(
    countries: Annotated[CountriesV1, Depends(CountriesV1)],
    db: DB = Depends(),
):
    order_by = countries.order_by
    if countries.order_by == "code":
        order_by = "code"
    elif countries.order_by == "count":
        order_by = "count"
    elif countries.order_by == "cities":
        order_by = "cities"
    elif countries.order_by == "locations":
        order_by = "locations"
    elif countries.order_by == "name":
        order_by = "name"

    q = f"""
        SELECT
	  c.iso AS code
        , c.name
        , count (DISTINCT sn.city) AS cities
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
        {countries.where()}
        AND
        c.iso IS NOT NULL
        GROUP BY code, c.name
        ORDER BY {order_by} {countries.sort}
        OFFSET :offset
        LIMIT :limit
        """
    params = countries.params()
    output = await db.fetchPage(q, params)

    return output
