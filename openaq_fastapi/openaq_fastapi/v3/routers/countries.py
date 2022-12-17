import logging
from typing import List, Union
from fastapi import APIRouter, Depends, Path, Query
from pydantic import root_validator
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import (
    LocationsResponse,
    CountriesResponse,
)

from openaq_fastapi.v3.models.queries import (
    QueryBaseModel,
    Paging,
    RadiusQuery,
    BboxQuery,
    QueryBuilder,
    ProviderQuery,
    OwnerQuery,
    MobileQuery,
)

from openaq_fastapi.v3.routers.locations import fetch_locations


logger = logging.getLogger("countries")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
)


class CountryPathQuery(QueryBaseModel):
    countries_id: int = Path(
        description="Limit the results to a specific country by id",
        ge=1,
    )

    def where(self) -> str:
        return "id = :countries_id"


class CountriesQueries(QueryBaseModel, Paging):
    ...


class LocationCountryPathQuery(QueryBaseModel):

    countries_id: int = Path(
        description="Limit the results to a specific country",
    )

    def where(self) -> str:
        return "(country->'id')::int = :countries_id"


class CountryLocationsQueries(
    Paging,
    LocationCountryPathQuery,
    RadiusQuery,
    BboxQuery,
    ProviderQuery,
    OwnerQuery,
    MobileQuery,
):
    ...


@router.get(
    "/countries/{countries_id}",
    response_model=CountriesResponse,
    summary="Get a country by ID",
    description="Provides a country by country ID",
)
async def country_get(
    country: CountryPathQuery = Depends(CountryPathQuery),
    db: DB = Depends(),
):
    response = await fetch_countries(country, db)
    return response


@router.get(
    "/countries/{countries_id}/locations",
    response_model=LocationsResponse,
    summary="Get locations within a country",
    description="Provides a country by country ID",
)
async def country_locations_get(
    locations: CountryLocationsQueries = Depends(CountryLocationsQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_locations(locations, db)
    return response


@router.get(
    "/countries",
    response_model=CountriesResponse,
    summary="Get countries",
    description="Provides a list of countries",
)
async def countries_get(
    countries: CountriesQueries = Depends(CountriesQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_countries(countries, db)
    return response


async def fetch_countries(query, db):
    query_builder = QueryBuilder(query)
    sql = f"""
    SELECT id
    , code
    , name
    , datetime_first
    , datetime_last
    , parameters
    , locations_count
    , measurements_count
    , providers_count
    {query_builder.total()}
    FROM countries_view_cached
    {query_builder.where()}
    {query_builder.pagination()}
    """
    print(sql)
    response = await db.fetchPage(sql, query_builder.params())
    return response
