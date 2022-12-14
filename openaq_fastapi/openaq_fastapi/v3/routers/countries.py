import logging
from fastapi import APIRouter, Depends, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import (
    LocationsResponse,
    CountriesResponse,
)

from openaq_fastapi.v3.models.queries import (
    QueryBaseModel,
    Paging,
    LocationsQueries,
)

from openaq_fastapi.v3.routers.locations import fetch_locations


logger = logging.getLogger("countries")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
)


class CountryQueries(QueryBaseModel):
    id: int = Path(
        description="Limit the results to a specific country by id",
        ge=1,
    )


class CountriesQueries(Paging):
    ...


@router.get(
    "/countries/{id}",
    response_model=CountriesResponse,
    summary="Get a country by ID",
    description="Provides a country by country ID",
)
async def country_get(
    country: CountryQueries = Depends(CountryQueries.depends()),
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
    locations: LocationsQueries = Depends(LocationsQueries.depends()),
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
    {query.total()}
    FROM countries_view_m
    {query.where()}
    {query.pagination()}
    """
    response = await db.fetchPage(sql, query.params())
    return response
