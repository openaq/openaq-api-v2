import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.openaq_fastapi.v3.routers.locations import SQL, Paging
from openaq_fastapi.v3.models.responses import LocationsResponse, CountriesResponse
from openaq_fastapi.models.queries import OBaseModel

logger = logging.getLogger("locations")

router = APIRouter()


class countryy(Paging, SQL):
    id: int = Query(description="Limit the results to a specific country by id", ge=1)

    def clause(self):
        return "WHERE id = :id"


class countries(Paging, SQL):
    ...


@router.get(
    "/v3/countries/{id}",
    response_model=LocationsResponse,
    summary="Get a country by ID",
    description="Provides a country by country ID",
    tags=["v3"],
)
async def country_get(
    country: countryy = Depends(countryy.depends()),
    db: DB = Depends(),
):
    country.id = id
    response = await fetch_countries(country, db)
    return response


@router.get(
    "/v3/countries",
    response_model=CountriesResponse,
    summary="Get countries",
    description="Provides a list of countries",
    tags=["v3"],
)
async def countries_get(
    countries: Countries = Depends(Countries.depends()), db: DB = Depends()
):
    response = await fetch_countries(countries, db)
    return response


async def fetch_countries(where, db):
    sql = f"""
    SELECT id
    , code
    , name
    {where.total()}
    FROM countries
    {where.clause()}
    {where.pagination()}
    """
    response = await db.fetchPage(sql, where.params())
    return response
