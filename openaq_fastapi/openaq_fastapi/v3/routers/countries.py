import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import LocationsResponse
from openaq_fastapi.models.queries import OBaseModel

logger = logging.getLogger("locations")

router = APIRouter()
"""

@router.get(
    "/v3/countries/{id}",
    response_model=LocationsResponse,
    summary="Get a country by ID",
    description="Provides a country by country ID",
    tags=["v3"],
)
async def country_get(
    location: Locationn = Depends(Locationn.depends()),
    db: DB = Depends(),
):
    response = await fetch_countries(location, db)
    return response


@router.get(
    "/v3/countries",
    response_model=LocationsResponse,
    summary="Get countries",
    description="Provides a list of locations",
    tags=["v3"],
)
async def countries_get(
    locations: Locations = Depends(Locations.depends()), db: DB = Depends()
):
    response = await fetch_countries(locations, db)
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
"""