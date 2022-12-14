import logging
from typing import Union
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from pydantic import ValidationError, root_validator, validator
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.queries import make_dependable
from openaq_fastapi.v3.models.responses import LocationsResponse

from openaq_fastapi.v3.models.queries import (
    QueryBaseModel,
    LocationsQueries,
)

logger = logging.getLogger("locations")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
)

# Needed query parameters


# source/owner

# parameter
# location must have a specific parameter(s)

# city/country


class LocationQueries(QueryBaseModel):
    id: int = Query(
        description="Limit the results to a specific location by id",
        ge=1
    )



@router.get(
    "/locations/{id}",
    response_model=LocationsResponse,
    summary="Get a location by ID",
    description="Provides a location by location ID",
)
async def location_get(
    location: LocationQueries = Depends(make_dependable(LocationQueries)),
    db: DB = Depends(),
):
    response = await fetch_locations(location, db)
    return response


@router.get(
    "/locations",
    response_model=LocationsResponse,
    summary="Get locations",
    description="Provides a list of locations",
)
async def locations_get(
    locations: LocationsQueries = Depends(LocationsQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_locations(locations, db)
    return response


async def fetch_locations(query, db):
    sql = f"""
    SELECT id
    , name
    , ismobile as is_mobile
    , ismonitor as is_monitor
    , city as locality
    , country
    , owner
    , provider
    , coordinates
    , instruments
    , sensors
    , timezone
    , bbox(geom) as bounds
    , datetime_first
    , datetime_last
    {query.fields() or ''}
    {query.total()}
    FROM locations_view_m
    {query.where()}
    {query.pagination()}
    """
    response = await db.fetchPage(sql, query.params())
    return response
