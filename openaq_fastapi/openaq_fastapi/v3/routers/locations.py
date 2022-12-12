import logging
from typing import Union
from fastapi import APIRouter, Depends, Query
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import LocationsResponse

from openaq_fastapi.v3.models.queries import (
    SQL,
    Paging,
    RadiusQuery,
    BboxQuery,
    ProviderQuery,
    OwnerQuery,
    CountryQuery,
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


class LocationQueries(SQL):
    id: int = Query(
        description="Limit the results to a specific location by id",
        ge=1
    )

    def clause(self):
        return "WHERE id = :id"


class LocationsQueries(
        Paging,
        SQL,
        RadiusQuery,
        BboxQuery,
        ProviderQuery,
        OwnerQuery,
        CountryQuery,
):
    mobile: Union[bool, None] = Query(
        description="Is the location considered a mobile location?"
    )

    monitor: Union[bool, None] = Query(
        description="Is the location considered a reference monitor?"
    )

    def fields(self):
        fields = []
        if self.has('coordinates'):
            fields.append(RadiusQuery.fields(self))
        return ', '+(',').join(fields) if len(fields) > 0 else ''

    def clause(self):
        where = ["WHERE TRUE"]
        if self.has('mobile'):
            where.append("ismobile = :mobile")
        if self.has('mobile'):
            where.append("ismonitor = :monitor")
        if self.has('coordinates'):
            where.append(RadiusQuery.where(self))
        if self.has('bbox'):
            where.append(BboxQuery.where(self))
        if self.has('providers_id'):
            where.append(ProviderQuery.where(self))
        return ("\nAND ").join(where)


@router.get(
    "/locations/{id}",
    response_model=LocationsResponse,
    summary="Get a location by ID",
    description="Provides a location by location ID",
)
async def location_get(
        location: LocationQueries = Depends(LocationQueries.depends()),
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
        db: DB = Depends()
):
    response = await fetch_locations(locations, db)
    return response


async def fetch_locations(where, db):
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
    {where.fields()}
    {where.total()}
    FROM locations_view_m
    {where.clause()}
    {where.pagination()}
    """
    response = await db.fetchPage(sql, where.params())
    return response
