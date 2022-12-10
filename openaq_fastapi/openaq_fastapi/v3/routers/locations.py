import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from pydantic import root_validator, validator
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import LocationsResponse
from openaq_fastapi.models.queries import OBaseModel, Geo

from openaq_fastapi.v3.models.queries import (
    SQL,
    Paging,
    Radius,
    Bbox,
)

logger = logging.getLogger("locations")

router = APIRouter()


# Needed query parameters

# bbox
# bounding box minx, miny, maxx, maxy
# use the coordinates in sensor nodes

# distance from point (radius)
# point (wgs84) and distance in meters

# provider

# source/owner

# sensor type?

# mobile yes/no
# defaults to not showing mobile locations?

# parameter
# location must have a specific parameter(s)

# city/country


class LocationQueries(Paging, SQL):
    id: int = Query(description="Limit the results to a specific location by id", ge=1)

    def clause(self):
        return "WHERE id = :id"


class LocationsQueries(Paging, SQL, Radius, Bbox):
    mobile: Union[bool, None] = Query(
        description="Is the location considered a mobile location?"
    )

    @root_validator(pre=True)
    def check_bbox_radius_set(cls, values):
        bbox = values.get("bbox", None)
        coordinates = values.get("coordinates", None)
        if bbox is not None and coordinates is not None:
            raise ValueError(
                "Cannot pass both bounding box and coordinate/radius query in the same URL"
            )
        return values

    def fields(self):
        fields = []
        if self.has("coordinates"):
            fields.append(self.fields_distance())
        return ", " + (",").join(fields) if len(fields) > 0 else ""

    def clause(self):
        where = ["WHERE TRUE"]
        if self.has("mobile"):
            where.append("ismobile = :mobile")
        if self.has("coordinates"):
            where.append(self.where_radius())
        if self.has("bbox"):
            where.append(self.where_bbox())
        print(where)
        return ("\nAND ").join(where)


@router.get(
    "/v3/locations/{id}",
    response_model=LocationsResponse,
    summary="Get a location by ID",
    description="Provides a location by location ID",
    tags=["v3"],
)
async def location_get(
    location: LocationQueries = Depends(LocationQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_locations(location, db)
    return response


@router.get(
    "/v3/locations",
    response_model=LocationsResponse,
    summary="Get locations",
    description="Provides a list of locations",
    tags=["v3"],
)
async def locations_get(
    locations: LocationsQueries = Depends(LocationsQueries.depends()),
    db: DB = Depends(),
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
