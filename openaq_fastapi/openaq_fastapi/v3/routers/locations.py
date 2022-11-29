import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import LocationsResponse
from openaq_fastapi.models.queries import OBaseModel

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

# Does it help to create a SQL class to help a dev
# write queries and not need to worry about how to
# create the total and pagination?
# and if so, how should that be done?
class SQL():
    def pagination(self):
        return "OFFSET :offset\nLIMIT :limit"

    def total(self):
        return ", COUNT(1) OVER() as found"

# Thinking about how the paging should be done
# we should not let folks pass an offset if we also include
# a page parameter. And until pydantic supports computed
# values (v2) we have to calculate the offset ourselves
# see the db.py method
class Paging(OBaseModel):
    limit: int = Query(
        100,
        gt=0,
        description="Change the number of results returned. e.g. limit=1000 will return up to 1000 results",
        example="100"
    )
    page: int = Query(
        1,
        gt=0,
        description="Paginate through results. e.g. page=1 will return first page of results",
        example="1"
    )


class Locationn(Paging, SQL):
    id: int = Query(
        description="Limit the results to a specific location by id",
        ge=1
    )
    def clause(self):
        return "WHERE id = :id"


class Locations(Paging):
    mobile: Union[bool, None] = Query(
        description="Is the location considered a mobile location?"
    )

    def clause(self):
        where = ["WHERE TRUE"]
        if hasattr(self, "mobile") and self.mobile is not None:
            where.append("ismobile = :mobile")
        return ("\nAND ").join(where)


@router.get(
    "/v3/locations/{id}",
    response_model=LocationsResponse,
    summary="Get a location by ID",
    description="Provides a location by location ID",
    tags=["v3"],
)
async def location_get(
        location: Locationn = Depends(Locationn.depends()),
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
        locations: Locations = Depends(Locations.depends()),
        db: DB = Depends()
):
    response = await fetch_locations(locations, db)
    return response


async def fetch_locations(where, db):
    sql = f"""
    SELECT id
    , name
    , ismobile as "isMobile"
    , ismonitor as "isMonitor"
    , city
    , country
    , owner
    , coordinates
    , instruments
    , parameters
    {where.total()}
    FROM locations_view_m
    {where.clause()}
    {where.pagination()}
    """
    response = await db.fetchPage(sql, where.params())
    return response
