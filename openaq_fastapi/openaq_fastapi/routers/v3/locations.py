from fastapi import APIRouter, Depends
from openaq_fastapi.db import DB
from openaq_fastapi.models.v3.responses import LocationsResponse


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


@router.get(
    "/v3/locations/{location_id}",
    response_model=LocationsResponse,
    summary="Get a location by ID",
    description="Provides a location by location ID",
    tags=["v3"],
)
@router.get(
    "/v3/locations",
    response_model=LocationsResponse,
    summary="Get locations",
    description="Provides a list of locations",
    tags=["v3"],
)
async def locations_get(
        db: DB = Depends()
):
    results = []
    return LocationsResponse(meta=[], data=results)
