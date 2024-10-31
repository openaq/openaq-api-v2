import logging
from typing import Annotated, Any
from datetime import datetime, date

from fastapi import APIRouter, Depends, Path, Query
from fastapi.exceptions import RequestValidationError

from pydantic import model_validator

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    Paging,
    QueryBaseModel,
    QueryBuilder,
)

from openaq_api.v3.models.responses import (
    VersionsResponse,
)

logger = logging.getLogger("versions")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
    include_in_schema=True,
)

# all versions
# /versions

# sensor versions for a given location
# /locations/:id/versions

# sensor versions of a given version
# /sensors/:id/versions

class VersionsQueries(QueryBaseModel):
    ...

class LocationVersionsQueries(QueryBaseModel):
    ...

class SensorVersionsQueries(QueryBaseModel):
    ...


@router.get(
    "/versions",
    response_model=VersionsResponse,
    summary="Get sensor versions",
    description="Provides a list of sensor versions",
)
async def versions_get(
    versions: Annotated[
        VersionsQueries, Depends(VersionsQueries.depends())
    ],
    db: DB = Depends(),
):
    return await fetch_versions(versions, db)


@router.get(
    "/locations/{locations_id}/versions",
    response_model=VersionsResponse,
    summary="Get sensor versions by location ID",
    description="Provides a list of sensor versions by location ID",
)
async def location_versions_get(
    location_versions: Annotated[
        LocationVersionsQueries, Depends(LocationVersionsQueries.depends())
    ],
    db: DB = Depends(),
):
    return await fetch_versions(location_versions, db)


@router.get(
    "/sensors/{sensor_id}/versions",
    response_model=VersionsResponse,
    summary="Get sensor verstions by sensor ID",
    description="Provides a list of sensor versions by sensor ID",
)
async def sensor_flags_get(
    sensor_flags: Annotated[
        SensorVersionsQueries, Depends(SensorVersionsQueries.depends())
    ],
    db: DB = Depends(),
):
    return await fetch_versions(sensor_versions, db)



async def fetch_versions(q, db):
    query = QueryBuilder(q)
    query.set_column_map({"timezone": "tz.tzid", "datetime": "lower(period)"})

    sql = f"""
    SELECT versions_id
    , parent_sensor
    , sensor
    , version_date
    , life_cycle
    , version_rank
    FROM versions_view
    {query.where()}
    """
    return await db.fetchPage(sql, query.params())
