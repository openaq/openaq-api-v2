import logging
from typing import Annotated, Any
from datetime import date

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

class ParentSensorPath(QueryBaseModel):
    parent_sensors_id: int = Path(
        ..., description="Limit the results to a specific parent sensor", ge=1
    )

    def where(self):
        return "parent_sensors_id = :parent_sensors_id"


class LocationPath(QueryBaseModel):
    location_id: int = Path(
        ..., description="Limit the results to a specific location", ge=1
    )

    def where(self):
        return "sensor_nodes_id = :location_id"


class ParentSensorQuery(QueryBaseModel):
    parent_sensors_id: int | None = Query(None, description='Id of parent sensor')

    def where(self):
        if self.has("parent_sensors_id"):
            return "parent_sensors_id = :parent_sensors_id"


class LocationQuery(QueryBaseModel):
    location_id: int | None = Query(None, description='Location of sensor node')

    def where(self):
        if self.has("location_id"):
            return "sensor_nodes_id = :location_id"


class VersionDateQuery(QueryBaseModel):
    version_date: date | None = Query(None, description='Date of version')

    def where(self):
        if self.has("version_date"):
            return "version_date = :version_date"


class VersionsQueries(ParentSensorQuery, LocationQuery, VersionDateQuery):
    ...

class LocationVersionsQueries(LocationPath, VersionDateQuery):
    ...


class SensorVersionsQueries(ParentSensorPath, VersionDateQuery):
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
    "/locations/{location_id}/versions",
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
    "/sensors/{parent_sensors_id}/versions",
    response_model=VersionsResponse,
    summary="Get sensor verstions by sensor ID",
    description="Provides a list of sensor versions by sensor ID",
)
async def sensors_versions_get(
    sensor_versions: Annotated[
        SensorVersionsQueries, Depends(SensorVersionsQueries.depends())
    ],
    db: DB = Depends(),
):
    return await fetch_versions(sensor_versions, db)



async def fetch_versions(q, db):
    query = QueryBuilder(q)

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
