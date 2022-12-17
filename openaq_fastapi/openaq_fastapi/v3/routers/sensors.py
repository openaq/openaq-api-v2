import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.routers.measurements import fetch_measurements
from openaq_fastapi.v3.models.responses import SensorsResponse, MeasurementsResponse

from openaq_fastapi.v3.models.queries import Paging, QueryBaseModel, CountryQuery

logger = logging.getLogger("sensors")

router = APIRouter(prefix="/v3", tags=["v3"])


class SensorQuery(QueryBaseModel):
    sensors_id: int = Path(
        description="Limit the results to a specific sensors id", ge=1
    )

    def where(self):
        return "WHERE sensors_id = :sensors_id"


class SensorsQueries(Paging, CountryQuery):
    ...


@router.get(
    "/sensors/{sensors_id}",
    response_model=SensorsResponse,
    summary="Get a sensor by ID",
    description="Provides a sensor by sensor ID",
)
async def sensor_get(
    sensor: SensorQuery = Depends(SensorQuery.depends()),
    db: DB = Depends(),
):
    response = await fetch_sensors(sensor, db)
    return response


@router.get(
    "/sensors/{id}/measurements",
    response_model=MeasurementsResponse,
    summary="Get measurements by sensor ID",
    description="Provides a list of measurements by sensor ID",
)
async def sensor_measurements_get(
    sensor: SensorsQueries = Depends(SensorsQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_measurements(sensor, db)
    return response


async def fetch_sensors(where, db):
    sql = f"""
    """
    response = await db.fetchPage(sql, where.params())
    return response
