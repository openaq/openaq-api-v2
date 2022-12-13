import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import SensorsResponse, MeasurementsReponse
from openaq_fastapi.models.queries import OBaseModel, Geo

from openaq_fastapi.v3.models.queries import (
    SQL,
    Paging,
)

logger = logging.getLogger("sensors")

router = APIRouter(prefix="/v3", tags=["v3"])


class SensorsQueries(Paging, SQL):
    def fields(self):
        fields = []
        return ", " + (",").join(fields) if len(fields) > 0 else ""

    def clause(self):
        where = ["WHERE TRUE"]
        return ("\nAND ").join(where)


@router.get(
    "/sensors/{id}",
    response_model=SensorsResponse,
    summary="Get a sensor by ID",
    description="Provides a sensor by sensor ID",
)
async def sensor_get(
    sensor: SensorsQueries = Depends(SensorsQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_sensors(sensor, db)
    return response


@router.get(
    "/sensors/{id}/measurements",
    response_model=MeasurementsReponse,
    summary="Get measurements by sensor ID",
    description="Provides a list of measurements by sensor ID",
)
async def sensor_measurements_get(
    sensor: SensorsQueries = Depends(SensorsQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_sensor_measurements(id, sensor, db)
    return response


async def fetch_sensors(where, db):
    sql = f"""
    """
    response = await db.fetchPage(sql, where.params())
    return response


async def fetch_sensor_measurements(id, where, db):
    sql = f"""
    """
    response = await db.fetchPage(sql, where.params())
    return response
