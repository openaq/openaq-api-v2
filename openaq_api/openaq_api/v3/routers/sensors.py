import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    QueryBaseModel,
    QueryBuilder,
)

from openaq_api.v3.models.responses import (
    SensorsResponse,
)

logger = logging.getLogger("sensors")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
    include_in_schema=True,
)


class SensorPathQuery(QueryBaseModel):
    sensors_id: int = Path(
        ..., description="Limit the results to a specific sensors id", ge=1
    )

    def where(self):
        return "s.sensors_id = :sensors_id"


class LocationSensorQuery(QueryBaseModel):
    locations_id: int = Path(
        ..., description="Limit the results to a specific sensors id", ge=1
    )

    def where(self):
        return "n.sensor_nodes_id = :locations_id"


@router.get(
    "/locations/{locations_id}/sensors",
    response_model=SensorsResponse,
    summary="Get sensors by location ID",
    description="Provides a list of sensors by location ID",
)
async def sensors_get(
    location_sensors: Annotated[
        LocationSensorQuery, Depends(LocationSensorQuery.depends())
    ],
    db: DB = Depends(),
):
    return await fetch_sensors(location_sensors, db)


@router.get(
    "/sensors/{sensors_id}",
    response_model=SensorsResponse,
    summary="Get a sensor by ID",
    description="Provides a sensor by sensor ID",
)
async def sensor_get(
    sensors: Annotated[SensorPathQuery, Depends(SensorPathQuery.depends())],
    db: DB = Depends(),
):
    response = await fetch_sensors(sensors, db)
    if len(response.results) == 0:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return response


async def fetch_sensors(q, db):
    query = QueryBuilder(q)

    logger.debug(query.params())
    sql = f"""
    SELECT s.sensors_id as id
    , m.measurand||' '||m.units as name
    , json_build_object(
    'id', m.measurands_id
    , 'name', m.measurand
    , 'units', m.units
    , 'display_name', m.display
    ) as parameter
    , s.sensors_id
    , CASE 
        WHEN r.value_latest IS NOT NULL THEN
    json_build_object(
      'min', r.value_min
    , 'max', r.value_max
    , 'avg', r.value_avg
    , 'sd', r.value_sd
    ) 
    ELSE NULL
    END as summary
    , CASE 
        WHEN r.value_latest IS NOT NULL THEN
      jsonb_build_object(
        'datetime_from', get_datetime_object(r.datetime_first, t.tzid),
        'datetime_to', get_datetime_object(r.datetime_last, t.tzid)
      ) || calculate_coverage(
        r.value_count,
        s.data_averaging_period_seconds,
        s.data_logging_period_seconds
      )::jsonb
    ELSE NULL
    END as coverage
    , get_datetime_object(r.datetime_first, t.tzid) as datetime_first
    , get_datetime_object(r.datetime_last, t.tzid) as datetime_last
    ,CASE 
        WHEN r.value_latest IS NOT NULL THEN
     json_build_object(
       'datetime', get_datetime_object(r.datetime_last, t.tzid)
      , 'value', r.value_latest
      , 'coordinates', json_build_object(
                'latitude', st_y(COALESCE(r.geom_latest, n.geom))
                ,'longitude', st_x(COALESCE(r.geom_latest, n.geom))
    )) 
    ELSE NULL
    END as latest
    FROM sensors s
    JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
    JOIN sensor_nodes n ON (sy.sensor_nodes_id = n.sensor_nodes_id)
    JOIN timezones t ON (n.timezones_id = t.timezones_id)
    JOIN measurands m ON (s.measurands_id = m.measurands_id)
    LEFT JOIN sensors_rollup r ON (s.sensors_id = r.sensors_id)
    {query.where()} AND n.is_public AND s.is_public
    {query.pagination()}
    """
    return await db.fetchPage(sql, query.params())
