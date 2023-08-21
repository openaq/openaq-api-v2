import logging
from typing import Annotated
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.routers.measurements import fetch_measurements

from openaq_fastapi.v3.models.responses import (
    SensorsResponse,
    MeasurementsResponse,
)

from openaq_fastapi.v3.models.queries import (
    Paging,
    QueryBaseModel,
    QueryBuilder,
)

logger = logging.getLogger("sensors")

router = APIRouter(
    prefix="/v3",
    tags=["v3-alpha"],
    include_in_schema=True,
)


class SensorQuery(QueryBaseModel):
    sensors_id: int = Path(
        ..., description="Limit the results to a specific sensors id", ge=1
    )

    def where(self):
        return "m.sensors_id = :sensors_id"


# class SensorsQueries(Paging, CountryQuery):
#     ...


# @router.get(
#     "/sensors/{id}/measurements",
#     response_model=MeasurementsResponse,
#     summary="Get measurements by sensor ID",
#     description="Provides a list of measurements by sensor ID",
# )
# async def sensor_measurements_get(
#     sensor: SensorsQueries = Depends(SensorsQueries.depends()),
#     db: DB = Depends(),
# ):
#     response = await fetch_measurements(sensor, db)
#     return response


@router.get(
    "/sensors/{sensors_id}",
    response_model=SensorsResponse,
    summary="Get a sensor by ID",
    description="Provides a sensor by sensor ID",
)
async def sensor_get(
    sensors: Annotated[SensorQuery, Depends(SensorQuery.depends())],
    db: DB = Depends(),
):
    response = await fetch_sensors(sensors, db)
    return response


async def fetch_sensors(q, db):
    query = QueryBuilder(q)

    sql = f"""
        WITH sensor AS (
        SELECT
        m.sensors_id
        , MIN(datetime - '1sec'::interval) as datetime_first
        , MAX(datetime - '1sec'::interval) as datetime_last
        , COUNT(1) as value_count
        , AVG(value_avg) as value_avg
        , STDDEV(value_avg) as value_sd
        , MIN(value_avg) as value_min
        , MAX(value_avg) as value_max
        , PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value_avg) as value_p02
        , PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value_avg) as value_p25
        , PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value_avg) as value_p50
        , PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value_avg) as value_p75
        , PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value_avg) as value_p98
        , current_timestamp as calculated_on
        FROM hourly_data m
        {query.where()}
        GROUP BY 1)
        SELECT c.sensors_id as id
        , 'sensor' as name
        , c.value_avg as value
        , get_datetime_object(c.datetime_first, ts.tzid) as datetime_first
        , get_datetime_object(c.datetime_last, ts.tzid) as datetime_last
        , json_build_object(
            'datetime', get_datetime_object(r.datetime_last, ts.tzid)
            , 'value', r.value_latest
            , 'coordinates', jsonb_build_object(
                'lat', st_y(r.geom_latest)
            , 'lon', st_x(r.geom_latest)
            )
        ) as latest
        , json_build_object(
            'id', s.measurands_id
            , 'units', m.units
            , 'name', m.measurand
            , 'display_name', m.display
        ) as parameter
        , json_build_object(
            'sd', c.value_sd
        , 'min', c.value_min
        , 'q02', c.value_p02
        , 'q25', c.value_p25
        , 'median', c.value_p50
        , 'q75', c.value_p75
        , 'q98', c.value_p98
        , 'max', c.value_max
        ) as summary
        , calculate_coverage(
            c.value_count::int
        , s.data_averaging_period_seconds
        , s.data_logging_period_seconds
        , EXTRACT(EPOCH FROM c.datetime_last - c.datetime_first)
        ) as coverage
        FROM sensors s
        JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
        JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
        JOIN timezones ts ON (sn.timezones_id = ts.gid)
        JOIN measurands m ON (s.measurands_id = m.measurands_id)
        LEFT JOIN sensors_rollup r ON (s.sensors_id = r.sensors_id)
        LEFT JOIN sensor c ON (c.sensors_id = s.sensors_id)
        WHERE s.sensors_id = :sensors_id;
    """
    response = await db.fetchPage(sql, query.params())
    return response
