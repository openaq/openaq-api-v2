from fastapi import APIRouter, Depends, Path, Query
from datetime import date, datetime
from openaq_fastapi.db import DB
from typing import Union, List
from pydantic import Field
from openaq_fastapi.v3.models.responses import (
    MeasurementsResponse,
    JsonBase,
    DatetimeObject,
    OpenAQResult,
)
from openaq_fastapi.v3.models.queries import (
    QueryBaseModel,
    QueryBuilder,
    Paging,
)

router = APIRouter(
    prefix="/v3",
    tags=["v3"]
)


class DateFromQuery(QueryBaseModel):
    date_from: Union[datetime, date] = Query(
        description="From when?"
    )

    def where(self) -> str:
        return "datetime > :date_from"


class DateToQuery(QueryBaseModel):
    date_to: Union[datetime, date] = Query(
        description="To when?"
    )

    def where(self) -> str:
        return "datetime <= :date_to"


class LocationPathQuery(QueryBaseModel):
    locations_id: int = Path(
        description="Limit the results to a specific location by id", ge=1
    )

    def where(self) -> str:
        return "sy.sensor_nodes_id = :locations_id"


class LocationMeasurementsQueries(
        Paging,
        LocationPathQuery,
        DateFromQuery,
        DateToQuery,
):
    ...


@router.get(
    "/locations/{locations_id}/measurements",
    response_model=MeasurementsResponse,
    summary="Get measurements by location",
    description="Provides a list of measurements by location ID",
)
async def measurements_get(
    measurements: LocationMeasurementsQueries = Depends(
        LocationMeasurementsQueries.depends()
    ),
    db: DB = Depends(),
):
    response = await fetch_measurements(measurements, db)
    return response


async def fetch_measurements(q, db):
    query = QueryBuilder(q)

    if True:
        # Query for hourly data
        sql = f"""
        SELECT sy.sensor_nodes_id as id
        , json_build_object(
        'label', '1hour'
        , 'datetime_from', get_datetime_object(h.datetime, 'utc')
        , 'datetime_to', get_datetime_object(h.datetime, 'utc')
        , 'interval',  '01:00:00'
        ) as period
        , json_build_object(
        'id', h.measurands_id
        , 'units', m.units
        , 'name', m.measurand
        ) as parameter
        , json_build_object(
          'sd', h.value_sd
        , 'min', h.value_min
        , 'q02', h.value_p02
        , 'q25', h.value_p25
        , 'median', h.value_p50
        , 'q75', h.value_p75
        , 'q98', h.value_p98
        , 'max', h.value_max
        ) as summary
        , h.value_avg as value
        , calculate_coverage(
        h.value_count
        , s.data_averaging_period_seconds
        , s.data_logging_period_seconds
        , 3600
        ) as coverage
        {query.fields()}
        {query.total()}
        FROM hourly_rollups h
        JOIN sensors s USING (sensors_id)
        JOIN sensor_systems sy USING (sensor_systems_id)
        JOIN measurands m ON (m.measurands_id = h.measurands_id)
        {query.where()}
        {query.pagination()}
        """
    else:
        # Query for the aggregate data
        interval = 'year'
        seconds = 24*365*3600
        sql = f"""
    WITH measurements AS (
      SELECT
      m.sensors_id
      , s.measurands_id
      , date_trunc('{interval}', datetime) as datetime
      , MIN(datetime) as first_datetime
      , MAX(datetime) as last_datetime
      , COUNT(1) as value_count
      , AVG(value_avg) as value_avg
      , STDDEV(value_avg) as value_sd
      , MIN(value_avg) as value_min
      , MAX(value_avg) as value_max
      , PERCENTILE_CONT(0.05) WITHIN GROUP(ORDER BY value_avg) as value_p05
      , PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value_avg) as value_p50
      , PERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY value_avg) as value_p95
      , current_timestamp as calculated_on
      FROM hourly_rollups m
      JOIN sensors s ON (m.sensors_id = s.sensors_id)
      JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
      WHERE sy.sensor_nodes_id = 13
      GROUP BY 1, 2, 3)
    SELECT 13 as id
        , json_build_object(
          'label', '1year'
          , 'datetime_from', get_datetime_object(h.datetime, 'utc')
          , 'datetime_to', get_datetime_object(h.datetime + '1{interval}'::interval, 'utc')
          , 'interval',  '1 {interval}'
        ) as period
        , json_build_object(
          'id', h.measurands_id
          , 'units', m.units
          , 'name', m.measurand
        ) as parameter
        , json_build_object(
            'min', h.value_min
          , 'q05', h.value_p05
          , 'median', h.value_p50
          , 'q95', h.value_p95
          , 'max', h.value_max
          , 'sd', h.value_sd
        ) as summary
        , h.value_avg as value
        , calculate_coverage(
            h.value_count::int
           , s.data_averaging_period_seconds
           , s.data_logging_period_seconds
           , {seconds}::numeric
        ) as coverage
        , COUNT(1) OVER() as found
    FROM measurements h
    JOIN measurands m ON (h.measurands_id = m.measurands_id)
    LIMIT 4;
        """

    response = await db.fetchPage(sql, query.params())
    return response


# Remove from here down once we update the v2/measurements endpoint
class MeasurementV2(JsonBase):
    location_id: int = Field(..., alias='locationId')
    parameter: str
    value: float
    date: DatetimeObject
    unit: str


class MeasurementsV2Response(OpenAQResult):
    results: List[MeasurementV2]


@router.get(
    "/locations/{locations_id}/measurementsv2",
    response_model=MeasurementsV2Response,
    summary="Get measurements by location",
    description="Provides a list of measurements by location ID",
)
async def measurements_get_v2(
    measurements: LocationMeasurementsQueries = Depends(
        LocationMeasurementsQueries.depends()
    ),
    db: DB = Depends(),
):
    response = await fetch_measurements_v2(measurements, db)
    return response


async def fetch_measurements_v2(q, db):
    query = QueryBuilder(q)

    sql = f"""
    SELECT n.sensor_nodes_id as location_id
    , get_datetime_object(h.datetime, 'utc') as date
    , m.measurand as parameter
    , m.units as unit
    , h.value
    {query.fields()}
    {query.total()}
    FROM measurements h
    JOIN sensors s USING (sensors_id)
    JOIN sensor_systems sy USING (sensor_systems_id)
    JOIN measurands m ON (m.measurands_id = s.measurands_id)
    JOIN sensor_nodes n ON (n.sensor_nodes_id = sy.sensor_nodes_id)
    {query.where()}
    {query.pagination()}
    """

    response = await db.fetchPage(sql, query.params())
    return response
