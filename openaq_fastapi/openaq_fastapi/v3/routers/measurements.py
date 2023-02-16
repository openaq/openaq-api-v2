from fastapi import APIRouter, Depends, Path
from openaq_fastapi.db import DB
from typing import List, Union
from fastapi import Query
from pydantic import Field
from openaq_fastapi.v3.models.responses import (
    MeasurementsResponse,
    JsonBase,
    DatetimeObject,
    OpenAQResult,
)
from openaq_fastapi.v3.models.queries import (
    CommaSeparatedList,
    QueryBaseModel,
    QueryBuilder,
    Paging,
    DateFromQuery,
    DateToQuery,
    PeriodNameQuery,
)

router = APIRouter(prefix="/v3", tags=["v3"])


class LocationPathQuery(QueryBaseModel):
    locations_id: int = Path(
        description="Limit the results to a specific location by id", ge=1
    )

    def where(self) -> str:
        return "sy.sensor_nodes_id = :locations_id"


class MeasurementsParametersQuery(QueryBaseModel):

    parameters_id: Union[CommaSeparatedList[int], None] = Query(description="")

    def where(self) -> Union[str, None]:
        if self.has("parameters_id"):
            return "m.measurands_id = ANY (:parameters_id)"


class LocationMeasurementsQueries(
    Paging,
    LocationPathQuery,
    DateFromQuery,
    DateToQuery,
    MeasurementsParametersQuery,
    PeriodNameQuery,
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
    dur = "01:00:00"
    expected_hours = 1

    if q.period_name == "hour":
        # Query for hourly data
        sql = f"""
        SELECT sy.sensor_nodes_id as id
        , json_build_object(
        'label', '1hour'
        , 'datetime_from', get_datetime_object(h.datetime - '1hour'::interval, 'utc')
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
        , sig_digits(h.value_avg, 2) as value
        , calculate_coverage(
          h.value_count
        , s.data_averaging_period_seconds
        , s.data_logging_period_seconds
        , {expected_hours} * 3600
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
        if q.period_name == "hour":
            dur = "01:00:00"
        elif q.period_name == "day":
            dur = "24:00:00"
        elif q.period_name == "month":
            dur = "1 month"

        sql = f"""
WITH meas AS (
SELECT
  sy.sensor_nodes_id
 , s.measurands_id
 , date_trunc(:period_name, datetime - '1sec'::interval) as datetime
 , AVG(s.data_averaging_period_seconds) as avg_seconds
 , AVG(s.data_logging_period_seconds) as log_seconds
 , MAX(date_trunc(:period_name, datetime + '1{q.period_name} -1sec'::interval)) as last_period
 , MIN(datetime - '1sec'::interval) as first_datetime
 , MAX(datetime - '1sec'::interval) as last_datetime
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
 FROM hourly_rollups m
 JOIN sensors s ON (m.sensors_id = s.sensors_id)
 JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
 {query.where()}
 GROUP BY 1, 2, 3)
 SELECT sensor_nodes_id
 , json_build_object(
    'label', '1{q.period_name}'
    , 'datetime_from', get_datetime_object(datetime, 'utc')
    , 'datetime_to', get_datetime_object(last_period, 'utc')
    , 'interval',  '{dur}'
    ) as period
 , sig_digits(value_avg, 2) as value
 , first_datetime
 , last_datetime
 , json_build_object(
    'id', t.measurands_id
    , 'units', m.units
    , 'name', m.measurand
 ) as parameter
 , json_build_object(
    'sd', t.value_sd
   , 'min', t.value_min
   , 'q02', t.value_p02
   , 'q25', t.value_p25
   , 'median', t.value_p50
   , 'q75', t.value_p75
   , 'q98', t.value_p98
   , 'max', t.value_max
 ) as summary
 , calculate_coverage(
     value_count::int
    , 3600
    , 3600
    , EXTRACT(EPOCH FROM last_period - datetime)
 ) as coverage,
 {query.total()}
 FROM meas t
 JOIN measurands m ON (t.measurands_id = m.measurands_id)
 {query.pagination()}
    """
    print(sql, query.params())
    response = await db.fetchPage(sql, query.params())
    return response


# Remove from here down once we update the v2/measurements endpoint
class MeasurementV2(JsonBase):
    location_id: int = Field(..., alias="locationId")
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
    , get_datetime_object(h.datetime, 'utc') as datetime
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
