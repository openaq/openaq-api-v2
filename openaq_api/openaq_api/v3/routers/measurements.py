import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Path
from fastapi.exceptions import RequestValidationError
from pydantic import model_validator
from datetime import date, timedelta

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    DateFromQuery,
    DateToQuery,
    DatetimeFromQuery,
    DatetimeToQuery,
    Paging,
    QueryBaseModel,
    QueryBuilder,
)

from openaq_api.v3.models.responses import (
    MeasurementsResponse,
    HourlyDataResponse,
    DailyDataResponse,
    AnnualDataResponse,
)

logger = logging.getLogger("measurements")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
    include_in_schema=True,
)


class SensorQuery(QueryBaseModel):
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


class BaseDatetimeQueries(
    SensorQuery,
    DatetimeFromQuery,
    DatetimeToQuery,
):
    @model_validator(mode="after")
    @classmethod
    def check_dates_are_in_order(cls, data: Any) -> Any:
        dt = getattr(data, "datetime_to")
        df = getattr(data, "datetime_from")
        if dt and df and dt <= df:
            raise RequestValidationError(
                f"Date/time from must be older than the date/time to. User passed {df} - {dt}"
            )


class PagedDatetimeQueries(
    Paging,
    BaseDatetimeQueries,
): ...


class BaseDateQueries(
    SensorQuery,
    DateFromQuery,
    DateToQuery,
):
    @model_validator(mode="after")
    @classmethod
    def check_dates_are_in_order(cls, data: Any) -> Any:
        dt = getattr(data, "date_to")
        df = getattr(data, "date_from")
        if dt and df and dt <= df:
            raise RequestValidationError(
                f"Date from must be older than the date to. User passed {df} - {dt}"
            )


class PagedDateQueries(
    Paging,
    BaseDateQueries,
): ...


@router.get(
    "/sensors/{sensors_id}/measurements",
    response_model=MeasurementsResponse,
    summary="Get measurements by sensor ID",
    description="Provides a list of measurements by sensor ID",
)
async def sensor_measurements_get(
    sensors: Annotated[PagedDatetimeQueries, Depends(PagedDatetimeQueries.depends())],
    db: DB = Depends(),
):
    query = QueryBuilder(sensors)
    response = await fetch_measurements(query, db)
    return response


@router.get(
    "/sensors/{sensors_id}/measurements/hourly",
    response_model=MeasurementsResponse,
    summary="Get measurements aggregated to hours by sensor ID",
    description="Provides a list of measurements by sensor ID",
)
async def sensor_measurements_aggregated_get_hourly(
    sensors: Annotated[PagedDatetimeQueries, Depends(PagedDatetimeQueries.depends())],
    db: DB = Depends(),
):
    aggregate_to = "hour"
    query = QueryBuilder(sensors)
    response = await fetch_measurements_aggregated(query, aggregate_to, db)
    return response


@router.get(
    "/sensors/{sensors_id}/measurements/daily",
    response_model=MeasurementsResponse,
    summary="Get measurements aggregated to days by sensor ID",
    description="Provides a list of measurements by sensor ID",
)
async def sensor_measurements_aggregated_get_daily(
    sensors: Annotated[PagedDatetimeQueries, Depends(PagedDatetimeQueries.depends())],
    db: DB = Depends(),
):
    aggregate_to = "day"
    query = QueryBuilder(sensors)
    response = await fetch_measurements_aggregated(query, aggregate_to, db)
    return response


@router.get(
    "/sensors/{sensors_id}/hours",
    response_model=HourlyDataResponse,
    summary="Get measurements aggregated to hour by sensor ID",
    description="Provides a list of hourly data by sensor ID",
)
async def sensor_hourly_measurements_get(
    sensors: Annotated[PagedDatetimeQueries, Depends(PagedDatetimeQueries.depends())],
    db: DB = Depends(),
):
    query = QueryBuilder(sensors)
    response = await fetch_hours(query, db)
    return response


@router.get(
    "/sensors/{sensors_id}/hours/daily",
    response_model=HourlyDataResponse,
    summary="Get measurements aggregated from hour to day by sensor ID",
    description="Provides a list of daily summaries of hourly data by sensor ID",
)
async def sensor_hourly_measurements_aggregate_to_day_get(
    sensors: Annotated[PagedDatetimeQueries, Depends(PagedDatetimeQueries.depends())],
    db: DB = Depends(),
):
    aggregate_to = "day"
    query = QueryBuilder(sensors)
    response = await fetch_hours_aggregated(query, aggregate_to, db)
    return response


@router.get(
    "/sensors/{sensors_id}/hours/monthly",
    response_model=HourlyDataResponse,
    summary="Get measurements aggregated from hour to month by sensor ID",
    description="Provides a list of daily summaries of hourly data by sensor ID",
)
async def sensor_hourly_measurements_aggregate_to_month_get(
    sensors: Annotated[PagedDatetimeQueries, Depends(PagedDatetimeQueries.depends())],
    db: DB = Depends(),
):
    aggregate_to = "month"
    query = QueryBuilder(sensors)
    response = await fetch_hours_aggregated(query, aggregate_to, db)
    return response


@router.get(
    "/sensors/{sensors_id}/hours/yearly",
    response_model=HourlyDataResponse,
    summary="Get measurements aggregated from hour to year by sensor ID",
    description="Provides a list of yearly summaries of hourly data by sensor ID",
)
async def sensor_hourly_measurements_aggregate_to_year_get(
    sensors: Annotated[PagedDatetimeQueries, Depends(PagedDatetimeQueries.depends())],
    db: DB = Depends(),
):
    aggregate_to = "year"
    query = QueryBuilder(sensors)
    response = await fetch_hours_aggregated(query, aggregate_to, db)
    return response


@router.get(
    "/sensors/{sensors_id}/hours/hourofday",
    response_model=HourlyDataResponse,
    summary="Get measurements aggregated from hour to day of week by sensor ID",
    description="Provides a list of yearly summaries of hourly data by sensor ID",
)
async def sensor_hourly_measurements_aggregate_to_hod_get(
    sensors: Annotated[BaseDatetimeQueries, Depends(BaseDatetimeQueries.depends())],
    db: DB = Depends(),
):
    aggregate_to = "hod"
    query = QueryBuilder(sensors)
    logger.debug(query)
    response = await fetch_hours_trends(aggregate_to, query, db)
    return response


@router.get(
    "/sensors/{sensors_id}/hours/dayofweek",
    response_model=HourlyDataResponse,
    summary="Get measurements aggregated from hour to day of week by sensor ID",
    description="Provides a list of yearly summaries of hourly data by sensor ID",
)
async def sensor_hourly_measurements_aggregate_to_dow_get(
    sensors: Annotated[BaseDatetimeQueries, Depends(BaseDatetimeQueries.depends())],
    db: DB = Depends(),
):
    aggregate_to = "dow"
    query = QueryBuilder(sensors)
    response = await fetch_hours_trends(aggregate_to, query, db)
    return response


@router.get(
    "/sensors/{sensors_id}/hours/monthofyear",
    response_model=HourlyDataResponse,
    summary="Get measurements aggregated from hour to day of week by sensor ID",
    description="Provides a list of yearly summaries of hourly data by sensor ID",
)
async def sensor_hourly_measurements_aggregate_to_moy_get(
    sensors: Annotated[BaseDatetimeQueries, Depends(BaseDatetimeQueries.depends())],
    db: DB = Depends(),
):
    aggregate_to = "moy"
    query = QueryBuilder(sensors)
    response = await fetch_hours_trends(aggregate_to, query, db)
    return response


@router.get(
    "/sensors/{sensors_id}/days/dayofweek",
    response_model=HourlyDataResponse,
    summary="Get measurements aggregated from day to day of week by sensor ID",
    description="Provides a list of yearly summaries of dayly data by sensor ID",
)
async def sensor_daily_measurements_aggregate_to_dow_get(
    sensors: Annotated[BaseDateQueries, Depends(BaseDateQueries.depends())],
    db: DB = Depends(),
):
    aggregate_to = "dow"
    query = QueryBuilder(sensors)
    response = await fetch_days_trends(aggregate_to, query, db)
    return response


@router.get(
    "/sensors/{sensors_id}/days/monthofyear",
    response_model=HourlyDataResponse,
    summary="Get measurements aggregated from day to day of week by sensor ID",
    description="Provides a list of yearly summaries of daily data by sensor ID",
)
async def sensor_daily_measurements_aggregate_to_moy_get(
    sensors: Annotated[BaseDateQueries, Depends(BaseDateQueries.depends())],
    db: DB = Depends(),
):
    aggregate_to = "moy"
    query = QueryBuilder(sensors)
    response = await fetch_days_trends(aggregate_to, query, db)
    return response


@router.get(
    "/sensors/{sensors_id}/days",
    response_model=DailyDataResponse,
    summary="Get measurements aggregated to day by sensor ID",
    description="Provides a list of daily data by sensor ID",
)
async def sensor_daily_get(
    sensors: Annotated[PagedDateQueries, Depends(PagedDateQueries.depends())],
    db: DB = Depends(),
):
    query = QueryBuilder(sensors)
    response = await fetch_days(query, db)
    return response


@router.get(
    "/sensors/{sensors_id}/days/monthly",
    response_model=HourlyDataResponse,
    summary="Get measurements aggregated from hour to month by sensor ID",
    description="Provides a list of daily summaries of hourly data by sensor ID",
)
async def sensor_daily_aggregate_to_month_get(
    sensors: Annotated[PagedDateQueries, Depends(PagedDateQueries.depends())],
    db: DB = Depends(),
):
    aggregate_to = "month"
    query = QueryBuilder(sensors)
    response = await fetch_days_aggregated(query, aggregate_to, db)
    return response


@router.get(
    "/sensors/{sensors_id}/days/yearly",
    response_model=HourlyDataResponse,
    summary="Get measurements aggregated from day to year by sensor ID",
    description="Provides a list of yearly summaries of daily data by sensor ID",
)
async def sensor_daily_aggregate_to_year_get(
    sensors: Annotated[PagedDateQueries, Depends(PagedDateQueries.depends())],
    db: DB = Depends(),
):
    aggregate_to = "year"
    query = QueryBuilder(sensors)
    response = await fetch_days_aggregated(query, aggregate_to, db)
    return response


@router.get(
    "/sensors/{sensors_id}/years",
    response_model=AnnualDataResponse,
    summary="Get measurements aggregated to year by sensor ID",
    description="Provides a list of annual data by sensor ID",
)
async def sensor_yearly_get(
    sensors: Annotated[PagedDateQueries, Depends(PagedDateQueries.depends())],
    db: DB = Depends(),
):
    query = QueryBuilder(sensors)
    response = await fetch_years(query, db)
    return response


async def fetch_measurements(query, db):

    query.set_column_map({"timezone": "tz.tzid"})

    sql = f"""
      SELECT m.sensors_id
       , value
        , get_datetime_object(m.datetime, tz.tzid)
        , json_build_object(
            'id', s.measurands_id
          , 'units', p.units
          , 'name', p.measurand
        ) as parameter
    , json_build_object(
         'label', 'raw'
       , 'interval', make_interval(secs=>s.data_logging_period_seconds)
       , 'datetime_from', get_datetime_object(m.datetime - make_interval(secs=>s.data_logging_period_seconds), tz.tzid)
       , 'datetime_to', get_datetime_object(m.datetime, tz.tzid)
      ) as period
    , json_build_object(
         'expected_count', 1
        , 'observed_count', 1
       , 'expected_interval', make_interval(secs=>s.data_logging_period_seconds)
       , 'observed_interval', make_interval(secs=>s.data_averaging_period_seconds)
       , 'datetime_from', get_datetime_object(m.datetime - make_interval(secs=>s.data_averaging_period_seconds), tz.tzid)
       , 'datetime_to', get_datetime_object(m.datetime, tz.tzid)
       , 'percent_complete', 100
       , 'percent_coverage', (s.data_averaging_period_seconds/s.data_logging_period_seconds)*100
      ) as coverage
        , sensor_flags_exist(m.sensors_id, m.datetime, make_interval(secs=>s.data_averaging_period_seconds*-1)) as flag_info
        FROM measurements m
        JOIN sensors s USING (sensors_id)
        JOIN measurands p USING (measurands_id)
        JOIN sensor_systems sy USING (sensor_systems_id)
        JOIN sensor_nodes sn USING (sensor_nodes_id)
        JOIN timezones tz USING (timezones_id)
        {query.where()}
        ORDER BY datetime
        {query.pagination()}
        """
    return await db.fetchPage(sql, query.params())


async def fetch_measurements_aggregated(query, aggregate_to, db):
    if aggregate_to == "hour":
        dur = "01:00:00"
        expected_hours = 1
        interval_seconds = 3600
    elif aggregate_to == "day":
        dur = "24:00:00"
        interval_seconds = 3600 * 24
    else:
        raise Exception(f"{aggregate_to} is not supported")

    query.set_column_map({"timezone": "tz.tzid"})

    sql = f"""
        WITH meas AS (
        SELECT
        s.sensors_id
        , s.measurands_id
        , tz.tzid as timezone
        , truncate_timestamp(datetime, :aggregate_to, tz.tzid) as datetime
        , AVG(s.data_averaging_period_seconds) as avg_seconds
        , AVG(s.data_logging_period_seconds) as log_seconds
        , MAX(truncate_timestamp(datetime, '{aggregate_to}', tz.tzid, '1{aggregate_to}'::interval))
           as last_period
        , MIN(timezone(tz.tzid, datetime)) as datetime_first
        , MAX(timezone(tz.tzid, datetime)) as datetime_last
        , COUNT(1) as value_count
        , AVG(value) as value_avg
        , STDDEV(value) as value_sd
        , MIN(value) as value_min
        , MAX(value) as value_max
        , PERCENTILE_CONT(0.02) WITHIN GROUP(ORDER BY value) as value_p02
        , PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY value) as value_p25
        , PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY value) as value_p50
        , PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY value) as value_p75
        , PERCENTILE_CONT(0.98) WITHIN GROUP(ORDER BY value) as value_p98
        , current_timestamp as calculated_on
        FROM measurements m
        JOIN sensors s ON (m.sensors_id = s.sensors_id)
        JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
        JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
        JOIN timezones tz ON (sn.timezones_id = tz.timezones_id)
        {query.where()}
        GROUP BY 1, 2, 3, 4)
        SELECT t.sensors_id
        ----------
        , json_build_object(
            'label', '1 {aggregate_to}'
            , 'datetime_from', get_datetime_object(datetime, t.timezone)
            , 'datetime_to', get_datetime_object(last_period, t.timezone)
            , 'interval',  '{dur}'
            ) as period
        ----------
        , sig_digits(value_avg, 3) as value
        -----------
        , json_build_object(
            'id', t.measurands_id
            , 'units', m.units
            , 'name', m.measurand
        ) as parameter
        ---------
        , json_build_object(
             'avg', t.value_avg
           , 'sd', t.value_sd
           , 'min', t.value_min
           , 'q02', t.value_p02
           , 'q25', t.value_p25
           , 'median', t.value_p50
           , 'q75', t.value_p75
           , 'q98', t.value_p98
           , 'max', t.value_max
        ) as summary
        --------
        , calculate_coverage(
            value_count::int
           , avg_seconds
           , log_seconds
            , EXTRACT(EPOCH FROM last_period - datetime)
        )||jsonb_build_object(
                'datetime_from', get_datetime_object(datetime_first - make_interval(secs=>log_seconds), t.timezone)
                , 'datetime_to', get_datetime_object(datetime_last, t.timezone)
                ) as coverage
        , sensor_flags_exist(t.sensors_id, t.datetime, '-{dur}'::interval) as flag_info
        {query.total()}
        FROM meas t
        JOIN measurands m ON (t.measurands_id = m.measurands_id)
        {query.pagination()}
    """

    params = query.params()
    params["aggregate_to"] = aggregate_to
    return await db.fetchPage(sql, params)


async def fetch_hours(query, db):
    sql = f"""
        SELECT sn.id
        , json_build_object(
        'label', '1hour'
        , 'datetime_from', get_datetime_object(h.datetime - '1hour'::interval, sn.timezone)
        , 'datetime_to', get_datetime_object(h.datetime, sn.timezone)
        , 'interval',  '01:00:00'
        ) as period
        , json_build_object(
        'id', s.measurands_id
        , 'units', m.units
        , 'name', m.measurand
        ) as parameter
        , json_build_object(
             'avg', h.value_avg
           , 'sd', h.value_sd
        , 'min', h.value_min
        , 'q02', h.value_p02
        , 'q25', h.value_p25
        , 'median', h.value_p50
        , 'q75', h.value_p75
        , 'q98', h.value_p98
        , 'max', h.value_max
        ) as summary
        , sig_digits(h.value_avg, 3) as value
        , calculate_coverage(
          h.value_count
        , s.data_averaging_period_seconds
        , s.data_logging_period_seconds
        , 1 * 3600
        )||jsonb_build_object(
          'datetime_from', get_datetime_object(h.datetime_first - '1h'::interval, sn.timezone)
        , 'datetime_to', get_datetime_object(h.datetime_last, sn.timezone)
        ) as coverage
        , sensor_flags_exist(h.sensors_id, h.datetime) as flag_info
        {query.fields()}
        FROM hourly_data h
        JOIN sensors s USING (sensors_id)
        JOIN sensor_systems sy USING (sensor_systems_id)
        JOIN locations_view_cached sn ON (sy.sensor_nodes_id = sn.id)
        JOIN measurands m ON (m.measurands_id = s.measurands_id)
        {query.where()}
        ORDER BY datetime
        {query.pagination()}
        """
    return await db.fetchPage(sql, query.params())


async def fetch_hours_aggregated(query, aggregate_to, db):
    if aggregate_to == "year":
        dur = "1year"
    elif aggregate_to == "month":
        dur = "1month"
    elif aggregate_to == "day":
        dur = "24:00:00"
    else:
        raise Exception(f"{aggregate_to} is not supported")

    query.set_column_map({"timezone": "tz.tzid"})

    sql = f"""
        WITH meas AS (
        SELECT
        s.sensors_id
        , s.measurands_id
        , tz.tzid as timezone
        , truncate_timestamp(datetime, :aggregate_to, tz.tzid) as datetime
        , AVG(s.data_averaging_period_seconds) as avg_seconds
        , AVG(s.data_logging_period_seconds) as log_seconds
        , MAX(truncate_timestamp(datetime, '{aggregate_to}', tz.tzid, '{dur}'::interval))
           as last_period
        , MIN(timezone(tz.tzid, datetime)) as datetime_first
        , MAX(timezone(tz.tzid, datetime)) as datetime_last
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
        JOIN sensors s ON (m.sensors_id = s.sensors_id)
        JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
        JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
        JOIN timezones tz ON (sn.timezones_id = tz.timezones_id)
        {query.where()}
        GROUP BY 1, 2, 3, 4)
        SELECT t.sensors_id
        ----------
        , json_build_object(
            'label', '1 {aggregate_to}'
            , 'datetime_from', get_datetime_object(datetime, t.timezone)
            , 'datetime_to', get_datetime_object(last_period, t.timezone)
            , 'interval',  '{dur}'
            ) as period
        ----------
        , sig_digits(value_avg, 3) as value
        -----------
        , json_build_object(
            'id', t.measurands_id
            , 'units', m.units
            , 'name', m.measurand
        ) as parameter
        ---------
        , json_build_object(
             'avg', t.value_avg
           , 'sd', t.value_sd
           , 'min', t.value_min
           , 'q02', t.value_p02
           , 'q25', t.value_p25
           , 'median', t.value_p50
           , 'q75', t.value_p75
           , 'q98', t.value_p98
           , 'max', t.value_max
        ) as summary
        --------
        , calculate_coverage(
            value_count::int
            , 3600
            , 3600
            , EXTRACT(EPOCH FROM last_period - datetime)
        )||jsonb_build_object(
                'datetime_from', get_datetime_object(datetime_first - '1h'::interval, t.timezone)
                , 'datetime_to', get_datetime_object(datetime_last, t.timezone)
                ) as coverage
        , sensor_flags_exist(t.sensors_id, t.datetime, '-{dur}'::interval) as flag_info
        {query.total()}
        FROM meas t
        JOIN measurands m ON (t.measurands_id = m.measurands_id)
        {query.pagination()}
    """
    params = query.params()
    params["aggregate_to"] = aggregate_to
    return await db.fetchPage(sql, params)


async def fetch_days_trends(aggregate_to, query, db):

    if aggregate_to == "dow":
        period_name = "day"
        period_format = "'ID'"
        period_first_offset = "'0sec'"
        period_last_offset = "'0sec'"
        aggregate_to = "day"
    elif aggregate_to == "moy":
        period_name = "month"
        period_format = "'MM'"
        period_first_offset = "'-1sec'"
        period_last_offset = "'+1sec'"
        aggregate_to = "month"

    dur = "24:00:00"
    interval_seconds = 3600 * 24

    params = query.params()
    params["aggregate_to"] = aggregate_to

    datetime_field_name = "date"
    if params.get("date_to") is None:
        params["date_to"] = date.today()

    if params.get("date_from") is None:
        dt = params.get("date_to")
        params["date_from"] = dt - timedelta(days=365)

    sql = f"""
    -----------------------------------
    -- start by getting some basic sensor information
    -- and transforming the timestamps
    -----------------------------------
    WITH sensor AS (
        SELECT s.sensors_id
        , sn.sensor_nodes_id
        , s.data_averaging_period_seconds
        , s.data_logging_period_seconds
        , tz.tzid as timezone
        , m.measurands_id
        , m.measurand
        , m.units
        , as_timestamptz(:date_from, tz.tzid) as datetime_from
        , as_timestamptz(:date_to, tz.tzid) as datetime_to
        FROM sensors s
        , sensor_systems sy
        , sensor_nodes sn
        , timezones tz
        , measurands m
        WHERE s.sensor_systems_id = sy.sensor_systems_id
        AND sy.sensor_nodes_id = sn.sensor_nodes_id
        AND sn.timezones_id = tz.timezones_id
        AND s.sensors_id = :sensors_id
        AND s.measurands_id = m.measurands_id
        AND sn.is_public AND s.is_public
    --------------------------------
    -- Then we calculate what we expect to find in the data
    --------------------------------
    ), expected AS (
        SELECT to_char(dd, {period_format}) as factor
        , s.timezone
        , COUNT(1) as n
        , MIN(dd) as period_first
        , MAX(dd) as period_last
        FROM sensor s
        , generate_series(s.datetime_from, s.datetime_to, '1day'::interval) dd
        GROUP BY 1,2
    ------------------------------------
    -- Then we query what we have in the db
    -- we join the sensor CTE here so that we have access to the timezone
    ------------------------------------
    ), observed AS (
        SELECT
        s.sensors_id
        , s.data_averaging_period_seconds
        , s.data_logging_period_seconds
 , s.timezone
 , s.measurands_id
 , s.measurand
 , s.units
 , to_char(datetime, {period_format}) as factor
 , MIN(datetime) as coverage_first
 , MAX(datetime) as coverage_last
 , COUNT(1) as n
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
 FROM daily_data m
 JOIN sensor s ON (m.sensors_id = s.sensors_id)
 {query.where()}
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8)
-----------------------------------------
-- And finally we tie it all together
-----------------------------------------
    SELECT o.sensors_id
  , sig_digits(value_avg, 3) as value
  , json_build_object(
     'id', o.measurands_id
   , 'units', o.units
   , 'name', o.measurand
  ) as parameter
  , json_build_object(
     'avg', o.value_avg
    , 'sd', o.value_sd
    , 'min', o.value_min
   , 'q02', o.value_p02
   , 'q25', o.value_p25
   , 'median', o.value_p50
   , 'q75', o.value_p75
   , 'q98', o.value_p98
   , 'max', o.value_max
     ) as summary
    , json_build_object(
       'label', e.factor
     , 'datetime_from', get_datetime_object(e.period_first::date, o.timezone)
     , 'datetime_to', get_datetime_object(e.period_last::date + '1day'::interval, o.timezone)
     , 'interval', :aggregate_to::text
    ) as period
    , calculate_coverage(
        o.n::int
      , {interval_seconds}
      , {interval_seconds}
      , e.n * {interval_seconds}
           )||
    jsonb_build_object(
        'datetime_from', get_datetime_object(o.coverage_first::timestamp, o.timezone)
      , 'datetime_to', get_datetime_object(o.coverage_last + '1day'::interval, o.timezone)
    ) as coverage
    , sensor_flags_exist(o.sensors_id, e.period_last, '-{dur}'::interval) as flag_info
    FROM expected e
    JOIN observed o ON (e.factor = o.factor)
    ORDER BY e.factor
    """

    return await db.fetchPage(sql, params)


async def fetch_hours_trends(aggregate_to, query, db):

    if aggregate_to == "hod":
        period_name = "hour"
        period_format = "'HH24'"
        period_first_offset = "'-1sec'"
        period_last_offset = "'+1sec'"
        aggregate_to = "hour"
    elif aggregate_to == "dow":
        period_name = "day"
        period_format = "'ID'"
        period_first_offset = "'0sec'"
        period_last_offset = "'0sec'"
        aggregate_to = "day"
    elif aggregate_to == "moy":
        period_name = "month"
        period_format = "'MM'"
        period_first_offset = "'-1sec'"
        period_last_offset = "'+1sec'"
        aggregate_to = "month"

    dur = "01:00:00"
    interval_seconds = 3600

    params = query.params()
    params["aggregate_to"] = aggregate_to

    if params.get("datetime_to") is None:
        params["datetime_to"] = date.today()

    if params.get("datetime_from") is None:
        dt = params.get("datetime_to")
        params["datetime_from"] = dt - timedelta(days=365)

    sql = f"""
    -----------------------------------
    -- start by getting some basic sensor information
    -- and transforming the timestamps
    -----------------------------------
    WITH sensor AS (
        SELECT s.sensors_id
        , sn.sensor_nodes_id
        , s.data_averaging_period_seconds
        , s.data_logging_period_seconds
        , tz.tzid as timezone
        , m.measurands_id
        , m.measurand
        , m.units
        , as_timestamptz(:datetime_from, tz.tzid) as datetime_from
        , as_timestamptz(:datetime_to, tz.tzid) as datetime_to
        FROM sensors s
        , sensor_systems sy
        , sensor_nodes sn
        , timezones tz
        , measurands m
        WHERE s.sensor_systems_id = sy.sensor_systems_id
        AND sy.sensor_nodes_id = sn.sensor_nodes_id
        AND sn.timezones_id = tz.timezones_id
        AND s.sensors_id = :sensors_id
        AND s.measurands_id = m.measurands_id
        AND sn.is_public AND s.is_public
    --------------------------------
    -- Then we calculate what we expect to find in the data
    --------------------------------
    ), expected AS (
        SELECT to_char(timezone(s.timezone, dd - '1sec'::interval), {period_format}) as factor
        , s.timezone
        , COUNT(1) as n
        , MIN(date_trunc(:aggregate_to, dd + {period_first_offset}::interval)) as period_first
        , MAX(date_trunc(:aggregate_to, dd + {period_last_offset}::interval)) as period_last
        FROM sensor s
        , generate_series(s.datetime_from + '{dur}'::interval, s.datetime_to, '{dur}'::interval) dd
        GROUP BY 1,2
    ------------------------------------
    -- Then we query what we have in the db
    -- we join the sensor CTE here so that we have access to the timezone
    ------------------------------------
    ), observed AS (
        SELECT
        s.sensors_id
        , s.data_averaging_period_seconds
        , s.data_logging_period_seconds
 , s.timezone
 , s.measurands_id
 , s.measurand
 , s.units
 , to_char(timezone(s.timezone, datetime - '1sec'::interval), {period_format}) as factor
 , MIN(datetime) as coverage_first
 , MAX(datetime) as coverage_last
 , COUNT(1) as n
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
 JOIN sensor s ON (m.sensors_id = s.sensors_id)
 {query.where()}
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8)
-----------------------------------------
-- And finally we tie it all together
-----------------------------------------
    SELECT o.sensors_id
  , sig_digits(value_avg, 3) as value
  , json_build_object(
     'id', o.measurands_id
   , 'units', o.units
   , 'name', o.measurand
  ) as parameter
  , json_build_object(
     'avg', o.value_avg
    , 'sd', o.value_sd
    , 'min', o.value_min
   , 'q02', o.value_p02
   , 'q25', o.value_p25
   , 'median', o.value_p50
   , 'q75', o.value_p75
   , 'q98', o.value_p98
   , 'max', o.value_max
     ) as summary
    , json_build_object(
       'label', e.factor
     , 'datetime_from', get_datetime_object(e.period_first::timestamp, o.timezone)
     , 'datetime_to', get_datetime_object(e.period_last::timestamp, o.timezone)
     , 'interval', :aggregate_to
    ) as period
    , calculate_coverage(
        o.n::int
      , {interval_seconds}
      , {interval_seconds}
      , e.n * {interval_seconds}
           )||
    jsonb_build_object(
        'datetime_from', get_datetime_object(o.coverage_first - make_interval(secs=>{interval_seconds}), o.timezone)
      , 'datetime_to', get_datetime_object(o.coverage_last, o.timezone)
    ) as coverage
    , sensor_flags_exist(o.sensors_id, o.coverage_first, '{dur}'::interval) as flag_info
    FROM expected e
    JOIN observed o ON (e.factor = o.factor)
    ORDER BY e.factor
    """
    logger.debug(params)

    return await db.fetchPage(sql, params)


async def fetch_days_aggregated(query, aggregate_to, db):
    if aggregate_to == "year":
        dur = "1year"
        interval_seconds = 3600 * 24 * 365.24
    elif aggregate_to == "month":
        dur = "1 month"
        interval_seconds = 3600 * 24
    else:
        raise Exception(f"{aggregate_to} is not supported")

    sql = f"""
        WITH meas AS (
        SELECT
        s.sensors_id
        , s.measurands_id
        , sn.timezone
        -- days are time begining
        , date_trunc(:aggregate_to, datetime) as datetime
        , AVG(s.data_averaging_period_seconds) as avg_seconds
        , AVG(s.data_logging_period_seconds) as log_seconds
        , MAX(date_trunc(:aggregate_to, datetime + '1{aggregate_to}'::interval)) as last_period
        --, MIN(timezone(sn.timezone, datetime - '1sec'::interval)) as datetime_first
        --, MAX(timezone(sn.timezone, datetime - '1sec'::interval)) as datetime_last
        , MIN(datetime) as datetime_first
        , MAX(datetime) as datetime_last
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
        FROM daily_data m
        JOIN sensors s ON (m.sensors_id = s.sensors_id)
        JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
        JOIN locations_view_cached sn ON (sy.sensor_nodes_id = sn.id)
        {query.where()}
        GROUP BY 1, 2, 3, 4)
        SELECT t.sensors_id
        ----------
        , json_build_object(
            'label', '1 {aggregate_to}'
            , 'datetime_from', get_datetime_object(datetime::date, t.timezone)
            , 'datetime_to', get_datetime_object(last_period, t.timezone)
            , 'interval',  '{dur}'
            ) as period
        ----------
        , sig_digits(value_avg, 3) as value
        -----------
        , json_build_object(
            'id', t.measurands_id
            , 'units', m.units
            , 'name', m.measurand
        ) as parameter
        ---------
        , json_build_object(
             'avg', t.value_avg
           , 'sd', t.value_sd
           , 'min', t.value_min
           , 'q02', t.value_p02
           , 'q25', t.value_p25
           , 'median', t.value_p50
           , 'q75', t.value_p75
           , 'q98', t.value_p98
           , 'max', t.value_max
        ) as summary
        --------
        , calculate_coverage(
            value_count::int
            , 3600 * 24
            , 3600 * 24
            , EXTRACT(EPOCH FROM last_period - datetime)
        )||jsonb_build_object(
                'datetime_from', get_datetime_object(datetime_first, t.timezone)
                , 'datetime_to', get_datetime_object(datetime_last + '1day'::interval, t.timezone)
                ) as coverage
        , sensor_flags_exist(t.sensors_id, t.datetime, '-{dur}'::interval) as flag_info
        {query.total()}
        FROM meas t
        JOIN measurands m ON (t.measurands_id = m.measurands_id)
        ORDER BY datetime
        {query.pagination()}
    """
    params = query.params()
    params["aggregate_to"] = aggregate_to
    return await db.fetchPage(sql, params)


async def fetch_days(query, db):
    sql = f"""
        SELECT sn.id
        , json_build_object(
        'label', '1day'
        , 'datetime_from', get_datetime_object(h.datetime, sn.timezone)
        , 'datetime_to', get_datetime_object(h.datetime + '1day'::interval, sn.timezone)
        , 'interval',  '24:00:00'
        ) as period
        , json_build_object(
        'id', s.measurands_id
        , 'units', m.units
        , 'name', m.measurand
        ) as parameter
        , json_build_object(
             'avg', h.value_avg
           , 'sd', h.value_sd
        , 'min', h.value_min
        , 'q02', h.value_p02
        , 'q25', h.value_p25
        , 'median', h.value_p50
        , 'q75', h.value_p75
        , 'q98', h.value_p98
        , 'max', h.value_max
        ) as summary
        , sig_digits(h.value_avg, 3) as value
        , calculate_coverage(
          h.value_count
        , 3600
        , 3600
        , 24 * 3600
        )||jsonb_build_object(
          'datetime_from', get_datetime_object(h.datetime_first, sn.timezone)
        , 'datetime_to', get_datetime_object(h.datetime_last, sn.timezone)
        ) as coverage
        , sensor_flags_exist(h.sensors_id, h.datetime, '-1day'::interval) as flag_info
        {query.fields()}
        FROM daily_data h
        JOIN sensors s USING (sensors_id)
        JOIN sensor_systems sy USING (sensor_systems_id)
        JOIN locations_view_cached sn ON (sy.sensor_nodes_id = sn.id)
        JOIN measurands m ON (m.measurands_id = s.measurands_id)
        {query.where()}
        ORDER BY datetime
        {query.pagination()}
        """
    return await db.fetchPage(sql, query.params())


def aggregate_days(query, aggregate_to, db): ...


async def fetch_years(query, db):
    sql = f"""
        SELECT sn.id
        , json_build_object(
        'label', '1year'
        , 'datetime_from', get_datetime_object(h.datetime, sn.timezone)
        , 'datetime_to', get_datetime_object(h.datetime + '1year'::interval, sn.timezone)
        , 'interval',  '1 year'
        ) as period
        , json_build_object(
        'id', s.measurands_id
        , 'units', m.units
        , 'name', m.measurand
        ) as parameter
        , json_build_object(
             'avg', h.value_avg
           , 'sd', h.value_sd
        , 'min', h.value_min
        , 'q02', h.value_p02
        , 'q25', h.value_p25
        , 'median', h.value_p50
        , 'q75', h.value_p75
        , 'q98', h.value_p98
        , 'max', h.value_max
        ) as summary
        , sig_digits(h.value_avg, 3) as value
        , calculate_coverage(
          h.value_count
        , 3600
        , 3600
        , 3600 * 24 * ((h.datetime + '1year'::interval)::date - h.datetime)
        )||jsonb_build_object(
          'datetime_from', get_datetime_object(h.datetime_first, sn.timezone)
        , 'datetime_to', get_datetime_object(h.datetime_last, sn.timezone)
        ) as coverage
        , sensor_flags_exist(h.sensors_id, h.datetime, '-1y'::interval) as flag_info
        {query.fields()}
        FROM annual_data h
        JOIN sensors s USING (sensors_id)
        JOIN sensor_systems sy USING (sensor_systems_id)
        JOIN locations_view_cached sn ON (sy.sensor_nodes_id = sn.id)
        JOIN measurands m ON (m.measurands_id = s.measurands_id)
        {query.where()}
        ORDER BY datetime
        {query.pagination()}
        """
    return await db.fetchPage(sql, query.params())
