import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Path, HTTPException
from pydantic import field_validator
from datetime import date, datetime

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    DateFromQuery,
    DateToQuery,
    Paging,
    PeriodNameQuery,
    QueryBaseModel,
    QueryBuilder,
)

from openaq_api.v3.models.responses import (
	SensorsResponse,
	MeasurementsResponse,
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
        return "s.sensors_id = :sensors_id"


class LocationSensorQuery(QueryBaseModel):
    locations_id: int = Path(
        ..., description="Limit the results to a specific sensors id", ge=1
    )

    def where(self):
        return "n.sensor_nodes_id = :locations_id"


class SensorMeasurementsQueries(
    Paging,
	SensorQuery,
    DateFromQuery,
    DateToQuery,
    PeriodNameQuery,
):
    @field_validator('date_to', 'date_from')
    @classmethod
    def must_be_date_if_aggregating_to_day(cls, v: Any, values) -> str:
        if values.data.get('period_name') in ['dow','day','moy','month']:
            if isinstance(v, datetime):
                # this is to deal with the error that is thrown when using ValueError with datetime objects
                err = [{
                    "type": "value_error",
                    "msg": "When aggregating data to daily values or higher you can only use whole dates in the `date_from` and `date_to` parameters. E.g. 2024-01-01, 2024-01-01 00:00:00",
                    "input": str(v)
                       }]
                raise HTTPException(status_code=422, detail=err)
        return v



@router.get(
    "/sensors/{sensors_id}/measurements",
    response_model=MeasurementsResponse,
    summary="Get measurements by sensor ID",
    description="Provides a list of measurements by sensor ID",
)
async def sensor_measurements_get(
    sensors: Annotated[SensorMeasurementsQueries, Depends(SensorMeasurementsQueries.depends())],
    db: DB = Depends(),
):
    response = await fetch_measurements(sensors, db)
    return response


@router.get(
    "/locations/{locations_id}/sensors",
    response_model=SensorsResponse,
    summary="Get sensors by location ID",
    description="Provides a list of sensors by location ID",
)
async def sensors_get(
    location_sensors: Annotated[LocationSensorQuery, Depends(LocationSensorQuery.depends())],
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
    sensors: Annotated[SensorQuery, Depends(SensorQuery.depends())],
    db: DB = Depends(),
):
    return await fetch_sensors(sensors, db)


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
	, json_build_object(
	  'min', r.value_min
	, 'max', r.value_max
	, 'avg', r.value_avg
	, 'sd', r.value_sd
	) as summary
	, calculate_coverage(
		  r.value_count
		, s.data_averaging_period_seconds
		, s.data_logging_period_seconds
		, r.datetime_first
		, r.datetime_last
	) as coverage
	, get_datetime_object(r.datetime_first, t.tzid) as datetime_first
	, get_datetime_object(r.datetime_last, t.tzid) as datetime_last
	, json_build_object(
	   'datetime', get_datetime_object(r.datetime_last, t.tzid)
	  , 'value', r.value_latest
	  , 'coordinates', json_build_object(
				'latitude', st_y(COALESCE(r.geom_latest, n.geom))
				,'longitude', st_x(COALESCE(r.geom_latest, n.geom))
	)) as latest
	FROM sensors s
	JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
	JOIN sensor_nodes n ON (sy.sensor_nodes_id = n.sensor_nodes_id)
    JOIN timezones t ON (n.timezones_id = t.gid)
	JOIN measurands m ON (s.measurands_id = m.measurands_id)
	LEFT JOIN sensors_rollup r ON (s.sensors_id = r.sensors_id)
	{query.where()}
    {query.pagination()}
	"""
    return await db.fetchPage(sql, query.params())



async def fetch_measurements(q, db):
    query = QueryBuilder(q)
    dur = "01:00:00"
    expected_hours = 1

    if q.period_name in [None, "hour"]:
        # Query for hourly data
        sql = f"""
        SELECT sn.id
        , json_build_object(
        'label', '1hour'
        , 'datetime_from', get_datetime_object(h.datetime - '1hour'::interval, sn.timezone)
        , 'datetime_to', get_datetime_object(h.datetime, sn.timezone)
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
        )||jsonb_build_object(
          'datetime_from', get_datetime_object(h.first_datetime, sn.timezone)
        , 'datetime_to', get_datetime_object(h.last_datetime, sn.timezone)
        ) as coverage
        {query.fields()}
        FROM hourly_data h
        JOIN sensors s USING (sensors_id)
        JOIN sensor_systems sy USING (sensor_systems_id)
        JOIN locations_view_cached sn ON (sy.sensor_nodes_id = sn.id)
        JOIN measurands m ON (m.measurands_id = h.measurands_id)
        {query.where()}
        ORDER BY datetime
        {query.pagination()}
        """
    elif q.period_name in ["raw"]:
        sql = f"""
	WITH sensor AS (
		SELECT s.sensors_id
	, sn.sensor_nodes_id
  , s.data_averaging_period_seconds
  , s.data_logging_period_seconds
  , format('%ssec', s.data_averaging_period_seconds)::interval as averaging_interval
  , format('%ssec', s.data_logging_period_seconds)::interval as logging_interval
	, tz.tzid as timezone
	, m.measurands_id
	, m.measurand
	, m.units
	, timezone(tz.tzid, :date_from) as datetime_from
	, timezone(tz.tzid, :date_to) as datetime_to
   FROM sensors s
	, sensor_systems sy
	, sensor_nodes sn
	, timezones tz
	, measurands m
	WHERE s.sensor_systems_id = sy.sensor_systems_id
	AND sy.sensor_nodes_id = sn.sensor_nodes_id
	AND sn.timezones_id = tz.gid
	AND s.sensors_id = :sensors_id
	AND s.measurands_id = m.measurands_id)
	  SELECT m.sensors_id
	   , value
		, get_datetime_object(m.datetime, s.timezone)
		, json_build_object(
		    'id', s.measurands_id
		  , 'units', s.units
		  , 'name', s.measurand
		) as parameter
    , json_build_object(
	     'label', 'raw'
	   , 'interval', s.logging_interval
	   , 'datetime_from', get_datetime_object(m.datetime - s.logging_interval, s.timezone)
	   , 'datetime_to', get_datetime_object(m.datetime, s.timezone)
	  ) as period
    , json_build_object(
	     'expected_count', 1
		, 'observed_count', 1
	   , 'expected_interval', s.logging_interval
	   , 'observed_interval', s.averaging_interval
	   , 'datetime_from', get_datetime_object(m.datetime - s.averaging_interval, s.timezone)
	   , 'datetime_to', get_datetime_object(m.datetime, s.timezone)
	   , 'percent_complete', 100
	   , 'percent_coverage', (s.data_averaging_period_seconds/s.data_logging_period_seconds)*100
	  ) as coverage
        FROM measurements m
        JOIN sensor s USING (sensors_id)
        WHERE datetime > datetime_from
			  AND datetime <= datetime_to
			  AND s.sensors_id = :sensors_id
        ORDER BY datetime
        {query.pagination()}
              """
    elif q.period_name in ["day", "month"]:
        # Query for the aggregate data
        if q.period_name == "day":
            dur = "24:00:00"
        elif q.period_name == "month":
            dur = "1 month"

        sql = f"""
            WITH meas AS (
            SELECT
            sy.sensor_nodes_id
            , s.measurands_id
            , sn.timezone
            , truncate_timestamp(datetime, :period_name, sn.timezone) as datetime
            , AVG(s.data_averaging_period_seconds) as avg_seconds
            , AVG(s.data_logging_period_seconds) as log_seconds
            , MAX(truncate_timestamp(datetime, :period_name, sn.timezone, '1{q.period_name}'::interval)) as last_period
            , MIN(timezone(sn.timezone, datetime - '1sec'::interval)) as first_datetime
            , MAX(timezone(sn.timezone, datetime - '1sec'::interval)) as last_datetime
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
			JOIN locations_view_cached sn ON (sy.sensor_nodes_id = sn.id)
            -- JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
            -- JOIN timezones ts ON (sn.timezones_id = ts.gid)
            {query.where()}
            GROUP BY 1, 2, 3, 4)
            SELECT t.sensor_nodes_id
			----------
            , json_build_object(
                'label', '1{q.period_name}'
                , 'datetime_from', get_datetime_object(datetime, t.timezone)
                , 'datetime_to', get_datetime_object(last_period, t.timezone)
                , 'interval',  '{dur}'
                ) as period
			----------
            , sig_digits(value_avg, 2) as value
			-----------
            , json_build_object(
                'id', t.measurands_id
                , 'units', m.units
                , 'name', m.measurand
            ) as parameter
			---------
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
			--------
            , calculate_coverage(
                value_count::int
                , 3600
                , 3600
                , EXTRACT(EPOCH FROM last_period - datetime)
            )||jsonb_build_object(
                    'datetime_from', get_datetime_object(first_datetime, t.timezone)
                    , 'datetime_to', get_datetime_object(last_datetime, t.timezone)
                    ) as coverage
            {query.total()}
            FROM meas t
            JOIN measurands m ON (t.measurands_id = m.measurands_id)
            {query.pagination()}
        """
    elif q.period_name in ["hod","dow","moy"]:
        if q.period_name == "hod":
            q.period_name = "hour"
            period_format = "'HH24'"
            period_first_offset = "'-1sec'"
            period_last_offset = "'+1sec'"
        elif q.period_name == "dow":
            q.period_name = "day"
            period_format = "'ID'"
            period_first_offset = "'0sec'"
            period_last_offset = "'0sec'"
        elif q.period_name == "moy":
            q.period_name = "month"
            period_format = "'MM'"
            period_first_offset = "'-1sec'"
            period_last_offset = "'+1sec'"


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
		, timezone(tz.tzid, :date_from) as datetime_from
		, timezone(tz.tzid, :date_to) as datetime_to
		FROM sensors s
		, sensor_systems sy
		, sensor_nodes sn
		, timezones tz
		, measurands m
		WHERE s.sensor_systems_id = sy.sensor_systems_id
		AND sy.sensor_nodes_id = sn.sensor_nodes_id
		AND sn.timezones_id = tz.gid
		AND s.sensors_id = :sensors_id
		AND s.measurands_id = m.measurands_id
	--------------------------------
	-- Then we calculate what we expect to find in the data
	--------------------------------
	), expected AS (
		SELECT to_char(timezone(s.timezone, dd - '1sec'::interval), {period_format}) as factor
		, s.timezone
		, COUNT(1) as n
		, MIN(date_trunc(:period_name, dd + {period_first_offset}::interval)) as period_first
		, MAX(date_trunc(:period_name, dd + {period_last_offset}::interval)) as period_last
		FROM sensor s
		, generate_series(s.datetime_from + '1hour'::interval, s.datetime_to, ('1hour')::interval) dd
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
 WHERE datetime > datetime_from
 AND datetime <= datetime_to
 AND s.sensors_id = :sensors_id
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8)
-----------------------------------------
-- And finally we tie it all together
-----------------------------------------
	SELECT o.sensors_id
  , sig_digits(value_avg, 2) as value
  , json_build_object(
     'id', o.measurands_id
   , 'units', o.units
   , 'name', o.measurand
  ) as parameter
  , json_build_object(
     'sd', o.value_sd
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
	 , 'datetime_from', get_datetime_object(e.period_first, o.timezone)
	 , 'datetime_to', get_datetime_object(e.period_last, o.timezone)
	 , 'interval', :period_name
	) as period
	, calculate_coverage(
	    o.n::int
	  , o.data_averaging_period_seconds
      , o.data_logging_period_seconds
	  , e.n * 3600.0)||
	jsonb_build_object(
	    'datetime_from', get_datetime_object(o.coverage_first, o.timezone)
	  , 'datetime_to', get_datetime_object(o.coverage_last, o.timezone)
	) as coverage
	FROM expected e
	JOIN observed o ON (e.factor = o.factor)
    {query.pagination()}
    """

    return await db.fetchPage(sql, query.params())
