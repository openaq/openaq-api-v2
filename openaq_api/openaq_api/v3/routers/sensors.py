import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path

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
    ...


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

        fmt = ""
        if q.period_name == "hod":
            fmt = "HH24"
            dur = "01:00:00"
            prd = "hour"
        elif q.period_name == "dow":
            fmt = "ID"
            dur = "24:00:00"
            prd = "day"
        elif q.period_name == "mod":
            fmt = "MM"
            dur = "1 month"
            prd = "month"


        q.period_name = prd
        sql = f"""
WITH trends AS (
SELECT
  sn.id
 , s.measurands_id
 , sn.timezone
 , to_char(timezone(sn.timezone, datetime - '1sec'::interval), '{fmt}') as factor
 , AVG(s.data_averaging_period_seconds) as avg_seconds
 , AVG(s.data_logging_period_seconds) as log_seconds
, MAX(truncate_timestamp(datetime, :period_name, sn.timezone, '1{prd}'::interval)) as last_period
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
 {query.where()}
 GROUP BY 1, 2, 3, 4)
 SELECT t.id
 , json_build_object(
	'label', factor
			   , 'datetime_from', get_datetime_object(first_datetime, t.timezone)
                , 'datetime_to', get_datetime_object(last_datetime, t.timezone)
                , 'interval',  '{dur}'
                ) as period
            , sig_digits(value_avg, 2) as value
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
     t.value_count::int
   , t.avg_seconds
   , t.log_seconds
  , expected_hours(first_datetime, last_datetime, '{prd}', factor) * 3600.0
)||jsonb_build_object(
          'datetime_from', get_datetime_object(first_datetime, t.timezone)
        , 'datetime_to', get_datetime_object(last_datetime, t.timezone)
 ) as coverage
 FROM trends t
 JOIN measurands m ON (t.measurands_id = m.measurands_id)
 {query.pagination()}
    """

    return await db.fetchPage(sql, query.params())
