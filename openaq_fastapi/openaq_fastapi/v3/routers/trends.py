import logging
from fastapi import APIRouter, Depends, Path, Query
from openaq_fastapi.db import DB
from datetime import date, datetime
from typing import Union, Annotated
from openaq_fastapi.v3.models.responses import TrendsResponse

logger = logging.getLogger("trends")

from openaq_fastapi.v3.models.queries import (
    QueryBaseModel,
    QueryBuilder,
    DateFromQuery,
    DateToQuery,
    PeriodNameQuery,
    Paging,
)


router = APIRouter(
    prefix="/v3",
    tags=["v3-alpha"],
    include_in_schema=True,
)


class ParameterPathQuery(QueryBaseModel):
    measurands_id: int = Path(description="The parameter to query")

    def where(self) -> str:
        return "s.measurands_id = :measurands_id"


class LocationPathQuery(QueryBaseModel):
    locations_id: int = Path(
        description="Limit the results to a specific location by id", ge=1
    )

    def where(self) -> str:
        return "sy.sensor_nodes_id = :locations_id"


class LocationTrendsQueries(
    Paging,
    LocationPathQuery,
    ParameterPathQuery,
    DateFromQuery,
    DateToQuery,
    PeriodNameQuery,
):
    ...


@router.get(
    "/locations/{locations_id}/trends/{measurands_id}",
    response_model=TrendsResponse,
    summary="Get trends by location",
    description="Provides a list of aggregated measurements by location ID and factor",
)
async def trends_get(
    trends: Annotated[LocationTrendsQueries, Depends(LocationTrendsQueries)],
    db: DB = Depends(),
):
    response = await fetch_trends(trends, db)
    return response


async def fetch_trends(q, db):
    fmt = ""
    if q.period_name == "hour":
        fmt = "HH24"
        dur = "01:00:00"
    elif q.period_name == "day":
        fmt = "ID"
        dur = "24:00:00"
    elif q.period_name == "month":
        fmt = "MM"
        dur = "1 month"

    query = QueryBuilder(q)
    sql = f"""
WITH trends AS (
SELECT
  sn.id
 , s.measurands_id
 , sn.timezone
 , to_char(timezone(sn.timezone, datetime - '1sec'::interval), '{fmt}') as factor
 , AVG(s.data_averaging_period_seconds) as avg_seconds
 , AVG(s.data_logging_period_seconds) as log_seconds
 , MIN(datetime - '1sec'::interval) as datetime_from
 , MAX(datetime - '1sec'::interval) as datetime_to
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
 , jsonb_build_object(
    'label', factor
    , 'interval', '{dur}'
    , 'order', factor::int
 ) as factor
 , value_avg as value
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
  , expected_hours(datetime_from, datetime_to, '{q.period_name}', factor) * 3600.0
)||jsonb_build_object(
          'datetime_from', get_datetime_object(datetime_from, t.timezone)
        , 'datetime_to', get_datetime_object(datetime_to, t.timezone)
 ) as coverage
 FROM trends t
 JOIN measurands m ON (t.measurands_id = m.measurands_id)
 {query.pagination()}
    """

    logger.debug(
        f"expected_hours(datetime_from, datetime_to, '{q.period_name}', factor) * 3600.0"
    )

    response = await db.fetchPage(sql, query.params())
    return response
