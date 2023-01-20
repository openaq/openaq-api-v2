from fastapi import APIRouter, Depends, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import TrendsResponse

from openaq_fastapi.v3.models.queries import (
    QueryBaseModel,
    Paging,
)


router = APIRouter(
    prefix="/v3",
    tags=["v3"]
)


class LocationPathQuery(QueryBaseModel):
    locations_id: int = Path(
        description="Limit the results to a specific location by id", ge=1
    )

    def where(self) -> str:
        return "id = :locations_id"


class LocationTrendsQueries(
        Paging,
        LocationPathQuery,
):
    ...


@router.get(
    "/locations/{locations_id}/trends",
    response_model=TrendsResponse,
    summary="Get trends by location",
    description="Provides a list of aggregated measurements by location ID and factor",
)
async def trends_get(
    trends: LocationTrendsQueries = Depends(LocationTrendsQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_trends(trends, db)
    return response


async def fetch_trends(query, db):
    sql = f"""
    SELECT sensors_id
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
        'min', h.value_min
      , 'q05', h.value_p05
      , 'median', h.value_p50
      , 'q95', h.value_p95
      , 'max', h.value_max
      , 'sd', h.value_sd
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
    response = await db.fetchPage(sql, query.params())
    return response
