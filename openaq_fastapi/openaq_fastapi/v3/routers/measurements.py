from fastapi import APIRouter, Depends
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import MeasurementsResponse
from openaq_fastapi.v3.models.queries import MeasurementsQueries

router = APIRouter(
    prefix="/v3",
    tags=["v3"]
)


@router.get(
    "/locations/{locations_id}/measurements",
    response_model=MeasurementsResponse,
    summary="Get measurements by location",
    description="Provides a list of measurements by location ID",
)
async def measurements_get(
    measurements: MeasurementsQueries = Depends(MeasurementsQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_measurements(measurements, db)
    return response


async def fetch_measurements(query, db):
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
    , json_build_object(
        'observed_count', h.value_count
      , 'datetime_first', get_datetime_object(h.first_datetime, 'utc')
      , 'datetime_last', get_datetime_object(h.last_datetime, 'utc')
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
