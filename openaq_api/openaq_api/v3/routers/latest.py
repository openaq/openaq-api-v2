from datetime import date, datetime
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query

from openaq_api.db import DB
from openaq_api.v3.models.queries import QueryBaseModel, QueryBuilder, Paging
from openaq_api.v3.models.responses import LatestResponse

logger = logging.getLogger("latest")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
    include_in_schema=True,
)


class DatetimeMinQuery(QueryBaseModel):
    """Pydantic query model for the `datetime_min` query parameter

    Inherits from QueryBaseModel

    Attributes:
        datetime_min: date or datetime in ISO-8601 format to filter results to a mininum data
    """

    datetime_min: datetime | date | None = Query(
        None,
        description="Minimum datetime",
        examples=["2022-10-01T11:19:38-06:00", "2022-10-01"],
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to datetime.

        Overrides the base QueryBaseModel `where` method

        If `datetime_min` is a `date` or `datetime` without a timezone a timezone
        is added as local timezone.

        Returns:
            string of WHERE clause if `datetime_min` is set
        """
        tz = self.map("timezone", "tzid")
        dt = self.map("datetime", "datetime_last")

        if self.datetime_min is None:
            return None
        elif isinstance(self.datetime_min, datetime):
            if self.datetime_min.tzinfo is None:
                return f"{dt} > (:datetime_min::timestamp AT TIME ZONE {tz})"
            else:
                return f"{dt} > :datetime_min"
        elif isinstance(self.datetime_min, date):
            return f"{dt} > (:datetime_min::timestamp AT TIME ZONE {tz})"


class ParameterLatestPathQuery(QueryBaseModel):
    """Path query to filter results by parameters ID

    Inherits from QueryBaseModel

    Attributes:
        parameters_id: countries ID value
    """

    parameters_id: int = Path(
        ..., description="Limit the results to a specific parameters id", ge=1
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to a single parameters_id

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause
        """
        return "m.measurands_id = :parameters_id"


class ParametersLatestQueries(ParameterLatestPathQuery, DatetimeMinQuery, Paging): ...


@router.get(
    "/parameters/{parameters_id}/latest",
    response_model=LatestResponse,
    summary="Get a owner by ID",
    description="Provides a owner by owner ID",
)
async def parameters_latest_get(
    parameters_latest: Annotated[
        ParametersLatestQueries, Depends(ParametersLatestQueries.depends())
    ],
    db: DB = Depends(),
):
    response = await fetch_latest(parameters_latest, db)
    return response


class LocationLatestPathQuery(QueryBaseModel):
    """Path query to filter results by locations ID.

    Inherits from QueryBaseModel.

    Attributes:
        locations_id: locations ID value.
    """

    locations_id: int = Path(
        description="Limit the results to a specific location by id", ge=1
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to a single locations_id

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause
        """
        return "n.sensor_nodes_id = :locations_id"


class LocationsLatestQueries(LocationLatestPathQuery, DatetimeMinQuery, Paging): ...


@router.get(
    "/locations/{locations_id}/latest",
    response_model=LatestResponse,
    summary="Get a owner by ID",
    description="Provides a owner by owner ID",
)
async def owner_get(
    locations_latest: Annotated[
        LocationsLatestQueries, Depends(LocationsLatestQueries.depends())
    ],
    db: DB = Depends(),
):
    response = await fetch_latest(locations_latest, db)
    return response


async def fetch_latest(query, db):
    query_builder = QueryBuilder(query)
    sql = f"""
    SELECT
      n.sensor_nodes_id AS locations_id
      ,s.sensors_id AS sensors_id
	  ,get_datetime_object(r.datetime_last, t.tzid) as datetime
      ,r.value_latest AS value
      ,json_build_object(
                'latitude', st_y(COALESCE(r.geom_latest, n.geom))
                ,'longitude', st_x(COALESCE(r.geom_latest, n.geom))
       ) AS coordinates
       {query_builder.total()}
    FROM 
        sensors s
    JOIN 
        sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
    JOIN 
        sensor_nodes n ON (sy.sensor_nodes_id = n.sensor_nodes_id)
    JOIN 
        timezones t ON (n.timezones_id = t.timezones_id)
    JOIN 
        measurands m ON (s.measurands_id = m.measurands_id)
    LEFT JOIN 
        sensors_rollup r ON (s.sensors_id = r.sensors_id)
    {query_builder.where()}
    {query_builder.pagination()}
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
