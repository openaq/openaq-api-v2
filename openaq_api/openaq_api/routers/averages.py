import logging
from datetime import date, datetime
from enum import StrEnum
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from openaq_api.v3.models.queries import (
    Paging,
    TemporalQuery,
    QueryBaseModel,
    QueryBuilder,
)

from ..db import DB
from ..models.responses import AveragesResponse

logger = logging.getLogger("averages")

router = APIRouter(deprecated=True)


class DateFromQuery(QueryBaseModel):
    """Pydantic query model for the `datetime_from` query parameter

    Inherits from QueryBaseModel

    Attributes:
        datetime_from: date or datetime in ISO-8601 format to filter results to a
        date range.
    """

    date_from: datetime | date | None = Query(
        None,
        description="From when?",
        examples=["2022-10-01T11:19:38-06:00", "2022-10-01"],
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to datetime.

        Overrides the base QueryBaseModel `where` method

        If `date_from` is a `date` or `datetime` without a timezone a timezone
        is added as UTC.

        Returns:
            string of WHERE clause if `date_from` is set
        """
        tz = self.map("timezone", "timezone")
        dt = self.map("datetime", "datetime")

        if self.date_from is None:
            return None
        elif isinstance(self.date_from, datetime):
            if self.date_from.tzinfo is None:
                return f"{dt} > (:date_from::timestamp AT TIME ZONE {tz})"
            else:
                return f"{dt} > :date_from"
        elif isinstance(self.date_from, date):
            return f"{dt} > (:date_from::timestamp AT TIME ZONE {tz})"


class DateToQuery(QueryBaseModel):
    """Pydantic query model for the `date_to` query parameter

    Inherits from QueryBaseModel

    Attributes:
        date_to: date or datetime in ISO-8601 format to filter results to a
        date range.
    """

    date_to: datetime | date | None = Query(
        None,
        description="To when?",
        examples=["2022-10-01T11:19:38-06:00", "2022-10-01"],
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to datetime.

        Overrides the base QueryBaseModel `where` method

        If `date_to` is a `date` or `datetime` without a timezone a timezone
        is added as UTC.

        Returns:
            string of WHERE clause if `date_to` is set
        """
        tz = self.map("timezone", "timezone")
        dt = self.map("datetime", "datetime")
        if self.date_to is None:
            return None
        elif isinstance(self.date_to, datetime):
            if self.date_to.tzinfo is None:
                return f"{dt} <= (:date_to::timestamp AT TIME ZONE {tz})"
            else:
                return f"{dt} <= :date_to"
        elif isinstance(self.date_to, date):
            return f"{dt} <= (:date_to::timestamp AT TIME ZONE {tz})"


class SpatialTypes(StrEnum):
    country = "country"
    location = "location"
    total = "total"


class SpatialTypeQuery(QueryBaseModel):
    spatial: SpatialTypes | None = Query(
        "location", description="Define how you want to aggregate in space"
    )


class LocationQuery(QueryBaseModel):
    locations_id: int = Query(
        70084,
        description="Limit the results to a specific location by id",
        ge=1,
    )

    def where(self) -> str:
        return "sy.sensor_nodes_id = :locations_id"


class ParametersQuery(QueryBaseModel):
    parameters_id: int | None = Query(
        None,
        description="What measurand would you like?",
    )

    def where(self) -> str | None:
        if self.has("parameters_id"):
            return "m.measurands_id = :parameters_id"


class AveragesQueries(
    Paging,
    SpatialTypeQuery,
    LocationQuery,
    DateFromQuery,
    DateToQuery,
    ParametersQuery,
    TemporalQuery,
): ...


@router.get(
    "/v2/averages",
    response_model=AveragesResponse,
    summary="Get averaged values",
    description="",
    tags=["v2"],
)
async def averages_v2_get(
    av: Annotated[AveragesQueries, Depends(AveragesQueries.depends())],
    db: DB = Depends(),
) -> AveragesResponse:
    query = QueryBuilder(av)
    config = None

    if av.temporal in [None, "hour"]:
        # Query for hourly data
        sql = f"""
        SELECT sn.id
        , sn.name
        , datetime as hour
        , datetime::date as day
        , date_trunc('month', datetime)::date as month
        , date_trunc('year', datetime)::date as year
        , to_char(datetime, 'HH24') as hod
        , to_char(datetime, 'ID') as dow
        , sig_digits(h.value_avg, 2)::float as average
        , h.value_count as measurement_count
        , m.measurand as parameter
        , m.measurands_id as "parameterId"
        , m.display as "displayName"
        , m.units as unit
        , h.datetime_first as first_datetime
        , h.datetime_last as last_datetime
        {query.fields()}
        FROM hourly_data h
        JOIN sensors s USING (sensors_id)
        JOIN sensor_systems sy USING (sensor_systems_id)
        JOIN locations_view_cached sn ON (sy.sensor_nodes_id = sn.id)
        JOIN measurands m ON (m.measurands_id = h.measurands_id)
        {query.where()}
        {query.pagination()}
        """
    else:
        # Query for the aggregate data
        if av.temporal == "day":
            factor = "datetime::date as day"
        elif av.temporal == "month":
            factor = "date_trunc('month', datetime - '1sec'::interval) as month"
        elif av.temporal == "year":
            factor = "date_trunc('year', datetime - '1sec'::interval) as year"
        elif av.temporal == "hod":
            factor = "to_char(datetime - '1sec'::interval, 'HH24') as hod"
        elif av.temporal == "dow":
            factor = "to_char(datetime - '1sec'::interval, 'ID') as dow"
        elif av.temporal == "moy":
            factor = "to_char(datetime - '1sec'::interval, 'MM') as moy"

        config = {"work_mem": "512MB"}
        sql = f"""
        SELECT sn.id
        , sn.name
        , {factor}
        , m.measurand as parameter
        , m.measurands_id as "parameterId"
        , m.display as "displayName"
        , m.units as unit
        , AVG(h.value_avg)::float as average
        , COUNT(1) as measurement_count
        , MIN(datetime) as first_datetime
        , MAX(datetime) as last_datetime
        FROM hourly_data h
        JOIN sensors s ON (h.sensors_id = s.sensors_id)
        JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
        JOIN locations_view_cached sn ON (sy.sensor_nodes_id = sn.id)
        JOIN measurands m ON (h.measurands_id = m.measurands_id)
        {query.where()}
        GROUP BY 1, 2, 3, 4, 5, 6, 7
        {query.pagination()}
    """

    response = await db.fetchPage(sql, query.params(), config=config)
    return response
