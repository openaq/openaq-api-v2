import logging
from typing import Annotated, Any
from datetime import datetime, date

from fastapi import APIRouter, Depends, Path, Query
from fastapi.exceptions import RequestValidationError

from pydantic import model_validator

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    Paging,
    QueryBaseModel,
    QueryBuilder,
)

from openaq_api.v3.models.responses import (
    LocationFlagsResponse,
)

logger = logging.getLogger("flags")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
    include_in_schema=True,
)


class DatetimePeriodQuery(QueryBaseModel):
    datetime_from: datetime | date | None = Query(
        None,
        description="To when?",
        examples=["2022-10-01T11:19:38-06:00", "2022-10-01"],
    )
    datetime_to: datetime | date | None = Query(
        None,
        description="To when?",
        examples=["2022-10-01T11:19:38-06:00", "2022-10-01"],
    )

    @model_validator(mode="after")
    @classmethod
    def check_dates_are_in_order(cls, data: Any) -> Any:
        dt = getattr(data, "datetime_to")
        df = getattr(data, "datetime_from")
        if dt and df and dt <= df:
            raise RequestValidationError(
                f"Date/time from must be older than the date/time to. User passed {df} - {dt}"
            )

    def where(self) -> str:
        pd = self.map("period", "period")
        if self.datetime_to is None and self.datetime_from is None:
            return None
        if self.datetime_to is not None and self.datetime_from is not None:
            return f"{pd} && tstzrange(:datetime_from, :datetime_to, '[]')"
        elif self.datetime_to is not None:
            return f"{pd} && tstzrange('-infinity'::timestamptz, :datetime_to, '[]')"
        elif self.datetime_from is not None:
            return f"{pd} && tstzrange(:datetime_from, 'infinity'::timestamptz, '[]')"


class LocationFlagQuery(QueryBaseModel):
    locations_id: int = Path(
        ...,
        description="Limit the results to a specific locations",
        ge=1,
        le=2147483647,
    )

    def where(self):
        return "f.sensor_nodes_id = :locations_id"


class SensorFlagQuery(QueryBaseModel):
    sensor_id: int = Path(
        ..., description="Limit the results to a specific sensor", ge=1, le=2147483647
    )

    def where(self):
        return "ARRAY[:sensor_id::int] @> f.sensors_ids"


class LocationFlagQueries(LocationFlagQuery, DatetimePeriodQuery, Paging): ...


class SensorFlagQueries(SensorFlagQuery, DatetimePeriodQuery, Paging): ...


@router.get(
    "/locations/{locations_id}/flags",
    response_model=LocationFlagsResponse,
    summary="Get flags by location ID",
    description="Provides a list of flags by location ID",
)
async def location_flags_get(
    location_flags: Annotated[
        LocationFlagQueries, Depends(LocationFlagQueries.depends())
    ],
    db: DB = Depends(),
):
    return await fetch_flags(location_flags, db)


@router.get(
    "/sensors/{sensor_id}/flags",
    response_model=LocationFlagsResponse,
    summary="Get flags by sensor ID",
    description="Provides a list of flags by sensor ID",
)
async def sensor_flags_get(
    sensor_flags: Annotated[SensorFlagQueries, Depends(SensorFlagQueries.depends())],
    db: DB = Depends(),
):
    return await fetch_flags(sensor_flags, db)


async def fetch_flags(q, db):
    query = QueryBuilder(q)
    query.set_column_map({"timezone": "tz.tzid", "datetime": "lower(period)"})

    sql = f"""
    SELECT f.sensor_nodes_id as location_id
    , json_build_object('id', ft.flag_types_id, 'label', ft.label, 'level', ft.flag_level) as flag_type
    , sensors_ids
    , get_datetime_object(lower(f.period), t.tzid) as datetime_from
    , get_datetime_object(upper(f.period), t.tzid) as datetime_to
    , note
    FROM flags f
    JOIN flag_types ft ON (f.flag_types_id = ft.flag_types_id)
    JOIN sensor_nodes n ON (f.sensor_nodes_id = n.sensor_nodes_id)
    JOIN timezones t ON (n.timezones_id = t.timezones_id)
    {query.where()}
    """
    return await db.fetchPage(sql, query.params())
