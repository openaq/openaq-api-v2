from enum import Enum
import logging
import os
from typing import Annotated
import jq

import orjson as json
from dateutil.tz import UTC
from datetime import timedelta, datetime
from fastapi import APIRouter, Depends, Query
from starlette.responses import Response
from ..db import DB
from ..models.responses import MeasurementsResponse, MeasurementsResponseV1, Meta
from ..models.queries import (
    APIBase,
    City,
    Country,
    DateRange,
    Geo,
    HasGeo,
    Location,
    Measurands,
    Sort,
    SensorTypes,
    EntityTypes,
)
import csv
import io


logger = logging.getLogger("measurements")

router = APIRouter(
    include_in_schema=True,
)


def meas_csv(rows, includefields):
    output = io.StringIO()
    writer = csv.writer(output)
    header = [
        "locationId",
        "location",
        "city",
        "country",
        "utc",
        "local",
        "parameter",
        "value",
        "unit",
        "latitude",
        "longitude",
    ]
    # include_fields in csv header
    if includefields is not None:
        include_fields = includefields.split(",")
        available_fields = ["sourceName", "attribution", "averagingPeriod"]
        for f in include_fields:
            if f in available_fields:
                header.append(f)
    writer.writerow(header)
    for r in rows:
        try:
            row = [
                r["locationId"],
                r["location"],
                r["city"],
                r["country"],
                r["date"]["utc"],
                r["date"]["local"],
                r["parameter"],
                r["value"],
                r["unit"],
                r["coordinates"]["latitude"],
                r["coordinates"]["longitude"],
            ]
            # include_fields in csv data
            if includefields is not None:
                include_fields = includefields.split(",")
                available_fields = ["sourceName", "attribution", "averagingPeriod"]
                for f in include_fields:
                    if f in available_fields:
                        row.append(r["{}".format(f)])
            writer.writerow(row)
        except Exception as e:
            logger.debug(e)

    return output.getvalue()


class MeasOrder(str, Enum):
    city = "city"
    country = "country"
    location = "location"
    datetime = "datetime"


class Measurements(
    Location, City, Country, Geo, Measurands, HasGeo, APIBase, DateRange
):
    order_by: MeasOrder = Query("datetime")
    sort: Sort = Query("desc")
    isMobile: bool | None = Query(
        None, description="Location is mobile e.g. ?isMobile=true", examples=["true"]
    )
    isAnalysis: bool | None = Query(
        None,
        description="Data is the product of a previous analysis/aggregation and not raw measurements e.g. ?isAnalysis=false",
        examples=["true"],
    )
    project: int | None = Query(None)
    entity: EntityTypes | None = Query(None)
    sensorType: SensorTypes | None = Query(
        None,
        description="Filter by sensor type (i,e. reference grade, low-cost sensor) e.g. ?sensorType=reference%20grade",
        examples=["reference%20grade"],
    )
    value_from: float | None = Query(None, description="", example="")
    value_to: float | None = Query(None, description="", example="")
    include_fields: str | None = Query(
        None,
        description="Additional fields to include in response e.g. ?include_fields=sourceName",
        examples=["sourceName"],
    )

    def where(self):
        wheres = []
        if self.lon and self.lat:
            wheres.append(
                " st_dwithin(st_makepoint(:lon, :lat)::geography,"
                " sn.geom::geography, :radius) "
            )
        for f, v in self:
            if v is not None:
                if f == "location":
                    if all(isinstance(x, int) for x in v):
                        type = "int[]"
                        col = "sn.id"
                    else:
                        type = "text[]"
                        col = "sn.name"

                    if len(v) > 1:
                        clause = f"ANY(:location::{type})"
                    else:
                        clause = f"(:location::{type})[1]"

                    wheres.append(f" {col}={clause}")

                elif f == "parameter":
                    if all(isinstance(x, int) for x in v):
                        type = "int[]"
                        col = "m.measurands_id"
                    else:
                        type = "text[]"
                        col = "m.measurand"

                    if len(v) > 1:
                        clause = f"ANY(:parameter::{type})"
                    else:
                        clause = f"(:parameter::{type})[1]"

                    wheres.append(f" {col}={clause}")

                elif f == "unit":
                    wheres.append("units = ANY(:unit) ")
                elif f == "isMobile":
                    wheres.append("ismobile = :is_mobile ")
                elif f == "isAnalysis":
                    wheres.append("is_analysis = :is_analysis ")
                elif f == "entity":
                    wheres.append("sn.owner->>'type' ~* :entity ")
                elif f == "sensorType":
                    if v == "reference grade":
                        wheres.append("i.is_monitor")
                    elif v == "low-cost sensor":
                        wheres.append("NOT i.is_monitor")
                elif f == "country":
                    wheres.append(f"sn.country->>'code' = ANY(:{f})")
                elif f == "city":
                    wheres.append(f"{f} = ANY(:{f})")
                elif f == "date_from":
                    wheres.append("h.datetime > :date_from")
                elif f == "date_to":
                    wheres.append("h.datetime <= :date_to")

        wheres = list(filter(None, wheres))
        # wheres.append(" sensor_nodes_id not in (61485,61505,61506) ")
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get(
    "/v2/measurements",
    summary="Get measurements",
    description="",
    response_model=MeasurementsResponse,
    tags=["v2"],
)
async def measurements_get(
    m: Annotated[Measurements, Depends(Measurements)],
    db: DB = Depends(),
    format: str | None = None,
):
    where = m.where()
    params = m.params()
    includes = m.include_fields

    sql = f"""
        SELECT sn.id as "locationId"
        , COALESCE(sn.name, 'N/A') as location
        , get_datetime_object(h.datetime, sn.timezone) as date
        , m.measurand as parameter
        , m.units as unit
        , h.value_avg as value
        , json_build_object(
            'latitude', st_y(sn.geom),
             'longitude', st_x(sn.geom)
        ) as coordinates
        , sn.country->>'code' as country
        , sn.ismobile as "isMobile"
        , sn.owner->>'type' as entity
        , CASE WHEN i.is_monitor
               THEN 'reference grade'
               ELSE 'low-cost sensor'
               END as "sensorType"
        , sn.is_analysis
        FROM hourly_data h
        JOIN sensors s USING (sensors_id)
        JOIN sensor_systems sy USING (sensor_systems_id)
        JOIN instruments i USING (instruments_id)
        JOIN locations_view_cached sn ON (sy.sensor_nodes_id = sn.id)
        JOIN measurands m ON (m.measurands_id = h.measurands_id)
        WHERE {where}
        OFFSET :offset
        LIMIT :limit;
        """

    response = await db.fetchPage(sql, params)

    if format == "csv":
        return Response(
            content=meas_csv(response.results, includes),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment;filename=measurements.csv"},
        )

    return response


@router.get(
    "/v1/measurements",
    summary="Get a list of measurements",
    response_model=MeasurementsResponseV1,
    tags=["v1"],
)
async def measurements_get_v1(
    m: Annotated[Measurements, Depends(Measurements)],
    db: DB = Depends(),
    format: str | None = None,
):
    m.entity = "government"
    params = m.params()
    where = m.where()

    sql = f"""
        SELECT sn.id as "locationId"
        , COALESCE(sn.name, 'N/A') as location
        , get_datetime_object(h.datetime, sn.timezone) as date
        , m.measurand as parameter
        , m.units as unit
        , h.value_avg as value
        , json_build_object(
            'latitude', st_y(sn.geom),
             'longitude', st_x(sn.geom)
        ) as coordinates
        , c.iso as country
        FROM hourly_data h
        JOIN sensors s USING (sensors_id)
        JOIN sensor_systems sy USING (sensor_systems_id)
        JOIN instruments i USING (instruments_id)
        JOIN locations_view_cached sn ON (sy.sensor_nodes_id = sn.id)
        JOIN measurands m ON (m.measurands_id = h.measurands_id)
        JOIN countries c ON (c.countries_id = sn.countries_id)
        WHERE {where}
        OFFSET :offset
        LIMIT :limit
        """

    response = await db.fetchPage(sql, params)

    if format == "csv":
        return Response(
            content=meas_csv(response.results, m.include_fields),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment;filename=measurements.csv"},
        )

    return response
