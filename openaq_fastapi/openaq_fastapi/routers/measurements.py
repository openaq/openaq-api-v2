from enum import Enum
import logging
import os
from typing import Union
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

from openaq_fastapi.models.responses import OpenAQResult, converter

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
    isMobile: Union[bool, None] = Query(
        None, description="Location is mobile e.g. ?isMobile=true", example="true"
    )
    isAnalysis: Union[bool, None] = Query(
        None,
        description="Data is the product of a previous analysis/aggregation and not raw measurements e.g. ?isAnalysis=false",
        example="true",
    )
    project: Union[int, None] = Query(None)
    entity: Union[EntityTypes, None] = Query(None)
    sensorType: Union[SensorTypes, None] = Query(
        None,
        description="Filter by sensor type (i,e. reference grade, low-cost sensor) e.g. ?sensorType=reference%20grade",
        example="reference%20grade",
    )
    value_from: Union[float, None] = Query(None, description="", example="")
    value_to: Union[float, None] = Query(None, description="", example="")
    include_fields: Union[str, None] = Query(
        None,
        description="Additional fields to include in response e.g. ?include_fields=sourceName",
        example="sourceName",
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
                if f == "location" and all(isinstance(x, int) for x in v):
                    wheres.append(" sn.sensor_nodes_id = ANY(:location) ")
                elif f == "location":
                    wheres.append(" site_name = ANY(:location) ")
                elif f == "parameter":
                    if all(isinstance(x, int) for x in v):
                        wheres.append(
                            """
                            m.measurands_id = ANY(:parameter::int[])
                            """
                        )
                    else:
                        wheres.append(
                            """
                            m.measurand = ANY(:parameter::text[])
                            """
                        )
                elif f == "unit":
                    wheres.append("units = ANY(:unit) ")
                elif f == "isMobile":
                    wheres.append("ismobile = :is_mobile ")
                elif f == "isAnalysis":
                    wheres.append("is_analysis = :is_analysis ")
                elif f == "entity":
                    wheres.append("e.entity_type::text ~* :entity ")
                elif f == "sensorType":
                    wheres.append('b."sensorType" = :sensor_type ')
                elif f in ["country", "city"]:
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
    db: DB = Depends(),
    m: Measurements = Depends(Measurements.depends()),
    format: Union[str, None] = None,
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
            'latitude', st_y(geom),
             'longitude', st_x(geom)
        ) as coordinates
        , 'NA' as country
        , sn.ismobile as "isMobile"
        , sn.owner->>'type' as entity
        , CASE WHEN i.is_monitor
               THEN 'reference grade'
               ELSE 'low-cost sensor'
               END as "sensorType"
        , sn.is_analysis
        , COUNT(1) OVER() as found
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
    rows = await db.fetch(q, params)

    if rows is None:
        return OpenAQResult()
    try:
        total_count = int(rows[0][0])
        range_start = rows[0][1].replace(tzinfo=UTC)
        range_end = rows[0][2].replace(tzinfo=UTC)
        # sensor_nodes = rows[0][3]
    except Exception:
        return OpenAQResult()

    response = await db.fetchPage(sql, params)

    # meta = Meta(
    #     website=os.getenv("DOMAIN_NAME", os.getenv("BASE_URL", "/")),
    #     page=m.page,
    #     limit=m.limit,
    #     found=count,
    # )

    if format == "csv":
        return Response(
            content=meas_csv(response.results, includes),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment;filename=measurements.csv"},
        )

    ##output = OpenAQResult(meta=meta, results=results)

    # output = await db.fetchOpenAQResult(q, m.dict())

    return response


@router.get(
    "/v1/measurements",
    summary="Get a list of measurements",
    response_model=MeasurementsResponseV1,
    tags=["v1"],
)
async def measurements_get_v1(
    db: DB = Depends(),
    m: Measurements = Depends(Measurements.depends()),
    format: Union[str, None] = None,
):
    m.entity = "government"
    params = m.params()
    where = m.where()

    sql = f"""
        SELECT sn.sensor_nodes_id as "locationId"
        , COALESCE(site_name, 'N/A') as location
        , get_datetime_object(h.datetime, tzid) as date
        , m.measurand as parameter
        , m.units as unit
        , h.value_avg as value
        , json_build_object(
            'latitude', st_y(sn.geom),
             'longitude', st_x(sn.geom)
        ) as coordinates
        , c.iso as country
        , COUNT(1) OVER() as found
        FROM hourly_data h
        JOIN sensors s USING (sensors_id)
        JOIN sensor_systems sy USING (sensor_systems_id)
        JOIN instruments i USING (instruments_id)
        JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
        JOIN entities e ON (sn.owner_entities_id = e.entities_id)
        JOIN timezones ts ON (sn.timezones_id = ts.gid)
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
