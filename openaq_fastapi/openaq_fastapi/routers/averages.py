import logging

from dateutil.tz import UTC
from fastapi import APIRouter, Depends, Query
from typing import Optional, List
from ..db import DB
from ..models.queries import (
    APIBase,
    Country,
    DateRange,
    Measurands,
    Project,
    Spatial,
    Temporal,
    Sort,
)
from openaq_fastapi.models.responses import OpenAQResult
from pydantic import root_validator

logger = logging.getLogger("averages")
logger.setLevel(logging.DEBUG)

router = APIRouter()


class Averages(APIBase, Country, Project, Measurands, DateRange):
    spatial: Spatial = Query(...)
    temporal: Temporal = Query(...)
    location: Optional[List[str]] = None
    group: Optional[bool] = False
    sort: Optional[Sort] = Query("desc", description="Define sort order.")

    def where(self):
        wheres = []
        if self.spatial == "country" and self.country is not None:
            wheres.append("name = ANY(:country)")
        if self.spatial == "project" and self.project is not None:
            if all(isinstance(x, int) for x in self.project):
                wheres.append("groups_id = ANY(:project)")
            else:
                wheres.append("name = ANY(:project)")
        if self.spatial == "location" and self.location is not None:
            wheres.append("name = ANY(:location)")
        if self.parameter is not None:
            if all(isinstance(x, int) for x in self.parameter):
                wheres.append(" measurands_id = ANY(:parameter) ")
            else:
                wheres.append(" measurand = ANY(:parameter) ")
        for f, v in self:
            if v is not None and f in ["units"]:
                wheres.append(f"{f} = ANY(:{f})")
        #wheres.append(" groups_id not in (28978,28972) ")
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "

    @root_validator
    def validate_date_range(cls, values):
        date_from = values.get("date_from")
        date_to = values.get("date_to")
        temporal = values.get("temporal")

        if (
            temporal in ["hour", "hod"]
            and (date_to - date_from).total_seconds() > 31 * 24 * 60 * 60
            and False # for testing purposes
        ):
            raise ValueError(
                "Date range cannot excede 1 month for hourly queries"
            )
        return values


@router.get("/v2/averages", response_model=OpenAQResult, tags=["v2"])
async def averages_v2_get(
    db: DB = Depends(),
    av: Averages = Depends(Averages.depends()),
):
    date_from = av.date_from
    date_to = av.date_to
    initwhere = av.where()
    qparams = av.dict(exclude_unset=True)

    if qparams["spatial"] == "project":
        qparams["spatial"] = "source"
    elif qparams["spatial"] == "location":
        qparams["spatial"] = "node"

    q = f"""
        SELECT
            min(first_datetime),
            max(last_datetime),
            count(distinct concat(groups_id,'~~~',measurands_id)) as groups
        FROM rollups
        LEFT JOIN groups_view USING (groups_id, measurands_id)
        WHERE
            rollup = 'total'
            AND
            type = :spatial::text
            AND
            {initwhere}
        """

    rows = await db.fetch(q, qparams)
    if rows is None:
        return OpenAQResult()
    try:
        range_start = rows[0][0].replace(tzinfo=UTC)
        range_end = rows[0][1].replace(tzinfo=UTC)
        count = rows[0][2]
    except Exception as e:
        logger.debug(f"exception setting range/count {e}")
        return OpenAQResult()

    if date_from is None:
        qparams["date_from"] = range_start
    else:
        qparams["date_from"] = max(date_from, range_start)

    if date_to is None:
        qparams["date_to"] = range_end
    else:
        qparams["date_to"] = min(date_to, range_end)

    if range_end < range_start:
        return OpenAQResult()

    temporal = av.temporal

    # estimate max number of rows to be returned
    hours = (range_end - range_start).total_seconds() / 3600
    logger.debug(
        "range_start %s range_end %s hours %s", range_start, range_end, hours
    )
    if av.temporal in ["hour"]:
        count = count * hours
    elif av.temporal in ["day"]:
        count = count * hours / 24
    elif av.temporal in ["month"]:
        count = count * hours / (24 * 30)
    elif av.temporal in ["year"]:
        count = count * hours / (24 * 365)
    elif av.temporal in ["hod"]:
        count * 24
    elif av.temporal in ["dom"]:
        count * 30
    else:
        count = count
    count = int(count)

    wrapper_start = ""
    wrapper_end = ""
    # groupby = "1,2,3,4,5,6,7"
    groupby = "1,2,3,4,5"
    if av.group:
        wrapper_start = "array_agg(DISTINCT "
        wrapper_end = ")"
        groupby = "1,2,3,4"

    if av.temporal in ["hour", "hod"]:
        if av.temporal == "hour":
            temporal_col = "date_trunc('hour', datetime)"
        else:
            temporal_col = "extract('hour' from datetime)"
        baseq = f"""
            SELECT
                measurands_id,
                {temporal_col} as {av.temporal},
                {temporal_col} as o,
                {temporal_col} as st,
                --{wrapper_start}groups_id{wrapper_end} as id,
                {wrapper_start}sensors_id{wrapper_end} as id,
                --{wrapper_start}name{wrapper_end} as name,
                --{wrapper_start}subtitle{wrapper_end} as subtitle,
                count(*) as measurement_count,
                round((sum(value)/count(*))::numeric, 4) as average
            FROM measurements
            JOIN sensor_stats_versioning USING (sensors_id)
            --LEFT JOIN sensors USING (sensors_id)
            --LEFT JOIN groups_sensors USING (sensors_id)
            --LEFT JOIN groups_view USING (groups_id, measurands_id)
            WHERE {initwhere}
            --AND type = :spatial::text
            AND datetime>=:date_from::timestamptz
            AND datetime<=:date_to::timestamptz
            GROUP BY {groupby}
            ORDER BY 4 {av.sort}
            OFFSET :offset
            LIMIT :limit
            """

    else:
        temporal_order = "st"
        temporal_col = "st::date"
        if av.group or av.temporal in ["dow", "moy"]:
            agg_clause = """
                sum(value_count) as measurement_count,
                round((sum(value_sum)/sum(value_count))::numeric, 4) as average
            """
        else:
            agg_clause = """
                value_count as measurement_count,
                round((value_sum/value_count)::numeric, 4) as average
            """

        if av.group:
            group_clause = " GROUP BY 1,2,3 "
        else:
            group_clause = " "

        if av.temporal == "moy":
            temporal = "month"
            temporal_order = "to_char(st, 'MM')"
            temporal_col = "to_char(st, 'Mon')"
        elif av.temporal == "dow":
            temporal = "day"
            temporal_col = "to_char(st, 'Dy')"
            temporal_order = "to_char(st, 'ID')"

        if av.temporal in ["dow", "moy"]:
            if av.group:
                group_clause = " GROUP BY 1,2,3 "
            else:
                group_clause = " GROUP BY 1,2,3,4,5,6 "

        where = f"""
            WHERE
                    rollup = :temporal::text
                    AND
                    type = :spatial::text
                    AND
                    st >= date_trunc(:temporal, :date_from::timestamptz)
                    AND
                    st <= date_trunc(:temporal, :date_to::timestamptz)
                    AND
                    {initwhere}
        """

        baseq = f"""
            SELECT
                measurands_id,
                {temporal_col} as {av.temporal},
                {temporal_order} as o,
                {wrapper_start}groups_id{wrapper_end} as id,
                {wrapper_start}name{wrapper_end} as name,
                {wrapper_start}subtitle{wrapper_end} as subtitle,
                {agg_clause}
            FROM rollups
            LEFT JOIN groups_view USING (groups_id, measurands_id)
            {where}
            {group_clause}
            ORDER BY 3 {av.sort}
            OFFSET :offset
            LIMIT :limit
        """

    qparams["count"] = count

    q = f"""
        WITH base AS (
            {baseq}
        )
        SELECT :count::bigint as count,
        (to_jsonb(base) ||
        parameter(measurands_id)) -
        '{{o,st, measurands_id}}'::text[]
        FROM base
        """
    av.temporal = temporal
    qparams["temporal"] = temporal
    output = await db.fetchOpenAQResult(q, qparams)

    return output
