import logging

from fastapi import APIRouter, Depends, Query
from openaq_fastapi.models.responses import OpenAQProjectsResult
from pydantic.typing import Literal

from ..db import DB
from ..models.queries import APIBase, Country, Measurands, Project

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


class Projects(Project, Measurands, APIBase, Country):
    order_by: Literal[
        "id", "name", "subtitle", "firstUpdated", "lastUpdated"
    ] = Query("lastUpdated")

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                logger.debug(f" setting where for {f} {v} ")
                if f == "project" and all(isinstance(x, int) for x in v):
                    logger.debug(" using int id")
                    wheres.append(" groups_id = ANY(:project) ")
                elif f == "project":
                    wheres.append(" g.name = ANY(:project) ")
                elif f == "units":
                    wheres.append(" units = ANY(:units) ")

                elif f == "parameter":
                    if all(isinstance(x, int) for x in v):
                        wheres.append(
                            """
                            measurands_id = ANY (:parameter)
                            """
                        )
                    else:
                        wheres.append(
                            """
                            measurand = ANY (:parameter)
                            """
                        )
                elif f == "country":
                    wheres.append(
                        """
                        countries && :country
                        """
                    )

                # elif isinstance(v, List):
                #    wheres.append(f"{f} = ANY(:{f})")

        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get(
    "/v2/projects/{project_id}",
    response_model=OpenAQProjectsResult,
    tags=["v2"],
)
@router.get("/v2/projects", response_model=OpenAQProjectsResult, tags=["v2"])
async def projects_get(
    db: DB = Depends(),
    projects: Projects = Depends(Projects.depends()),
):

    q = f"""
        WITH bysensor AS (
            SELECT
                groups_id as "id",
                g.name,
                subtitle,
                --geog,
                value_count as count,
                value_sum / value_count as average,
                locations,
                measurand as parameter,
                units as unit,
                measurands_id as "parameterId",
                sensor_nodes_arr as location_ids,
                sources(sensor_nodes_arr) as sources,
                countries,
                last(last_value, last_datetime) as "lastValue",
                max(last_datetime) as "lastUpdated",
                min(first_datetime) as "firstUpdated",
                min(minx) as minx,
                min(miny) as miny,
                max(maxx) as maxx,
                max(maxy) as maxy
            FROM
                rollups LEFT JOIN groups_view g
                USING (groups_id, measurands_id)
                --LEFT JOIN sources s ON (g.name=s.slug)
            WHERE
                g.type='organization' AND rollup='total'
                AND {projects.where()}
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
            ORDER BY "{projects.order_by}" {projects.sort}

        )
        , overall as (
        SELECT
            "id",
            name,
            subtitle,
            CASE WHEN min(minx) is null THEN null ELSE
            ARRAY[min(minx), min(miny), max(maxx), max(maxy)] END as bbox,
            array_agg(distinct sources) as sources,
            sum(count) as measurements,
            max(locations) as locations,
            max("lastUpdated") as "lastUpdated",
            min("firstUpdated") as "firstUpdated",
            array_merge_agg(DISTINCT location_ids) as "locationIds",
            array_merge_agg(DISTINCT countries) as countries,
            jsonb_agg(to_jsonb(bysensor) || parameter("parameterId") -
            '{{
                id,
                name,
                subtitle,
                geog,
                sources,
                location_ids,
                minx,
                miny,
                maxx,
                maxy,
                countries
            }}'::text[]) as parameters
            FROM bysensor
            GROUP BY 1,2,3
        )
        select count(*) OVER () as count,
        --jsonb_strip_nulls(
            to_jsonb(overall)
        --)
        as json
        from overall
        LIMIT :limit
        OFFSET :offset
            ;
    """

    output = await db.fetchOpenAQResult(q, projects.dict())

    return output
