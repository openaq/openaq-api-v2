import logging

from fastapi import APIRouter, Depends, Query, Path
from typing import Annotated
from openaq_fastapi.models.responses import ProjectsResponse
from typing import Union, List

from ..db import DB
from ..models.queries import APIBase, Country, Measurands, Project
from enum import Enum

logger = logging.getLogger("projects")

router = APIRouter()


class ProjectsOrder(str, Enum):
    id = "id"
    name = "name"
    subtitle = "subtitle"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"


class Projects(Project, Measurands, APIBase, Country):
    order_by: ProjectsOrder = Query("lastUpdated")
    isMobile: Union[bool, None] = None
    isAnalysis: Union[bool, None] = None
    entity: Union[str, None] = None
    sensorType: Union[str, None] = None
    sourceName: Union[List[str], None] = None

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
                elif f == "isMobile":
                    wheres.append(
                        """
                        "isMobile" = :is_mobile
                        """
                    )
                elif f == "isAnalysis":
                    wheres.append(
                        """
                        "isAnalysis" = :is_analysis
                        """
                    )
                elif f == "entity":
                    wheres.append(
                        """
                        "entity" = :entity
                        """
                    )
                elif f == "sensorType":
                    wheres.append(
                        """
                        "sensorType" = :sensor_type
                        """
                    )
                elif f == "sourceName":
                    wheres.append(
                        """
                        source_in_nodes(sensor_nodes_arr, :source_name)
                        """
                    )

                # elif isinstance(v, List):
                #    wheres.append(f"{f} = ANY(:{f})")

        wheres.append(" groups_id not in (28978,28972) ")

        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get(
    "/v2/projects/{project_id}",
    response_model=ProjectsResponse,
    summary="Project by ID",
    description="Provides a project by project ID",
    tags=["v2"],
)
async def projects_get(
    projects: Annotated[Projects, Depends(Projects)],
    db: DB = Depends(),
):
    ...
    # TODO implement projects/{project_id}


@router.get(
    "/v2/projects",
    response_model=ProjectsResponse,
    summary="Projects",
    description="Provides a list of projects",
    tags=["v2"],
)
async def projects_get(
    projects: Annotated[Projects, Depends(Projects)],
    db: DB = Depends(),
):
    q = f"""
        WITH bysensor AS (
            SELECT
                groups_id as "id",
                g.name,
                subtitle,
                "isMobile",
                "entity",
                "sensorType",
                "isAnalysis",
                locations,
                measurand as parameter,
                units as unit,
                measurands_id as "parameterId",
                sensor_nodes_arr as location_ids,
                sources(sensor_nodes_arr) as sources,
                countries,
                sum(value_count) as count,
                sum(value_sum) / sum(value_count) as average,
                last(last_value, last_datetime) as "lastValue",
                max(last_datetime) as "lastUpdated",
                min(first_datetime) as "firstUpdated",
                min(minx) as minx,
                min(miny) as miny,
                max(maxx) as maxx,
                max(maxy) as maxy
            FROM
                --rollups
                sensor_stats
                LEFT JOIN groups_sensors using (sensors_id)
                LEFT JOIN groups_view g
                USING (groups_id, measurands_id)
            WHERE
                g.type='source'
                AND {projects.where()}
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            ORDER BY "{projects.order_by}" {projects.sort}

        )
        , overall as (
        SELECT
            "id",
            name,
            subtitle,
            "isMobile",
            "entity",
            "sensorType",
            "isAnalysis",
            CASE WHEN min(minx) is null THEN null ELSE
            ARRAY[min(minx), min(miny), max(maxx), max(maxy)] END as bbox,
            sources,
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
            GROUP BY 1,2,3,4,5,6,7,9
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

    output = await db.fetchOpenAQResult(q, projects.params())

    return output
