import logging
from typing import Optional

from fastapi import APIRouter, Depends, Path, Query
from fastapi.responses import HTMLResponse
from markdown import markdown
from starlette.exceptions import HTTPException
from enum import Enum
from ..db import DB
from ..models.queries import (
    APIBase,
    SourceName,
)

from openaq_fastapi.models.responses import (
    OpenAQResult,
)

logger = logging.getLogger("sources")

router = APIRouter()


class SourcesOrder(str, Enum):
    sourceName = "sourceName"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"


class Sources(SourceName, APIBase):
    order_by: SourcesOrder = Query("sourceName")

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                logger.debug(f" setting where for {f} {v} ")
                if f == "sourceId":
                    wheres.append(" sources_id = ANY(:source_id) ")
                elif f == "sourceName":
                    wheres.append(" sources.name = ANY(:source_name) ")
                elif f == "sourceSlug":
                    wheres.append(" sources.slug = ANY(:source_slug) ")
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get(
    "/v2/sources", 
    response_model=OpenAQResult, 
    summary="Provides a list of sources",
    tags=["v2"]
)
async def sources_get(
    db: DB = Depends(),
    sources: Sources = Depends(Sources.depends()),
):
    qparams = sources.params()

    #
    q = f"""
    WITH t AS (
    SELECT
        sources_id as "sourceId",
        slug as "sourceSlug",
        sources.name as "sourceName",
        sources.metadata as data,
        case when readme is not null then
        '/v2/sources/readmes/' || slug
        else null end as readme,
        --sum(value_count) as count,
        count(*) as locations
        --to_char(min(first_datetime),'YYYY-MM-DD') as "firstUpdated",
        --to_char(max(last_datetime), 'YYYY-MM-DD') as "lastUpdated",
        --array_agg(DISTINCT measurand) as parameters
    FROM sources
    LEFT JOIN sensor_nodes_sources USING (sources_id)
    LEFT JOIN sensor_systems USING (sensor_nodes_id)
    LEFT JOIN sensors USING (sensor_systems_id)
    --LEFT JOIN rollups USING (sensors_id, measurands_id)
    --LEFT JOIN groups_view USING (groups_id, measurands_id)
    --WHERE rollup='total' AND groups_view.type='node'
    WHERE {sources.where()}
    GROUP BY
    1,2,3,4,5
    ORDER BY "{sources.order_by}" {sources.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count,
        to_jsonb(t) FROM t;
    """

    output = await db.fetchOpenAQResult(q, qparams)

    return output


class SourcesV1Order(str, Enum):
    name = "name"


class SourcesV1(APIBase):
    name: Optional[str] = None
    order_by: SourcesV1Order = Query("name")

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                if f == "name":
                    wheres.append(" source_name = ANY(:name) ")
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get(
    "/v1/sources", 
    response_model=OpenAQResult, 
    summary="Provides a list of sources",
    tags=["v1"]
)
async def sources_v1_get(
    db: DB = Depends(),
    sources: SourcesV1 = Depends(SourcesV1.depends()),
):
    qparams = sources.params()

    if sources.order_by == "name":
        ob = "source_name"
    else:
        ob = sources.order_by

    q = f"""
    WITH t AS (
    SELECT
        data
    FROM sources_from_openaq
    WHERE {sources.where()}
    ORDER BY {ob}
    LIMIT :limit
    OFFSET :offset
    )
    SELECT count(*) OVER () as count,
        data FROM t;
    """

    output = await db.fetchOpenAQResult(q, qparams)

    return output


@router.get(
    "/v2/sources/readme/{slug}", 
    summary="Provides a list of parameters",
    tags=["v2"]
)
async def readme_get(
    db: DB = Depends(),
    slug: str = Path(...),
):
    q = """
        SELECT readme FROM sources WHERE slug=:slug
        """

    readme = await db.fetchval(q, {"slug": slug})
    if readme is None:
        raise HTTPException(
            status_code=404, detail=f"No readme found for {slug}."
        )

    readme = str.replace(readme, "\\", "")

    return HTMLResponse(content=markdown(readme), status_code=200)
