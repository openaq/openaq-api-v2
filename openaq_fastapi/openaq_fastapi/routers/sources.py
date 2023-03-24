import logging
from typing import Union

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

from ..models.responses import SourcesResponse, SourcesResponseV1

logger = logging.getLogger("sources")

router = APIRouter()


class SourcesOrder(str, Enum):
    sourceName = "sourceName"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"


class Sources(SourceName, APIBase):
    order_by: SourcesOrder = Query(
        "sourceName",
        description="Field by which to order the results e.g. ?order_by=sourceName or ?order_by=firstUpdated",
        example="sourceName",
    )

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
    response_model=SourcesResponse,
    summary="Sources",
    description="Provides a list of sources",
    tags=["v2"],
)
async def sources_get(
    db: DB = Depends(),
    sources: Sources = Depends(Sources.depends()),
):
    qparams = sources.params()

    q = f"""
    WITH l AS 
    (
	SELECT 
		p.source_name AS "sourceName"
		, COUNT(*) as locations
	FROM 
		sensor_nodes sn
	JOIN 
		providers p ON (sn.providers_id = p.providers_id)
	GROUP BY 
		p.source_name
	)
    SELECT 
        p.metadata AS data
        , 
        CASE 
        WHEN p.readme IS NOT NULL 
        THEN
        '/v2/sources/readmes/' || p.slug
        ELSE NULL 
        END 
        AS readme
        , p.providers_id AS "sourceId"
        , l.locations
        , p.source_name AS "sourceName"
        , p.slug "sourceSlug"
    FROM
        providers p
        JOIN l ON (p.source_name = l."sourceName")
    GROUP BY 
        p.source_name
        , s.metadata
        , s.readme
        , p.providers_id
        , l.locations
        , s.slug
    """

    output = await db.fetchPage(q, qparams)

    return output


class SourcesV1Order(str, Enum):
    url = "url"
    adapter = "adapter"
    name = "name"
    city = "city"
    country = "country"
    description = "description"
    sourceURL = "sourceURL"
    resolution = "resolution"
    contacts = "contacts"
    active = "active"


class SourcesV1(APIBase):
    # name: Union[str, None] = None
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
    response_model=SourcesResponseV1,
    summary="Sources",
    description="Provides a list of sources",
    tags=["v1"],
)
async def sources_v1_get(
    db: DB = Depends(),
    sources: SourcesV1 = Depends(SourcesV1.depends()),
):

    if sources.order_by == "name":
        ob = "source_name"
    else:
        ob = sources.order_by

    q = f""" 
    WITH t AS 
	(
	SELECT
        COALESCE(sn.metadata -> 'attribution' -> 0 ->> 'url', '') AS url
        , a.name AS adapter
        , COALESCE( p.metadata ->> 'name', '' ) AS name
        , '' AS city
        , '' AS country
        , p.description AS description
        , COALESCE(p.metadata ->> 'url', '') AS source_url
        , COALESCE(p.metadata ->> 'resolution', '') AS resolution
        , jsonb_array_elements_text(COALESCE(p.metadata -> 'contacts', '[]')) AS contacts
        , p.is_active AS active
    FROM 
		sensor_nodes sn
	JOIN 
		providers p ON (sn.providers_id = p.providers_id)
    JOIN 
    	adapters a ON (p.adapters_id = a.adapters_id)
   	)
    SELECT DISTINCT
		url
		, adapter
		, name
		, city
		, country
		, description
		, source_url
		, resolution
		, array_agg(DISTINCT contacts) AS contacts
		, active
	FROM t
	GROUP BY
		name
		, adapter
		, url
		, city
		, country
        , description
		, source_url
		, resolution
		, active
    """

    qparams = sources.params()
    output = await db.fetchPage(q, qparams)
    return output


@router.get(
    "/v2/sources/readme/{slug}",
    summary="Source Readme",
    description="Provides a readme for a given source by the source slug",
    response_class=HTMLResponse,
    tags=["v2"],
)
async def readme_get(
    db: DB = Depends(),
    slug: str = Path(..., example="london_mobile"),
):
    q = """
        SELECT readme FROM sources WHERE slug=:slug
        """

    readme = await db.fetchval(q, {"slug": slug})
    if readme is None:
        raise HTTPException(status_code=404, detail=f"No readme found for {slug}.")

    readme = str.replace(readme, "\\", "")

    return HTMLResponse(content=markdown(readme), status_code=200)
