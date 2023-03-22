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

    #
    q = f"""
    
    WITH l AS (
	SELECT
		p.source_name AS "sourceName"
		, COUNT(DISTINCT sn.sensor_nodes_id) AS locations
	FROM
		sensors_rollup sr
		JOIN sensors s USING (sensors_id)
		JOIN sensor_systems ss USING (sensor_systems_id)
		JOIN sensor_nodes sn USING (sensor_nodes_id)
		JOIN providers p USING (providers_id)
		JOIN adapters a ON (p.adapters_id = a.adapters_id)
	WHERE
		p.source_name IS NOT NULL
	GROUP BY
		p.source_name
)
SELECT DISTINCT ON (p.source_name)
	json_build_object(
		'url', COALESCE(sn.metadata -> 'attribution' -> 0 ->> 'url', '')
		, 'data_avg_dur', NULL
		, 'organization', NULL
		, 'lifecycle_stage', CASE
			WHEN (sn.metadata ->> 'is_analysis'::text)::boolean THEN 'Analysis result'
			ELSE NULL
		END
	) AS data
	, NULL AS readme
	, '42'::INTEGER AS "sourceId"
	, l.locations
	, p.source_name AS "sourceName"
	, NULL AS "sourceSlug"
FROM
	sensors_rollup sr
	JOIN sensors s USING (sensors_id)
	JOIN sensor_systems ss USING (sensor_systems_id)
	JOIN sensor_nodes sn USING (sensor_nodes_id)
	JOIN providers p USING (providers_id)
	JOIN adapters a ON (p.adapters_id = a.adapters_id)
	JOIN l ON (p.source_name = l."sourceName")
GROUP BY
	p.source_name
	, sn.source_id
	, sn.metadata
	, l.locations
ORDER BY
	p.source_name

    """

    old_q = f"""
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

        ## NEEDS WORK
    q = f""" 
    WITH t AS (SELECT
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
        sensors_rollup sr
        JOIN sensors s USING (sensors_id)
        JOIN sensor_systems ss USING (sensor_systems_id)
        JOIN sensor_nodes sn USING (sensor_nodes_id)
        JOIN providers p USING (providers_id)
        JOIN adapters a ON (p.adapters_id = a.adapters_id)
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
		url
		, adapter
		, name
		, city
		, country
        , description
		, source_url
		, resolution
		, active
    """
    old_query = f"""
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
    print("did we make it?")
    qparams = sources.params()
    output = await db.fetchPage(q, qparams)
    print("output:", output)
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
