import logging

from fastapi import APIRouter, Depends, Query
from enum import Enum
from ..db import DB
from ..models.queries import APIBase, Country
from openaq_fastapi.models.responses import (
    OpenAQResult,
)

logger = logging.getLogger("summary")
logger.setLevel(logging.DEBUG)

router = APIRouter()






@router.get(
    "/v2/summary",
    response_model=OpenAQResult,
    tags=["v2"],
)
async def summary_get(
    db: DB = Depends(),
):
    q = f"""
    WITH t as (
        SELECT
            approximate_row_count('measurements') as count,
            count(distinct sensor_nodes_id) as locations,
            count(distinct country) as countries,
            count(distinct city) as cities,
            count(distinct sources_id) as sources
        FROM
            sensor_nodes
            LEFT JOIN sensor_nodes_sources USING (sensor_nodes_id)
    ) SELECT 1 as count, to_jsonb(t) as json FROM t
;

    """

    output = await db.fetchOpenAQResult(q, {"page": 1, "limit": 1000})

    return output
