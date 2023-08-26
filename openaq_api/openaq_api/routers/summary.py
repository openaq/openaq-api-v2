import logging

from fastapi import APIRouter, Depends

from ..db import DB
from ..models.responses import SummaryResponse

logger = logging.getLogger("summary")
logger.setLevel(logging.DEBUG)

router = APIRouter()


@router.get(
    "/v2/summary",
    response_model=SummaryResponse,
    summary="Platform Summary",
    description="Provides a summary of platform data",
    tags=["v2"],
)
async def summary_get(
    db: DB = Depends(),
):
    q = f"""
    WITH t AS (
    SELECT
        SUM(sr.value_count) AS count
        , COUNT(DISTINCT sn.sensor_nodes_id) AS locations
        , COUNT(DISTINCT sn.country) AS countries
        , COUNT(DISTINCT sn.city) AS cities
        , COUNT(DISTINCT sn.providers_id) AS sources
    FROM
        sensors_rollup sr
        JOIN sensors s ON sr.sensors_id = s.sensors_id
        JOIN sensor_systems ss ON s.sensor_systems_id = ss.sensor_systems_id
        JOIN sensor_nodes sn ON ss.sensor_nodes_id = sn.sensor_nodes_id
    )
    SELECT *
    FROM t;
    """

    output = await db.fetchPage(q, {"page": 1, "limit": 1000})

    return output
