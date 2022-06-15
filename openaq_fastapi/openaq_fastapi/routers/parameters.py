import logging

from fastapi import APIRouter, Depends, Query
from pydantic.typing import Literal

from ..db import DB
from ..models.queries import (
    APIBase,
    SourceName,
)

from openaq_fastapi.models.responses import (
    ParametersResponse, converter
)
import jq
logger = logging.getLogger("parameters")

router = APIRouter()


class Parameters(SourceName, APIBase):
    order_by: Literal["id", "name", "preferredUnit"] = Query("id")


@router.get(
    "/v2/parameters", 
    response_model=ParametersResponse, 
    summary="Get a list of parameters",
    tags=["v2"]
)
async def parameters_get(
    db: DB = Depends(),
    parameters: Parameters = Depends(Parameters.depends()),
):

    q = f"""
    WITH t AS (
    SELECT
        measurands_id as id,
        measurand as name,
        display as "displayName",
        coalesce(description, display) as description,
        units as "preferredUnit",
        is_core as "isCore",
        max_color_value as "maxColorValue"
    FROM measurands
    WHERE display is not null and is_core is not null
    ORDER BY "{parameters.order_by}" {parameters.sort}
    )
    SELECT count(*) OVER () as count,
    jsonb_strip_nulls(to_jsonb(t)) as json FROM t
    LIMIT :limit
    OFFSET :offset
    """

    output = await db.fetchOpenAQResult(q, parameters.params())

    return output


@router.get(
    "/v1/parameters", 
    response_model=ParametersResponse, 
    summary="Get a list of parameters",
    tags=["v1"]
)
async def parameters_getv1(
    db: DB = Depends(),
    parameters: Parameters = Depends(Parameters.depends()),
):
    data = await parameters_get(db, parameters)
    meta = data.meta
    res = data.results

    if len(res) == 0:
        return data

    v1_jq = jq.compile(
        """
        .[] | . as $m |
            {
                id: .id,
                name: .name,
                description: .description,
                preferredUnit: .preferredUnit
            }
        """
    )

    return converter(meta, res, v1_jq)
