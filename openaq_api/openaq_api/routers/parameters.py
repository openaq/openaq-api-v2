import logging
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query

from openaq_api.models.responses import ParametersResponse, ParametersResponseV1

from ..db import DB
from ..models.queries import APIBase

logger = logging.getLogger("parameters")

router = APIRouter()


class ParametersV1(APIBase):
    order_by: Literal["id", "name", "preferredUnit"] = Query("id")


class Parameters(APIBase):
    order_by: Literal["id", "name", "preferredUnit"] = Query("id")


@router.get(
    "/v2/parameters",
    response_model=ParametersResponse,
    summary="Get parameters",
    description="Provides a list of parameters supported by the platform",
    tags=["v2"],
)
async def parameters_get(
    parameters: Annotated[Parameters, Depends(Parameters.depends())],
    db: DB = Depends(),
):
    q = f"""
    SELECT
        measurands_id as id
        , measurand as name
        , display as "displayName"
        , coalesce(description, display, 'n/a') as description
        , units as "preferredUnit"
        , COUNT(1) OVER() as found
    FROM
        measurands
    ORDER BY "{parameters.order_by}" {parameters.sort}
    LIMIT :limit
    OFFSET :offset
    """

    output = await db.fetchPage(q, parameters.params())

    return output


@router.get(
    "/v1/parameters",
    response_model=ParametersResponseV1,
    summary="Get parameters",
    description="Provides a list of parameters supported by the platform",
    tags=["v1"],
)
async def parameters_getv1(
    parameters: Annotated[ParametersV1, Depends(ParametersV1.depends())],
    db: DB = Depends(),
):
    q = f"""
    SELECT
        measurands_id as id
        , measurand as name
        , display as "displayName"
        , coalesce(description, display) as description
        , units as "preferredUnit"
        , COUNT(1) OVER() as found
    FROM measurands
    ORDER BY "{parameters.order_by}" {parameters.sort}
    LIMIT :limit
    OFFSET :offset
    """

    output = await db.fetchPage(q, parameters.params())
    return output
