import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import ParametersResponse
from openaq_fastapi.models.queries import OBaseModel, Geo

from openaq_fastapi.v3.models.queries import (
    QueryBaseModel,
    Paging,
)

logger = logging.getLogger("parameters")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
)


class ParameterQueries(QueryBaseModel):
    id: int = Path(
        description="Limit the results to a specific id",
        ge=1
    )

    def where(self):
        return "WHERE measurands_id = :id"


class ParametersQueries(Paging):
    def where(self):
        where = ["WHERE TRUE"]
        return ("\nAND ").join(where)


@router.get(
    "/parameters/{id}",
    response_model=ParametersResponse,
    summary="Get a parameter by ID",
    description="Provides a parameter by parameter ID",
)
async def parameter_get(
    parameter: ParameterQueries = Depends(ParameterQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_parameters(parameter, db)
    return response


@router.get(
    "/parameters",
    response_model=ParametersResponse,
    summary="Get a parameters",
    description="Provides a list of parameters",
)
async def parameters_get(
    parameter: ParametersQueries = Depends(ParametersQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_parameters(parameter, db)
    return response


async def fetch_parameters(query, db):
    sql = f"""
    SELECT measurands_id as id
    , measurand as name
    , units
    , description
    , display as display_name
    , 0 as locations_count
    , 0 as measurements_count
    {query.total()}
    FROM measurands
    {query.where()}
    {query.pagination()}
    """
    response = await db.fetchPage(sql, query.params())
    return response
