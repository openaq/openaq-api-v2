import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import ParametersResponse
from openaq_fastapi.models.queries import OBaseModel, Geo

from openaq_fastapi.v3.models.queries import (
    QueryBuilder,
    QueryBaseModel,
    CountryQuery,
    BboxQuery,
    RadiusQuery,
    Paging,
)

logger = logging.getLogger("parameters")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
)


class ParameterQuery(QueryBaseModel):
    parameters_id: int = Path(description="Limit the results to a specific id", ge=1)

    def where(self):
        return "WHERE measurands_id = :parameters_id"


class ParametersQueries(Paging, CountryQuery, BboxQuery, RadiusQuery):
    ...


@router.get(
    "/parameters/{parameters_id}",
    response_model=ParametersResponse,
    summary="Get a parameter by ID",
    description="Provides a parameter by parameter ID",
)
async def parameter_get(
    parameter: ParameterQuery = Depends(ParameterQuery.depends()),
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
    parameter: ParametersQueries = Depends(ParametersQueries),
    db: DB = Depends(),
):
    response = await fetch_parameters(parameter, db)
    return response


async def fetch_parameters(query, db):
    query_builder = QueryBuilder(query)
    sql = f"""
    SELECT measurands_id as id
    , measurand as name
    , units
    , description
    , display as display_name
    , 0 as locations_count
    , 0 as measurements_count
    {query_builder.total()}
    FROM measurands
    {query_builder.where()}
    {query_builder.pagination()}
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
