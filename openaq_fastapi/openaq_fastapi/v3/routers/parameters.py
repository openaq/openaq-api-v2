import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import ParametersResponse

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


class ParameterPathQuery(QueryBaseModel):
    parameters_id: int = Path(
        description="Limit the results to a specific parameters id", ge=1
    )

    def where(self) -> str:
        return "WHERE id = :parameters_id"


class ParametersQueries(Paging, CountryQuery, BboxQuery, RadiusQuery):
    ...


@router.get(
    "/parameters/{parameters_id}",
    response_model=ParametersResponse,
    summary="Get a parameter by ID",
    description="Provides a parameter by parameter ID",
)
async def parameter_get(
    parameter: ParameterPathQuery = Depends(ParameterPathQuery.depends()),
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
    SELECT id
    , name
    , display_name
    , units
    , description
    , locations_count
    , measurements_count
    {query_builder.total()}
    FROM parameters_view_cached
    {query_builder.where()}
    {query_builder.pagination()}
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
