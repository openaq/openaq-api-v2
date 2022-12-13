import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import ParametersResponse
from openaq_fastapi.models.queries import OBaseModel, Geo

from openaq_fastapi.v3.models.queries import (
    Paging,
)

logger = logging.getLogger("parameters")

router = APIRouter(prefix="/v3", tags=["v3"])


class ParametersQueries(Paging):
    def fields(self):
        fields = []
        return ", " + (",").join(fields) if len(fields) > 0 else ""

    def clause(self):
        where = ["WHERE TRUE"]
        return ("\nAND ").join(where)


@router.get(
    "/parameters/{id}",
    response_model=ParametersResponse,
    summary="Get a parameter by ID",
    description="Provides a parameter by parameter ID",
)
async def parameter_get(
    parameter: ParametersQueries = Depends(ParametersQueries.depends()),
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


async def fetch_parameters(where, db):
    sql = f"""
    """
    response = await db.fetchPage(sql, where.params())
    return response
