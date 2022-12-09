import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import ManufacturersResponse
from openaq_fastapi.models.queries import OBaseModel, Geo

from openaq_fastapi.v3.models.queries import (
    SQL,
    Paging,
)

logger = logging.getLogger("manufacturers")

router = APIRouter(prefix="/v3", tags=["v3"])


class ManufacturersQueries(Paging, SQL):
    def fields(self):
        fields = []
        return ", " + (",").join(fields) if len(fields) > 0 else ""

    def clause(self):
        where = ["WHERE TRUE"]
        return ("\nAND ").join(where)


@router.get(
    "/manufacturers/{id}",
    response_model=ManufacturersResponse,
    summary="Get a manufacturer by ID",
    description="Provides a manufacturer by manufacturer ID",
)
async def manufacturer_get(
    parameter: ManufacturersQueries = Depends(ManufacturersQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_manufacturers(parameter, db)
    return response


@router.get(
    "/manufacturers",
    response_model=ManufacturersResponse,
    summary="Get manufacturers",
    description="Provides a list of manufacturers",
)
async def manufacturers_get(
    parameter: ManufacturersQueries = Depends(ManufacturersQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_manufacturers(parameter, db)
    return response


async def fetch_manufacturers(where, db):
    sql = f"""
    """
    response = await db.fetchPage(sql, where.params())
    return response
