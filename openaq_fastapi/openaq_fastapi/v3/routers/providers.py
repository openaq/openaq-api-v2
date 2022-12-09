import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import ProvidersResponse
from openaq_fastapi.models.queries import OBaseModel, Geo

from openaq_fastapi.v3.models.queries import (
    SQL,
    Paging,
)

logger = logging.getLogger("providers")

router = APIRouter(prefix="/v3", tags=["v3"])


class ProvidersQueries(Paging, SQL):
    def fields(self):
        fields = []
        return ", " + (",").join(fields) if len(fields) > 0 else ""

    def clause(self):
        where = ["WHERE TRUE"]
        return ("\nAND ").join(where)


@router.get(
    "/providers/{id}",
    response_model=ProvidersResponse,
    summary="Get a provider by ID",
    description="Provides a owner by provider ID",
)
async def provider_get(
    provider: ProvidersQueries = Depends(ProvidersQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_providers(provider, db)
    return response


@router.get(
    "/providers",
    response_model=ProvidersResponse,
    summary="Get providers",
    description="Provides a list of providers",
)
async def providers_get(
    provider: ProvidersQueries = Depends(ProvidersQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_providers(provider, db)
    return response


async def fetch_providers(where, db):
    sql = f"""
    """
    response = await db.fetchPage(sql, where.params())
    return response
