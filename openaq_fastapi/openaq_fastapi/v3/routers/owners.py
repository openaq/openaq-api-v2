import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import OwnersResponse, LocationsReponse
from openaq_fastapi.models.queries import OBaseModel, Geo

from openaq_fastapi.v3.models.queries import (
    SQL,
    Paging,
)

logger = logging.getLogger("owners")

router = APIRouter(prefix="/v3", tags=["v3"])


class OwnersQueries(Paging, SQL):
    def fields(self):
        fields = []
        return ", " + (",").join(fields) if len(fields) > 0 else ""

    def clause(self):
        where = ["WHERE TRUE"]
        return ("\nAND ").join(where)


@router.get(
    "/owners/{id}",
    response_model=OwnersResponse,
    summary="Get a owner by ID",
    description="Provides a owner by owner ID",
)
async def owner_get(
    owner: OwnersQueries = Depends(OwnersQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_owners(owner, db)
    return response


@router.get(
    "/owners",
    response_model=OwnersResponse,
    summary="Get owners",
    description="Provides a list of owners",
)
async def owners_get(
    owner: OwnersQueries = Depends(OwnersQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_owners(owner, db)
    return response


@router.get(
    "/owners/{id}/locations",
    response_model=LocationsReponse,
    summary="Get locations by owner ID",
    description="Provides a list of locations by owner ID",
)
async def owner_locations_get(
    owner: OwnersQueries = Depends(OwnersQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_owner_locations(owner, db)
    return response


async def fetch_owners(where, db):
    sql = f"""
    """
    response = await db.fetchPage(sql, where.params())
    return response


async def fetch_owner_locations(id, where, db):
    sql = f"""
    """
    response = await db.fetchPage(sql, where.params())
    return response
