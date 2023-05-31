import logging
from typing import Union
from fastapi import APIRouter, Depends, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.routers.locations import fetch_locations
from openaq_fastapi.v3.models.responses import OwnersResponse, LocationsResponse

from openaq_fastapi.v3.models.queries import Paging, QueryBaseModel, QueryBuilder

logger = logging.getLogger("owners")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
    include_in_schema=False,
)


class OwnerPathQuery(QueryBaseModel):
    owners_id: int = Path(
        description="Limit the results to a specific owner by id",
        ge=1,
    )

    def where(self) -> str:
        return "id = :owners_id"


class OwnersQueries(QueryBaseModel, Paging):
    ...


class OwnerLocationPathQuery(QueryBaseModel):
    owners_id: int = Path(
        description="Limit the results to a specific country",
    )

    def where(self) -> str:
        return "(owner->'id')::int = :owners_id"


class OwnerLocationsQueries(
    Paging,
    OwnerLocationPathQuery,
):
    ...


@router.get(
    "/owners/{owners_id}",
    response_model=OwnersResponse,
    summary="Get a owner by ID",
    description="Provides a owner by owner ID",
)
async def owner_get(
    owner: OwnerPathQuery = Depends(OwnerPathQuery.depends()),
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
    "/owners/{owners_id}/locations",
    response_model=LocationsResponse,
    summary="Get locations by owner ID",
    description="Provides a list of locations by owner ID",
)
async def owner_locations_get(
    owner_locations: OwnerLocationsQueries = Depends(OwnerLocationsQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_locations(owner_locations, db)
    return response


async def fetch_owners(query, db):
    query_builder = QueryBuilder(query)
    sql = f"""
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
