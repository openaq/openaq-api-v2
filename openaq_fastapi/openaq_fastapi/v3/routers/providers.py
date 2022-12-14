import logging
from typing import Union
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB

from openaq_fastapi.v3.models.responses import (
    ProvidersResponse,
    LocationsResponse,
)

from openaq_fastapi.v3.models.queries import (
    QueryBaseModel,
    Paging,
    LocationsQueries,
)

from openaq_fastapi.v3.routers.locations import fetch_locations

logger = logging.getLogger("providers")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
)


class ProviderQueries(QueryBaseModel):
    id: int = Path(
        description="Limit the results to a specific provider by id",
        ge=1,
    )

    def where(self):
        return "WHERE id = :id"


class ProvidersQueries(Paging):
    def where(self):
        return "WHERE id = :id"


@router.get(
    "/providers/{id}",
    response_model=ProvidersResponse,
    summary="Get a provider by ID",
    description="Provides a provider by provider ID",
)
async def provider_get(
    provider: ProviderQueries = Depends(ProviderQueries.depends()),
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


@router.get(
    "/providers/{id}/locations",
    response_model=LocationsResponse,
    summary="Get lociations by provider ID",
    description="Provides a list of locations by provider ID",
)
async def provider_locations_get(
    locations: LocationsQueries = Depends(LocationsQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_locations(locations, db)
    return response


async def fetch_providers(query, db):
    sql = f"""
    SELECT id
    , name
    , source_name
    , export_prefix
    , datetime_first
    , datetime_last
    , datetime_added
    , measurements_count
    , locations_count
    , countries_count
    , owner_entity
    , parameters
    , st_asgeojson(extent)::json as bbox
    {query.total()}
    FROM providers_view_cached
    {query.where()}
    {query.pagination()}
    """
    response = await db.fetchPage(sql, query.params())
    return response
