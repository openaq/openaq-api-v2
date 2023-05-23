import logging
from fastapi import APIRouter, Depends, Query, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.queries import (
    QueryBuilder,
    QueryBaseModel,
    Paging,
    BboxQuery,
    CountryQuery,
    OwnerQuery,
    RadiusQuery,
    MobileQuery,
    MonitorQuery,
)

from openaq_fastapi.v3.models.responses import (
    ProvidersResponse,
    LocationsResponse,
)

from openaq_fastapi.v3.routers.locations import fetch_locations


logger = logging.getLogger("providers")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
    include_in_schema=False,
)


class ProviderPathQuery(QueryBaseModel):
    providers_id: int = Path(
        description="Limit the results to a specific provider by id",
        ge=1,
    )

    def where(self):
        return "id = :providers_id"


class ProvidersQueries(QueryBaseModel, Paging):
    ...


class ProviderLocationPathQuery(QueryBaseModel):
    providers_id: int = Path(
        description="Limit the results to a specific country",
    )

    def where(self) -> str:
        return "(provider->'id')::int = :providers_id"


class ProviderLocationsQueries(
    ProviderLocationPathQuery,
    Paging,
    RadiusQuery,
    BboxQuery,
    OwnerQuery,
    CountryQuery,
    MobileQuery,
    MonitorQuery,
):
    ...


@router.get(
    "/providers/{providers_id}",
    response_model=ProvidersResponse,
    summary="Get a provider by ID",
    description="Provides a provider by provider ID",
)
async def provider_get(
    provider: ProviderPathQuery = Depends(ProviderPathQuery.depends()),
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
    "/providers/{providers_id}/locations",
    response_model=LocationsResponse,
    summary="Get lociations by provider ID",
    description="Provides a list of locations by provider ID",
)
async def provider_locations_get(
    locations: ProviderLocationsQueries = Depends(ProviderLocationsQueries.depends()),
    db: DB = Depends(),
):
    response = await fetch_locations(locations, db)
    return response


async def fetch_providers(query, db):
    query_builder = QueryBuilder(query)
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
    , license
    , st_asgeojson(extent)::json as bbox
    {query_builder.total()}
    FROM providers_view_cached
    {query_builder.where()}
    {query_builder.pagination()}
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
