import logging
from typing import Union
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
)


class ProviderQueries(QueryBaseModel):
    id: int = Path(
        description="Limit the results to a specific provider by id",
        ge=1,
    )

    def where(self):
        return "WHERE id = :id"


class ProvidersQueries(QueryBaseModel, Paging):
    ...


class ProviderLocationsQueries(
    QueryBuilder,
    Paging,
    RadiusQuery,
    BboxQuery,
    OwnerQuery,
    CountryQuery,
):
    mobile: Union[bool, None] = Query(
        description="Is the location considered a mobile location?"
    )

    providers_id: int = Path(description="", ge=1)

    monitor: Union[bool, None] = Query(
        description="Is the location considered a reference monitor?"
    )

    def fields(self):
        fields = []
        if self.has("coordinates"):
            fields.append(RadiusQuery.fields(self))
        return ", " + (",").join(fields) if len(fields) > 0 else ""

    def generate_where(self):
        where = []
        where.append("(provider->'id')::int = :providers_id")
        if self.has("mobile"):
            where.append("ismobile = :mobile")
        if self.has("monitor"):
            where.append("ismonitor = :monitor")
        return where


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
    , st_asgeojson(extent)::json as bbox
    {query_builder.total()}
    FROM providers_view_cached
    {query_builder.where()}
    {query_builder.pagination()}
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
