import logging
from fastapi import APIRouter, Depends, Query, Path
from typing import Annotated
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.queries import (
    QueryBuilder,
    QueryBaseModel,
    CountryQuery,
    Paging,
    BboxQuery,
    RadiusQuery,
    MonitorQuery,
    ParametersQuery,
)

from openaq_fastapi.v3.models.responses import (
    ProvidersResponse,
)

logger = logging.getLogger("providers")

router = APIRouter(
    prefix="/v3",
    tags=["v3-alpha"],
    include_in_schema=True,
)


class ProviderPathQuery(QueryBaseModel):
    providers_id: int = Path(
        description="Limit the results to a specific provider by id",
        ge=1,
    )

    def where(self):
        return "id = :providers_id"


## TODO
class ProvidersQueries(
    Paging, RadiusQuery, BboxQuery, CountryQuery, MonitorQuery, ParametersQuery
):
    ...


@router.get(
    "/providers/{providers_id}",
    response_model=ProvidersResponse,
    summary="Get a provider by ID",
    description="Provides a provider by provider ID",
)
async def provider_get(
    providers: Annotated[ProviderPathQuery, Depends(ProviderPathQuery.depends())],
    db: DB = Depends(),
):
    response = await fetch_providers(providers, db)
    return response


@router.get(
    "/providers",
    response_model=ProvidersResponse,
    summary="Get providers",
    description="Provides a list of providers",
)
async def providers_get(
    provider: Annotated[ProvidersQueries, Depends(ProvidersQueries.depends())],
    db: DB = Depends(),
):
    response = await fetch_providers(provider, db)
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
