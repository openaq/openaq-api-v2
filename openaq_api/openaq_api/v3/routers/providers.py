import logging
from typing import Annotated
from enum import StrEnum, auto

from fastapi import APIRouter, Depends, Path, Query

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    BboxQuery,
    CountryIdQuery,
    CountryIsoQuery,
    MonitorQuery,
    Paging,
    ParametersQuery,
    QueryBaseModel,
    QueryBuilder,
    RadiusQuery,
    SortingBase,
)
from openaq_api.v3.models.responses import ProvidersResponse

logger = logging.getLogger("providers")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
    include_in_schema=True,
)


class ProvidersSortFields(StrEnum):
    ID = auto()


class ProvidersSorting(SortingBase):
    order_by: ProvidersSortFields | None = Query(
        "id",
        description="""Order results by ID""",
        examples=["order_by=id"],
    )


class ProviderPathQuery(QueryBaseModel):
    """Path query to filter results by providers ID

    Inherits from QueryBaseModel

    Attributes:
        providers_id: providers ID value
    """

    providers_id: int = Path(
        description="Limit the results to a specific provider by id",
        ge=1,
    )

    def where(self):
        """Generates SQL condition for filtering to a single providers_id

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause
        """
        return "id = :providers_id"


class ProvidersSortFields(StrEnum):
    ID = auto()


class ParametersSorting(SortingBase):
    order_by: ProvidersSortFields | None = Query(
        "id",
        description="The field by which to order results",
        examples=["order_by=id"],
    )


## TODO
class ProvidersQueries(
    Paging,
    RadiusQuery,
    BboxQuery,
    CountryIdQuery,
    CountryIsoQuery,
    MonitorQuery,
    ParametersQuery,
    ParametersSorting,
): ...


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
    , owner_entity->>'id' AS entities_id
    , parameters
    , st_asgeojson(extent)::json as bbox
    {query_builder.total()}
    FROM providers_view_cached
    {query_builder.where()}
    {query_builder.pagination()}
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
