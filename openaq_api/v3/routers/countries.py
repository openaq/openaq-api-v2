from enum import StrEnum, auto
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import ConfigDict

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    CommaSeparatedList,
    Paging,
    ParametersQuery,
    QueryBaseModel,
    QueryBuilder,
    SortingBase,
)
from openaq_api.v3.models.responses import CountriesResponse

logger = logging.getLogger("countries")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
    include_in_schema=True,
)


class CountryPathQuery(QueryBaseModel):
    """Path query to filter results by countries ID

    Inherits from QueryBaseModel

    Attributes:
        countries_id: countries ID value
    """

    countries_id: int = Path(
        description="Limit the results to a specific country by id",
        ge=1,
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to a single countries_id

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause
        """
        return "id = :countries_id"


class CountriesSortFields(StrEnum):
    ID = auto()


class CountriesSorting(SortingBase):
    order_by: CountriesSortFields | None = Query(
        "id",
        description="The field by which to order results",
        examples=["order_by=id"],
    )


class CountriesProvider(QueryBaseModel):
    """Pydantic query model for the `providers_id` query parameter

    Inherits from QueryBaseModel

    Attributes:
        providers_id: providers_id or comma separated list of providers_id
            for filtering results to a provider or providers
    """

    providers_id: CommaSeparatedList[int] | None = Query(
        None,
        description="Limit the results to a specific provider or multiple providers  with a single provider ID or a comma delimited list of IDs",
        examples=["1", "1,2,3"],
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def where(self) -> str | None:
        """ """
        if self.has("providers_id"):
            return "providers_ids && :providers_id"


class CountriesQueries(
    Paging, ParametersQuery, CountriesProvider, CountriesSorting
): ...


@router.get(
    "/countries/{countries_id}",
    response_model=CountriesResponse,
    summary="Get a country by ID",
    description="Provides a country by country ID",
)
async def country_get(
    countries: Annotated[CountryPathQuery, Depends(CountryPathQuery)],
    db: DB = Depends(),
):
    response = await fetch_countries(countries, db)
    if len(response.results) == 0:
        raise HTTPException(status_code=404, detail="Country not found")
    return response


@router.get(
    "/countries",
    response_model=CountriesResponse,
    summary="Get countries",
    description="Provides a list of countries",
)
async def countries_get(
    countries: Annotated[CountriesQueries, Depends(CountriesQueries.depends())],
    db: DB = Depends(),
):
    response = await fetch_countries(countries, db)
    return response


async def fetch_countries(query, db):
    query_builder = QueryBuilder(query)
    print("QUERY")
    print(query_builder.where())
    sql = f"""
    SELECT id
    , code
    , name
    , datetime_first
    , datetime_last
    , parameters
    {query_builder.total()}
    FROM countries_view_cached
    {query_builder.where()}
    {query_builder.pagination()}
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
