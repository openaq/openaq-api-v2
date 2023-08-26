import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    Paging,
    ParametersQuery,
    ProviderQuery,
    QueryBaseModel,
    QueryBuilder,
)
from openaq_api.v3.models.responses import CountriesResponse

logger = logging.getLogger("countries")

router = APIRouter(
    prefix="/v3",
    tags=["v3-alpha"],
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


## TODO
class CountriesQueries(
    Paging,
    ParametersQuery,
    ProviderQuery,
):
    ...


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
    sql = f"""
    SELECT id
    , code
    , name
    , datetime_first
    , datetime_last
    , parameters
    , locations_count
    , measurements_count
    , providers_count
    {query_builder.total()}
    FROM countries_view_cached
    {query_builder.where()}
    {query_builder.pagination()}
    """
    print(sql)
    response = await db.fetchPage(sql, query_builder.params())
    return response
