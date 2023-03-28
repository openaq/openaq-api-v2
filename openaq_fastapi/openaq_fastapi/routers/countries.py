import logging

from fastapi import APIRouter, Depends, Query
from enum import Enum
from ..db import DB
from ..models.queries import APIBase, Country
from openaq_fastapi.models.responses import CountriesResponse, converter
import jq

logger = logging.getLogger("countries")

router = APIRouter()


class CountriesOrder(str, Enum):
    country = "country"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"
    locations = "locations"
    count = "count"


class Countries(Country, APIBase):
    order_by: CountriesOrder = Query(
        "country",
        description="Order by a field e.g. ?order_by=country",
        example="country",
    )
    limit: int = Query(
        200,
        description="Limit the number of results returned. e.g. limit=200 will return up to 200 results",
        example="200",
    )

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                if f == "country":
                    wheres.append(" code = ANY(:country) ")
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get(
    "/v1/countries/{country_id}",
    include_in_schema=False,
    response_model=CountriesResponse,
    summary="Get country by ID",
    description="Provides a single country by country ID",
    tags=["v1"],
)
@router.get(
    "/v2/countries/{country_id}",
    include_in_schema=False,
    response_model=CountriesResponse,
    summary="Get country by ID",
    description="Provides a single country by country ID",
    tags=["v2"],
)
@router.get(
    "/v2/countries",
    include_in_schema=False,
    response_model=CountriesResponse,
    summary="Get countries",
    description="Providecs a list of countries",
    tags=["v2"],
)
async def countries_get(
    db: DB = Depends(),
    countries: Countries = Depends(Countries.depends()),
):
    order_by = countries.order_by
    if countries.order_by == "lastUpdated":
        order_by = "8"
    elif countries.order_by == "firstUpdated":
        order_by = "7"
    elif countries.order_by == "country":
        order_by = "code"
    elif countries.order_by == "count":
        order_by = "count"
    elif countries.order_by == "locations":
        order_by = "locations"

    q = f"""
    WITH t AS (
    SELECT
    count(*) over () as countriescount,
        *
    FROM country_stats
    WHERE
    {countries.where()}
    AND code is not null
    ORDER BY {order_by} {countries.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT countriescount as count, to_jsonb(t)-'{{countriescount}}'::text[] as json FROM t

    """
    params = countries.params()
    output = await db.fetchOpenAQResult(q, params)

    return output


@router.get(
    "/v1/countries",
    include_in_schema=False,
    response_model=CountriesResponse,
    summary="Get countries",
    description="Providecs a list of countries",
    tags=["v1"],
)
async def countries_getv1(
    db: DB = Depends(),
    countries: Countries = Depends(Countries.depends()),
):
    data = await countries_get(db, countries)
    meta = data.meta
    res = data.results

    if len(res) == 0:
        return data

    v1_jq = jq.compile(
        """
        .[] | . as $m |
            {
                code: .code,
                count: .count,
                locations: .locations,
                cities: .cities,
                name:.name
            }

        """
    )

    return converter(meta, res, v1_jq)
