import logging

from fastapi import APIRouter, Depends, Query
from enum import Enum
from ..db import DB
from ..models.queries import APIBase, Country
from openaq_fastapi.models.responses import (
    OpenAQCountriesResult,
)

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


class CountriesOrder(str, Enum):
    country = "country"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"
    locations = "locations"
    count = "count"


class Countries(Country, APIBase):

    order_by: CountriesOrder = Query("country")

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                if f == "country":
                    wheres.append(" cl.iso = ANY(:country) ")
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get(
    "/v1/countries/{country_id}",
    response_model=OpenAQCountriesResult,
    tags=["v1"],
)
@router.get(
    "/v2/countries/{country_id}",
    response_model=OpenAQCountriesResult,
    tags=["v2"],
)
@router.get("/v1/countries", response_model=OpenAQCountriesResult, tags=["v1"])
@router.get("/v2/countries", response_model=OpenAQCountriesResult, tags=["v2"])
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
        order_by = "iso"
    elif countries.order_by == "count":
        order_by = "sum(value_count)"
    elif countries.order_by == "locations":
        order_by = "count(*)"

    q = f"""
    WITH t AS (
    SELECT
        cl.iso as code,
        cl.name,
        --cl.bbox,
        cities,
        sum(value_count) as count,
        count(*) as locations,
        min(first_datetime) as "firstUpdated",
        max(last_datetime) as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM countries cl
    JOIN groups_view gv ON (gv.name=cl.iso)
    JOIN rollups r USING (groups_id, measurands_id)
    JOIN LATERAL (
        SELECT count(DISTINCT city) as cities FROM sensor_nodes
        WHERE country=cl.iso
        ) cities ON TRUE
    WHERE r.rollup='total' AND gv.type='country'
    AND
    {countries.where()}
    GROUP BY
    1,2,3
    ORDER BY {order_by} {countries.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count, to_jsonb(t) as json FROM t

    """

    output = await db.fetchOpenAQResult(q, countries.params())

    return output
