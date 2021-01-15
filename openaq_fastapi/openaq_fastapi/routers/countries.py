import logging

from fastapi import APIRouter, Depends, Query
from enum import Enum
from ..db import DB
from ..models.queries import APIBase, Country
from openaq_fastapi.models.responses import (
    OpenAQCountriesResult,
    converter
)
import jq
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
    count(*) over () as countriescount,
        cl.iso as code,
        cl.name,
        --cl.bbox,
        sn.cities,
        sn.locations,
        sn.sources,
        sum(value_count) as count,
        min(first_datetime) as "firstUpdated",
        max(last_datetime) as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM countries cl
    JOIN groups_view gv ON (gv.name=cl.iso)
    JOIN rollups r USING (groups_id, measurands_id)
    JOIN LATERAL (
        SELECT
            count(DISTINCT sensor_nodes_id) as locations,
            count(DISTINCT city) as cities ,
            count(DISTINCT sources_id) as sources
        FROM sensor_nodes
        LEFT JOIN sensor_nodes_sources USING (sensor_nodes_id)
        WHERE country=cl.iso
        ) sn ON TRUE
    WHERE r.rollup='total' AND gv.type='country'
    AND
    {countries.where()}
    GROUP BY
    2,3,4,5,6
    ORDER BY {order_by} {countries.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT countriescount as count, to_jsonb(t)-'{{countriescount}}'::text[] as json FROM t

    """
    params = countries.params()
    output = await db.fetchOpenAQResult(q, params)

    return output

@router.get("/v1/countries", response_model=OpenAQCountriesResult, tags=["v1"])
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