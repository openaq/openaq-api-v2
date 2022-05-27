import logging
from enum import Enum
from typing import Optional

from fastapi import APIRouter, Depends, Query
from openaq_fastapi.models.responses import OpenAQCitiesResult, converter
import jq
from ..db import DB
from ..models.queries import APIBase, City, Country

logger = logging.getLogger("cities")

router = APIRouter()


class CitiesOrder(str, Enum):
    city = "city"
    country = "country"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"


class Cities(City, Country, APIBase):
    order_by: CitiesOrder = Query("city", description="Order by a field")
    entity: Optional[str] = None

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                if f == "city":
                    wheres.append(
                        """
                        city = ANY(:city)
                        """
                    )
                elif f == "country":
                    wheres.append(
                        """
                        code = ANY(:country)
                        """
                    )
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get(
    "/v2/cities",
    response_model=OpenAQCitiesResult,
    tags=["v2"],
    summary="Provides a listing of cities within the platform",
)
async def cities_get(
    db: DB = Depends(), cities: Cities = Depends(Cities.depends())
):
    order_by = cities.order_by
    if cities.order_by == "lastUpdated":
        order_by = "8"
    elif cities.order_by == "firstUpdated":
        order_by = "7"
    elif cities.order_by == "country":
        order_by = "code"
    elif cities.order_by == "count":
        order_by = "count"
    elif cities.order_by == "city":
        order_by = "city"
    elif cities.order_by == "locations":
        order_by = "locations"
    q = f"""
    WITH t AS (
    SELECT
    count(*) over () as citiescount,
        code as country,
        city,
        count,
        locations,
        "firstUpdated",
        "lastUpdated",
        parameters
    FROM city_stats
    WHERE
    {cities.where()}
    and city is not null
    ORDER BY {order_by} {cities.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT citiescount as count, to_jsonb(t)-'{{citiescount}}'::text[] as json FROM t

    """
    params=cities.params()
    output = await db.fetchOpenAQResult(q, params)

    return output


@router.get(
    "/v1/cities",
    response_model=OpenAQCitiesResult,
    tags=["v1"],
    summary="Provides a simple listing of cities within the platform",
)
async def cities_getv1(
    db: DB = Depends(), cities: Cities = Depends(Cities.depends()),
):
    cities.entity = "government"
    data = await cities_get(db, cities)
    meta = data.meta
    res = data.results

    if len(res) == 0:
        return data

    v1_jq = jq.compile(
        """
        .[] | . as $m |
            {
                country: .country,
                name: .city,
                city: .city,
                count: .count,
                locations: .locations
            }

        """
    )

    return converter(meta, res, v1_jq)
