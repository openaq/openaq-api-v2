import logging
from enum import Enum

from fastapi import APIRouter, Depends, Query
from openaq_fastapi.models.responses import OpenAQCitiesResult

from ..db import DB
from ..models.queries import APIBase, City, Country

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


class CitiesOrder(str, Enum):
    city = "city"
    country = "country"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"


class Cities(City, Country, APIBase):
    order_by: CitiesOrder = Query("city", description="Order by a field")

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
                        country = ANY(:country)
                        """
                    )
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get(
    "/v1/cities",
    response_model=OpenAQCitiesResult,
    tags=["v1"],
    summary="Provides a simple listing of cities within the platform",
)
@router.get(
    "/v2/cities",
    response_model=OpenAQCitiesResult,
    tags=["v2"],
    summary="Provides a simple listing of cities within the platform",
)
async def cities_get(
    db: DB = Depends(), cities: Cities = Depends(Cities.depends())
):
    q = f"""
    WITH t AS (
    SELECT
        city,
        country,
        sum(value_count) as count,
        count(*) as locations,
        min(first_datetime) as "firstUpdated",
        max(last_datetime) as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM
    sensor_nodes
    LEFT JOIN sensor_systems USING (sensor_nodes_id)
    LEFT JOIN sensors USING (sensor_systems_id)
    LEFT JOIN rollups USING (sensors_id, measurands_id)
    LEFT JOIN groups_view USING (groups_id, measurands_id)
    WHERE rollup='total' AND groups_view.type='node' and city is not null
    AND {cities.where()}
    GROUP BY
    1,2
    ORDER BY "{cities.order_by}" {cities.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count,
        to_jsonb(t) FROM t;
    """

    output = await db.fetchOpenAQResult(q, cities.params())

    return output
