import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    BboxQuery,
    CountryIdQuery,
    CountryIsoQuery,
    OwnerQuery,
    Paging,
    ProviderQuery,
    QueryBaseModel,
    QueryBuilder,
    RadiusQuery,
)
from openaq_api.v3.models.responses import ManufacturersResponse

logger = logging.getLogger("manufacturers")

router = APIRouter(
    prefix="/v3",
    tags=["v3-alpha"],
    include_in_schema=True,
)

class ManufacturerPathQuery(QueryBaseModel):
    """Path query to filter results by manufacturers ID

    Inherits from QueryBaseModel

    Attributes:
        manufacturers_id: manufacturers ID value
    """

    manufacturers_id: int = Path(
        ..., description="Limit the results to a specific manufacturers id", ge=1
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to a single manufacturers_id

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause
        """
        return "(json_element->'manufacturer'->>'id')::int = :manufacturers_id"


class ManufacturersQueries(
    Paging,
    RadiusQuery,
    BboxQuery,
    ProviderQuery,
    OwnerQuery,
    CountryIdQuery,
    CountryIsoQuery
):
    ...


@router.get(
    "/manufacturers/{manufacturers_id}",
    response_model=ManufacturersResponse,
    summary="Get a manufacturer by ID",
    description="Provides a manufacturer by manufacturer ID",
)
async def manufacturer_get(
    manufacturers: Annotated[
        ManufacturerPathQuery, Depends(ManufacturerPathQuery.depends())
    ],
    db: DB = Depends(),
):
    response = await fetch_manufacturers(manufacturers, db)
    return response


@router.get(
    "/manufacturers",
    response_model=ManufacturersResponse,
    summary="Get manufacturers",
    description="Provides a list of manufacturers",
)
async def manufacturers_get(
    manufacturer: Annotated[
        ManufacturersQueries, Depends(ManufacturersQueries.depends())
    ],
    db: DB = Depends(),
):
    response = await fetch_manufacturers(manufacturer, db)
    return response


async def fetch_manufacturers(query, db):
    query_builder = QueryBuilder(query)
    sql = f"""
        WITH Manufacturers AS (
            SELECT 
                (json_element->'manufacturer'->>'id')::int AS manufacturer_id,
                json_element->'manufacturer'->>'name' AS manufacturer_name,
                (json_element->>'id')::int AS instrument_id,
                json_element->>'name' AS instrument_name,
                country,
                owner,
                provider,
                coordinates,
                instruments,
                sensors,
                timezone,
                bbox(geom) as bounds,
                datetime_first,
                datetime_last
                {query_builder.fields() or ''} 
                FROM locations_view_cached,
                LATERAL json_array_elements(instruments) AS json_element
                {query_builder.where()}
        )

        SELECT 
            manufacturer_id AS id,
            manufacturer_name AS name,
            COUNT(manufacturer_id) AS locations_count,
            ARRAY_AGG(DISTINCT (JSON_BUILD_OBJECT('id', instrument_id, 'name', instrument_name))::jsonb) AS instruments
        FROM 
            Manufacturers
        GROUP BY 
            manufacturer_id, manufacturer_name
        ORDER BY 
            manufacturer_id
        {query_builder.pagination()};
        
        """


    response = await db.fetchPage(sql, query_builder.params())
    return response
