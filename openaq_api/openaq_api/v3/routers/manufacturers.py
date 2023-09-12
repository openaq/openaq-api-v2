import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path

from openaq_api.db import DB
from openaq_api.v3.models.queries import Paging, QueryBaseModel, QueryBuilder
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
        return "id = :manufacturers_id"


class ManufacturersQueries(Paging):
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
    SELECT instruments_id
    , manufacturer_entities_id
    , label
    , description
    , is_monitor
    {query_builder.fields() or ''} 
    {query_builder.total()}
    FROM instruments 
    {query_builder.where()}
    {query_builder.pagination()}
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response

#  SELECT i.instruments_id
#     , i.manufacturer_entities_id as id
#     , i.label as name
#     , i.description
#     , i.is_monitor as isMonitor
#     {query_builder.fields() or ''} 
#     {query_builder.total()}
#     FROM instruments i
#     {query_builder.where()}
#     {query_builder.pagination()}
