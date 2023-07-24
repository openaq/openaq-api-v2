import logging
from fastapi import APIRouter, Depends, Path
from typing import Annotated
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import ManufacturersResponse

from openaq_fastapi.v3.models.queries import Paging, QueryBaseModel, QueryBuilder

logger = logging.getLogger("manufacturers")

router = APIRouter(
    prefix="/v3",
    tags=["v3-alpha"],
    include_in_schema=True,
)


class ManufacturerPathQuery(QueryBaseModel):
    manufacturers_id: int = Path(
        ..., description="Limit the results to a specific manufacturers id", ge=1
    )

    def where(self) -> str:
        return "id = :manufacturers_id"


class ManufacturersQueries(QueryBaseModel, Paging):
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
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
