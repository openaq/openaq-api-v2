from enum import StrEnum, auto
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from db import DB
from v3.models.queries import (
    Paging,
    QueryBaseModel,
    QueryBuilder,
    SortingBase,
)
from v3.models.responses import ManufacturersResponse

logger = logging.getLogger("manufacturers")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
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
        return "e.entities_id = :manufacturers_id"


class ManufacturersSortFields(StrEnum):
    ID = auto()


class InstrumentsSorting(SortingBase):
    order_by: ManufacturersSortFields | None = Query(
        "id",
        description="The field by which to order results",
        examples=["order_by=id"],
    )


class ManufacturersQueries(Paging, InstrumentsSorting): ...


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
    if len(response.results) == 0:
        raise HTTPException(status_code=404, detail="Manufacturer not found")
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
        SELECT
            e.entities_id AS id
            , e.full_name AS name
            , ARRAY_AGG(DISTINCT (jsonb_build_object('id', i.instruments_id, 'name', i.label))) AS instruments
            , COUNT(1) OVER() AS found
        FROM
            sensor_nodes sn
        JOIN
            sensor_systems ss ON sn.sensor_nodes_id = ss.sensor_nodes_id
        JOIN
            instruments i ON i.instruments_id = ss.instruments_id
        JOIN
            entities e ON e.entities_id = i.manufacturer_entities_id
        {query_builder.where()}
        GROUP BY id, name
        {query_builder.pagination()};

        """

    response = await db.fetchPage(sql, query_builder.params())
    return response
