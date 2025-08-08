from enum import StrEnum, auto
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query

from openaq_api.db import DB
from openaq_api.exceptions import NotFoundException
from openaq_api.v3.models.queries import (
    Paging,
    QueryBaseModel,
    QueryBuilder,
    SortingBase,
)
from openaq_api.v3.models.responses import InstrumentsResponse, additional_responses
from openaq_api.v3.routers.manufacturers import fetch_manufacturers

logger = logging.getLogger("instruments")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
    include_in_schema=True,
)


class ManufacturerInstrumentsQuery(QueryBaseModel):
    """
    Path query to filter results by manufacturers ID

    Inherits from QueryBaseModel

    Attributes:
        manufacturers_id: manufacturers ID value
    """

    manufacturers_id: int = Path(
        ..., description="Limit results to a specific manufacturer id", ge=1
    )

    def where(self) -> str:
        return "i.manufacturer_entities_id = :manufacturers_id"


class InstrumentPathQuery(QueryBaseModel):
    """Path query to filter results by instruments ID

    Inherits from QueryBaseModel

    Attributes:
        instruments_id: instruments ID value
    """

    instruments_id: int = Path(
        ..., description="Limit the results to a specific instruments id", ge=1
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to a single instruments_id

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause
        """
        return "i.instruments_id = :instruments_id"


class InstrumentsSortFields(StrEnum):
    ID = auto()


class InstrumentsSorting(SortingBase):
    order_by: InstrumentsSortFields | None = Query(
        "id",
        description="The field by which to order results",
        examples=["order_by=id"],
    )


class InstrumentsQueries(Paging, InstrumentsSorting): ...


@router.get(
    "/instruments/{instruments_id}",
    response_model=InstrumentsResponse,
    summary="Get an instrument by ID",
    description="Provides a instrument by instrument ID",
    responses=additional_responses("insrument", True),
)
async def instrument_get(
    instruments: Annotated[InstrumentPathQuery, Depends(InstrumentPathQuery.depends())],
    db: DB = Depends(),
):
    response = await fetch_instruments(instruments, db)
    if len(response.results) == 0:
        raise NotFoundException("Instrument", instruments.instruments_id)
    return response


@router.get(
    "/instruments",
    response_model=InstrumentsResponse,
    summary="Get instruments",
    description="Provides a list of instruments",
    responses=additional_responses("instrument"),
)
async def instruments_get(
    instruments: Annotated[InstrumentsQueries, Depends(InstrumentsQueries.depends())],
    db: DB = Depends(),
):
    response = await fetch_instruments(instruments, db)
    return response


@router.get(
    "/manufacturers/{manufacturers_id}/instruments",
    response_model=InstrumentsResponse,
    summary="Get instruments by manufacturer ID",
    description="Provides a list of instruments for a specific manufacturer",
    responses=additional_responses("manufacturer", True),
)
async def get_instruments_by_manufacturer(
    manufacturers: Annotated[
        ManufacturerInstrumentsQuery, Depends(ManufacturerInstrumentsQuery.depends())
    ],
    db: DB = Depends(),
):
    response = await fetch_instruments(manufacturers, db)
    if not response.results:
        manufacturers_response = await fetch_manufacturers(manufacturers, db)
        if not manufacturers_response.results:
            raise NotFoundException("Manufacturer", manufacturers.manufacturers_id)
    return response


async def fetch_instruments(query, db):
    query_builder = QueryBuilder(query)
    sql = f"""
        WITH locations_summary AS (
            SELECT
                i.instruments_id
            FROM
                sensor_nodes sn
            JOIN
                sensor_systems ss ON sn.sensor_nodes_id = ss.sensor_nodes_id
            JOIN
                instruments i ON i.instruments_id = ss.instruments_id

            GROUP BY i.instruments_id
        )
        SELECT
            instruments_id AS id
            , label AS name
            , is_monitor
            , json_build_object('id', e.entities_id, 'name', e.full_name) AS manufacturer
        FROM
            instruments i
        JOIN
            locations_summary USING (instruments_id)
        JOIN
            entities e
        ON
            i.manufacturer_entities_id = e.entities_id
            {query_builder.where()}
        ORDER BY
            instruments_id
        {query_builder.pagination()};

        """

    response = await db.fetchPage(sql, query_builder.params())
    return response
