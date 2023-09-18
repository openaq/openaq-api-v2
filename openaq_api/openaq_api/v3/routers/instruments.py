import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    Paging,
    QueryBaseModel,
    QueryBuilder,

)
from openaq_api.v3.models.responses import InstrumentsResponse

logger = logging.getLogger("instruments")

router = APIRouter(
    prefix="/v3",
    tags=["v3-alpha"],
    include_in_schema=True,
)

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


class ManufacturersQueries(
    Paging,
):
    ...


@router.get(
    "/instruments/{instruments_id}",
    response_model=InstrumentsResponse,
    summary="Get an instrument by ID",
    description="Provides a instrument by instrument ID",
)
async def instrument_get(
    instruments: Annotated[
        InstrumentPathQuery, Depends(InstrumentPathQuery.depends())
    ],
    db: DB = Depends(),
):
    response = await fetch_instruments(instruments, db)
    return response


@router.get(
    "/instruments",
    response_model=InstrumentsResponse,
    summary="Get manufacturers",
    description="Provides a list of manufacturers",
)
async def instruments_get(
    instruments: Annotated[
        ManufacturersQueries, Depends(ManufacturersQueries.depends())
    ],
    db: DB = Depends(),
):
    response = await fetch_instruments(instruments, db)
    return response


async def fetch_instruments(query, db):
    query_builder = QueryBuilder(query)
    sql = f"""
        WITH Instruments AS (
            SELECT 
                e.entities_id
                , e.full_name AS entities_name
                , i.instruments_id
                , i.label AS instruments_name
                , i.is_monitor
                {query_builder.fields() or ''} 
                {query_builder.total()}
            FROM 
                sensor_nodes sn 
            JOIN 
                sensor_systems ss ON sn.sensor_nodes_id = ss.sensor_nodes_id
            JOIN 
                instruments i ON i.instruments_id = ss.instruments_id
            JOIN 
                entities e ON e.entities_id = i.manufacturer_entities_id
            {query_builder.where()}
        )

        SELECT 
            instruments_id AS id
            , instruments_name AS name
            , is_monitor
            , ARRAY_AGG(DISTINCT (JSON_BUILD_OBJECT('id', entities_id, 'name', entities_name))::jsonb) AS manufacturer         
        FROM 
            Instruments
        GROUP BY 
            instruments_id, instruments_name, is_monitor
        ORDER BY 
            instruments_id
        {query_builder.pagination()};

        """


    response = await db.fetchPage(sql, query_builder.params())
    return response