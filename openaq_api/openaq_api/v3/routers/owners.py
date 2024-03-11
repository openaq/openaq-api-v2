from enum import StrEnum, auto
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    Paging,
    QueryBaseModel,
    QueryBuilder,
    SortingBase,
)
from openaq_api.v3.models.responses import OwnersResponse

logger = logging.getLogger("owners")

router = APIRouter(
    prefix="/v3",
    tags=["v3-alpha"],
    include_in_schema=True,
)


class OwnerPathQuery(QueryBaseModel):
    """Path query to filter results by Owners ID.

    Inherits from QueryBaseModel.

    Attributes:
        owners_id: owners ID value.
    """

    owners_id: int = Path(
        description="Limit the results to a specific owner by id",
        ge=1,
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to a single owners_id

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause
        """
        return "entities_id = :owners_id"


class OwnersSortFields(StrEnum):
    ID = auto()


class OwnersSorting(SortingBase):
    order_by: OwnersSortFields | None = Query(
        "id",
        description="The field by which to order results",
        examples=["order_by=id"],
    )


class OwnersQueries(Paging, OwnersSorting):
    ...


@router.get(
    "/owners/{owners_id}",
    response_model=OwnersResponse,
    summary="Get a owner by ID",
    description="Provides a owner by owner ID",
)
async def owner_get(
    owners: Annotated[OwnerPathQuery, Depends(OwnerPathQuery.depends())],
    db: DB = Depends(),
):
    response = await fetch_owners(owners, db)
    return response


@router.get(
    "/owners",
    response_model=OwnersResponse,
    summary="Get owners",
    description="Provides a list of owners",
)
async def owners_get(
    owner: Annotated[OwnersQueries, Depends(OwnersQueries.depends())],
    db: DB = Depends(),
):
    response = await fetch_owners(owner, db)
    return response


async def fetch_owners(query, db):
    query_builder = QueryBuilder(query)
    sql = f"""
    SELECT e.entities_id AS id
    , e.full_name AS name
    FROM entities e
    JOIN sensor_nodes sn ON e.entities_id = sn.owner_entities_id
    {query_builder.where()}
    AND sn.is_public
    GROUP BY e.entities_id, name
    ORDER BY e.entities_id
    {query_builder.pagination()};
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
