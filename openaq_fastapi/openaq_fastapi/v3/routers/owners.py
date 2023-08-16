import logging
from typing import Union, Annotated
from fastapi import APIRouter, Depends, Path
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import OwnersResponse

from openaq_fastapi.v3.models.queries import Paging, QueryBaseModel, QueryBuilder

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
        return "id = :owners_id"


class OwnersQueries(Paging):
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
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
