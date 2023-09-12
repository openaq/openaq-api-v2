# owners.py:

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    Paging, 
    ParametersQuery, 
    ProviderQuery, 
    QueryBaseModel, 
    QueryBuilder
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


class OwnersQueries(Paging, ParametersQuery, ProviderQuery):
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
    SELECT e.entities_id as id
    , e.full_name as name
    , e.added_on
    , COUNT(sn.owner_entities_id) AS locations_count
    FROM (
        SELECT entities_id, full_name, added_on
        FROM entities
        WHERE entity_type NOT IN ('Person', 'Organization')
    ) AS e
    LEFT JOIN sensor_nodes sn ON e.entities_id = sn.owner_entities_id
    GROUP BY e.entities_id, name, e.added_on
    ORDER BY e.entities_id;
    """
    print(sql)
    response = await db.fetchPage(sql, query_builder.params())
    return response

 # {query_builder.total()}
    # FROM entities
    # {query_builder.where()}
    # {query_builder.pagination()}