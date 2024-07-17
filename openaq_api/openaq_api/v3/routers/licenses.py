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
from openaq_api.v3.models.responses import LicensesResponse

logger = logging.getLogger("licenses")

router = APIRouter(
    prefix="/v3",
    tags=["v3-alpha"],
    include_in_schema=True,
)


class LicensesPathQuery(QueryBaseModel):
    """Path query to filter results by license ID

    Inherits from QueryBaseModel

    Attributes:
        licenses_id: license ID value
    """

    licenses_id: int = Path(
        ..., description="Limit the results to a specific licenses id", ge=1
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to a single licenses_id

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause
        """
        return "l.licenses_id = :licenses_id"


class LicensesSortFields(StrEnum):
    ID = auto()


class LicensesSorting(SortingBase):
    order_by: LicensesSortFields | None = Query(
        "id",
        description="The field by which to order results",
        examples=["order_by=id"],
    )


class LicensesQueries(Paging, LicensesSorting): ...


@router.get(
    "/licenses/{licenses_id}",
    response_model=LicensesResponse,
    summary="Get an instrument by ID",
    description="Provides a instrument by instrument ID",
)
async def license_get(
    licenses: Annotated[LicensesPathQuery, Depends(LicensesPathQuery.depends())],
    db: DB = Depends(),
):
    response = await fetch_licenses(licenses, db)
    return response


@router.get(
    "/licenses",
    response_model=LicensesResponse,
    summary="Get licenses",
    description="Provides a list of licenses",
)
async def instruments_get(
    licenses: Annotated[LicensesQueries, Depends(LicensesQueries.depends())],
    db: DB = Depends(),
):
    response = await fetch_licenses(licenses, db)
    return response


async def fetch_licenses(query, db):
    query_builder = QueryBuilder(query)
    sql = f"""
        SELECT 
            licenses_id AS id
            , name
            , url AS source_url
            , attribution_required
            , share_alike_required
            , commercial_use_allowed
            , redistribution_allowed
            , modification_allowed
        FROM licenses
            {query_builder.where()}
        ORDER BY 
            licenses_id
        {query_builder.pagination()};

        """

    response = await db.fetchPage(sql, query_builder.params())
    return response
