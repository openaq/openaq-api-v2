import logging
from enum import StrEnum, auto
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    BboxQuery,
    CountryIdQuery,
    CountryIsoQuery,
    Paging,
    QueryBaseModel,
    QueryBuilder,
    RadiusQuery,
    SortingBase,
)
from openaq_api.v3.models.responses import ParametersResponse

logger = logging.getLogger("parameters")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
    include_in_schema=True,
)


class ProvidersSortFields(StrEnum):
    ID = auto()


class ProvidersSorting(SortingBase):
    order_by: ProvidersSortFields | None = Query(
        "id",
        description="""Order results by ID""",
        examples=["order_by=id"],
    )


class ParameterPathQuery(QueryBaseModel):
    """Path query to filter results by parameters ID

    Inherits from QueryBaseModel

    Attributes:
        parameters_id: countries ID value
    """

    parameters_id: int = Path(
        ..., description="Limit the results to a specific parameters id", ge=1
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to a single parameters_id

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause
        """
        return "id = :parameters_id"


class ParameterType(StrEnum):
    pollutant = "pollutant"
    meteorological = "meteorological"


class ParameterTypeQuery(QueryBaseModel):
    """Query to filter results by parameter_type

    Inherits from QueryBaseModel

    Attributes:
        parameter_type: a string representing the parameter type to filter
    """

    parameter_type: ParameterType | None = Query(
        None,
        description="Limit the results to a specific parameters type",
        examples=["pollutant", "meteorological"],
    )

    def where(self) -> str | None:
        """Generates SQL condition for filtering to a single parameters_id

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause if `parameter_type` is set
        """
        if self.parameter_type == None:
            return None
        return "m.parameter_type = :parameter_type"


class ParametersCountryIsoQuery(CountryIsoQuery):
    """Pydantic query model for the `iso` query parameter.

    Specialty query object for parameters_view_cached to handle ISO code IN ARRAY

    Inherits from CountryIsoQuery
    """

    def where(self) -> str | None:
        """Generates SQL condition for filtering to country ISO code

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause
        """
        if self.iso is not None:
            return "country->>'code' IN :iso"


class ParametersSortFields(StrEnum):
    ID = auto()


class ParametersSorting(SortingBase):
    order_by: ParametersSortFields | None = Query(
        "id",
        description="The field by which to order results",
        examples=["order_by=id"],
    )


class ParametersQueries(
    Paging,
    CountryIdQuery,
    CountryIsoQuery,  # TODO replace with ParametersCountryIsoQuery when parameters_view_cached is updated with ISO array field
    BboxQuery,
    RadiusQuery,
    ParameterTypeQuery,
    ParametersSorting,
): ...


@router.get(
    "/parameters/{parameters_id}",
    response_model=ParametersResponse,
    summary="Get a parameter by ID",
    description="Provides a parameter by parameter ID",
)
async def parameter_get(
    parameter: Annotated[ParameterPathQuery, Depends(ParameterPathQuery.depends())],
    db: DB = Depends(),
) -> ParametersResponse:
    response = await fetch_parameters(parameter, db)
    if len(response.results) == 0:
        raise HTTPException(status_code=404, detail="Parameter not found")
    return response


@router.get(
    "/parameters",
    response_model=ParametersResponse,
    summary="Get a parameters",
    description="Provides a list of parameters",
)
async def parameters_get(
    parameter: Annotated[ParametersQueries, Depends(ParametersQueries.depends())],
    db: DB = Depends(),
):
    response = await fetch_parameters(parameter, db)
    return response


async def fetch_parameters(query, db) -> ParametersResponse:
    query_builder = QueryBuilder(query)
    ## TODO
    sql = f"""
    SELECT id
        , p.name
        , p.display_name
        , p.units
        , p.description
        {query_builder.total()}
    FROM
        parameters_view_cached p
    JOIN
        measurands m ON p.id = m.measurands_id
    {query_builder.where()}
    {query_builder.pagination()}
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
