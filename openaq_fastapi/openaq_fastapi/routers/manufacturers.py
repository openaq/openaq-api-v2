import logging

from fastapi import APIRouter, Depends
from openaq_fastapi.models.responses import ManufacturersResponse, ModelsResponse
from ..db import DB


logger = logging.getLogger("manufacturers")
logger.setLevel(logging.DEBUG)

router = APIRouter()


@router.get(
    "/v2/manufacturers", 
    response_model=ManufacturersResponse, 
    summary="Get a list of manufacturers",
    tags=["v2"])
async def mfr_get(db: DB = Depends()):

    q = """
    WITH t AS (
    select distinct metadata->>'manufacturer_name' as vals from sensor_systems
    where metadata ? 'manufacturer_name'
    )
    SELECT count(*) OVER () as count, to_jsonb(vals) as json FROM t
    """

    output = await db.fetchOpenAQResult(q, {"page": 1, "limit": 1000})

    return output


@router.get(
    "/v2/models", 
    response_model=ModelsResponse,
    summary="Get a list of models", 
    tags=["v2"])
async def model_get(db: DB = Depends()):

    q = """
    WITH t AS (
    select distinct metadata->>'model_name' as vals from sensor_systems
    where metadata ?'model_name'
    )
    SELECT count(*) OVER () as count, to_jsonb(vals) as json FROM t
    """

    output = await db.fetchOpenAQResult(q, {"page": 1, "limit": 1000})

    return output
