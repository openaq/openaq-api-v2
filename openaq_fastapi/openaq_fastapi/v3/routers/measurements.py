from fastapi import APIRouter, Depends
from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.responses import MeasurementsResponse


router = APIRouter(
    prefix="/v3",
    tags=["v3"]
)


@router.get(
    "/locations/{location_id}/measurements",
    response_model=MeasurementsResponse,
    summary="Get measurements by location",
    description="Provides a list of measurements by location ID",
)
async def measurements_get(db: DB = Depends()):
    ...
