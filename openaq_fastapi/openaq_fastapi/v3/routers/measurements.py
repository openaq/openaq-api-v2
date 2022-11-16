from fastapi import APIRouter, Depends
from openaq_fastapi.db import DB
from openaq_fastapi.models.v3.responses import MeasurementsResponse


router = APIRouter()


@router.get(
    "/v3/locations/{location_id}/measurements",
    response_model=MeasurementsResponse,
    summary="Get measurements by location",
    description="Provides a list of measurements by location ID",
    tags=["v3"],
)
async def measurements_get(db: DB = Depends()):
    ...
