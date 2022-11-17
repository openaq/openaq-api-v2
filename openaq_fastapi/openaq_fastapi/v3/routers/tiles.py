import logging
from typing import List, Union
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Response
from openaq_fastapi.db import DB
from openaq_fastapi.models.queries import OBaseModel

logger = logging.getLogger("tiles")

router = APIRouter()


class TileBase(OBaseModel):
    z: int = (Path(..., ge=0, le=30, description="Mercator tiles's zoom level"),)
    x: int = (Path(..., description="Mercator tiles's column"),)
    y: int = (Path(..., description="Mercator tiles's row"),)


class Tile(TileBase):
    parameters_id: int = Query(
        description="Limit the results to a specific location by id", ge=1
    )
    providers: Union[List[int], None] = Query(
        description="Limit the results to a specific provider by id"
    )
    sensor_types: Union[List[int], None] = Query(
        description="Limit the results to one or more sensor types"
    )

    def clause(self):
        where = ["WHERE TRUE"]
        where.append("parameters_id = :parameter")
        if hasattr(self, "providers_id") and self.providers_id is not None:
            where.append("providers_id = :providers_id")
        if hasattr(self, "providers") and self.providers is not None:
            where.append("providers_id = :providers_id")
        if hasattr(self, "sensor_type") and self.sensor_type is not None:
            where.append("sensor_type = ANY(:sensor_types::int[])")
        return ("\nAND ").join(where)


@router.get(
    "/v3/locations/tiles/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
    tags=["v3"],
    include_in_schema=False,
)
async def get_tile(
    db: DB = Depends(),
    where: Tile = Depends(Tile.depends()),
):
    vt = await fetch_tiles(where, db)
    if vt is None:
        raise HTTPException(status_code=204, detail="no data found for this tile")

    return Response(content=vt, status_code=200, media_type="application/x-protobuf")


async def fetch_tiles(where, db):
    sql = f"""
    """
    response = await db.fetchval(sql, where.params())
    return response
