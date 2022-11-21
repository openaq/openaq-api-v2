import logging
import urllib
from typing import List, Union
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Request, Response
from pydantic import BaseModel, Field
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
    is_monitor: Union[List[int], None] = Query(
        description="Limit the results to one or more sensor types"
    )
    is_active: Union[bool, None] = Query(
        description="Limit the results to locations active within the last 48 hours"
    )

    def clause(self):
        where = ["WHERE ismobile = false"]
        where.append("measurands_id = :parameters_id")
        if hasattr(self, "providers") and self.providers is not None:
            where.append("providers_id = ANY(:providers::int[])")
        if hasattr(self, "is_monitor") and self.is_monitor is not None:
            where.append("is_monitor = :is_monitor")
        if hasattr(self, "is_active") and self.is_active is not None:
            where.append("active = :is_active")
        return ("\nAND ").join(where)


class MobileTile(TileBase):
    parameters_id: int = Query(
        description="Limit the results to a specific location by id", ge=1
    )
    providers: Union[List[int], None] = Query(
        description="Limit the results to a specific provider by id"
    )
    is_monitor: Union[List[int], None] = Query(
        description="Limit the results to one or more sensor types"
    )
    is_active: Union[bool, None] = Query(
        description="Limit the results to locations active within the last 48 hours"
    )

    def clause(self):
        where = ["WHERE ismobile = false"]
        where.append("measurands_id = :parameters_id")
        if hasattr(self, "providers") and self.providers is not None:
            where.append("providers_id = ANY(:providers::int[])")
        if hasattr(self, "is_monitor") and self.is_monitor is not None:
            where.append("is_monitor = :is_monitor")
        if hasattr(self, "is_active") and self.is_active is not None:
            where.append("active = :is_active")
        return ("\nAND ").join(where)


@router.get(
    "/v3/locations/tiles/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
    tags=["v3"],
    include_in_schema=True,
)
async def get_tile(
    db: DB = Depends(),
    tile: Tile = Depends(Tile.depends()),
):
    vt = await fetch_tiles(tile, db)
    if vt is None:
        raise HTTPException(status_code=204, detail="no data found for this tile")

    return Response(content=vt, status_code=200, media_type="application/x-protobuf")


async def fetch_tiles(where, db):
    sql = f"""
    WITH
        tile AS (
            SELECT ST_TileEnvelope(:z,:x,:y) AS tile
        ),
        locations AS (
            SELECT
                sensor_nodes.sensor_nodes_id
                , sensor_nodes.ismobile
                , sensors.measurands_id
                , ST_AsMVTGeom(ST_Transform(sensor_nodes.geom, 3857), tile) AS mvt
                , sensors_latest.value
                , sensors_latest.datetime > (NOW() - INTERVAL '48 hours' ) as active
                , providers.providers_id
            FROM 
                sensor_nodes
            JOIN 
                tile 
            ON 
                TRUE
            JOIN 
                sensor_systems 
            ON 
                sensor_systems.sensor_nodes_id = sensor_nodes.sensor_nodes_id
            JOIN 
                sensors 
            ON 
                sensors.sensor_systems_id = sensor_systems.sensor_systems_id
            JOIN 
                sensors_latest 
            ON 
                sensors_latest.sensors_id = sensors.sensors_id
            JOIN 
                providers 
            ON 
                providers.providers_id = sensor_nodes.providers_id
        ),
        t AS (
            SELECT
                sensor_nodes_id
                , measurands_id
                , mvt
                , value
                , active
                , providers_id
                , true AS is_monitor
            FROM 
                locations
            {where.clause()}
        )
        SELECT ST_AsMVT(t, 'default') FROM t;
    """
    response = await db.fetchval(sql, where.params())
    return response


@router.get(
    "/v3/locations/tiles/mobile-generalized/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
    tags=["v3"],
    include_in_schema=False,
)
async def get_mobile_gen_tiles(
    db: DB = Depends(),
    tile: Tile = Depends(Tile.depends()),
):
    ...


async def fetch_mobile_gen_tiles(where, db):
    sql = f"""
    """
    response = await db.fetchval(sql, where.params())
    return response


@router.get(
    "/v3/locations/tiles/mobile-paths/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
    tags=["v3"],
    include_in_schema=False,
)
async def get_mobile_path_tiles(
    db: DB = Depends(),
    tile: Tile = Depends(Tile.depends()),
):
    ...


async def fetch_mobile_path_tiles(where, db):
    sql = f"""
    """
    response = await db.fetchval(sql, where.params())
    return response


@router.get(
    "/v3/locations/tiles/mobile/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
    tags=["v3"],
    include_in_schema=False,
)
async def get_mobiletiles(
    db: DB = Depends(),
    mt: MobileTile = Depends(MobileTile.depends()),
):
    ...


async def fetch_mobile_tiles(where, db):
    sql = f"""
    """
    response = await db.fetchval(sql, where.params())
    return response


async def tilejsonfunc(
    request: Request,
    endpoint: str,
    minzoom: int = 0,
    maxzoom: int = 30,
):
    """Return TileJSON document."""
    kwargs = {
        "z": "{z}",
        "x": "{x}",
        "y": "{y}",
    }
    params = urllib.parse.unquote(request.url.query)
    tile_endpoint = request.url_for(endpoint, **kwargs).replace("\\", "")
    if params is not None:
        tile_endpoint = f"{tile_endpoint}?{params}"
    return {
        "minzoom": minzoom,
        "maxzoom": maxzoom,
        "name": "table",
        "tiles": [tile_endpoint],
    }


class TileJSON(BaseModel):
    """
    TileJSON model.
    Based on https://github.com/mapbox/tilejson-spec/tree/master/2.2.0
    """

    tilejson: str = "2.2.0"
    name: Union[str, None]
    description: Union[str, None]
    version: str = "1.0.0"
    attribution: Union[str, None]
    template: Union[str, None]
    legend: Union[str, None]
    scheme: str = "xyz"
    tiles: List[str]
    grids: List[str] = []
    data: List[str] = []
    minzoom: int = Field(0, ge=0, le=30)
    maxzoom: int = Field(30, ge=0, le=30)
    bounds: List[float] = [-180, -90, 180, 90]


@router.get(
    "/v3/locations/tiles/tiles.json",
    response_model=TileJSON,
    responses={200: {"description": "Return a tilejson"}},
    response_model_exclude_none=True,
    tags=["v3"],
    include_in_schema=False,
)
async def tilejson(
    request: Request,
):
    return await tilejsonfunc(request, "get_tile", maxzoom=15)
