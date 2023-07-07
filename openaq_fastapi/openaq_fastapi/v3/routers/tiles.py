import logging
import urllib
from typing import List, Union, Annotated
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Request, Response
from pydantic import BaseModel, Field
from openaq_fastapi.db import DB

from openaq_fastapi.v3.models.queries import (
    QueryBaseModel,
    CommaSeparatedList,
    ParametersQuery,
    MonitorQuery,
    MobileQuery,
    QueryBuilder,
)

logger = logging.getLogger("tiles")

router = APIRouter(
    prefix="/v3",
    tags=["v3"],
    include_in_schema=False,
)


class TileProvidersQuery(QueryBaseModel):
    providers_id: Union[CommaSeparatedList[int], None] = Query(
        description="Limit the results to a specific provider or providers"
    )

    def where(self) -> Union[str, None]:
        if self.has("providers_id"):
            return "providers_id = ANY (:providers_id)"


class TileOwnersQuery(QueryBaseModel):
    owners_id: Union[CommaSeparatedList[int], None] = Query(
        description="Limit the results to a specific owner or owners"
    )

    def where(self) -> Union[str, None]:
        if self.has("owners_id"):
            return "owners_id = ANY (:owners_id)"


class ActiveQuery(QueryBaseModel):
    active: Union[bool, None] = Query(
        description="Limits to locations with recent measurements (<48 hours)"
    )

    def where(self) -> Union[str, None]:
        if self.has("active"):
            return "active = :active"


class ThresholdsQuery(QueryBaseModel):
    period: int = Query(description="")
    threshold: int = Query(description="")

    def where(self) -> Union[str, None]:
        if self.has("period") and self.has("threshold"):
            return "threshold = :threshold AND period = :period"


class TileBase(QueryBaseModel):
    z: int = Path(..., ge=0, le=30, description="Mercator tiles's zoom level")
    x: int = Path(..., description="Mercator tiles's column")
    y: int = Path(..., description="Mercator tiles's row")


class Tile(
    TileBase,
    ParametersQuery,
    TileProvidersQuery,
    MonitorQuery,
    MobileQuery,
    TileOwnersQuery,
    ActiveQuery,
):
    ...


class ThresholdTile(
    Tile,
    ThresholdsQuery,
):
    ...


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

    def where(self):
        where = ["WHERE ismobile = false"]
        where.append("measurands_id = :parameters_id")
        if hasattr(self, "providers") and self.providers is not None:
            where.append("providers_id = ANY(:providers)")
        if hasattr(self, "is_monitor") and self.is_monitor is not None:
            where.append("is_monitor = :is_monitor")
        if hasattr(self, "is_active") and self.is_active is not None:
            where.append("active = :is_active")
        return ("\nAND ").join(where)


@router.get(
    "/locations/tiles/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
)
async def get_tile(
    tile: Annotated[Tile, Depends(Tile)],
    db: DB = Depends(),
):
    vt = await fetch_tiles(tile, db)
    if vt is None:
        raise HTTPException(status_code=204, detail="no data found for this tile")

    return Response(content=vt, status_code=200, media_type="application/x-protobuf")


@router.get(
    "/thresholds/tiles/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
)
async def get_threshold_tile(
    threshold_tile: Annotated[ThresholdTile, Depends(ThresholdTile)],
    db: DB = Depends(),
):
    vt = await fetch_threshold_tiles(threshold_tile, db)
    if vt is None:
        raise HTTPException(status_code=204, detail="no data found for this tile")

    return Response(content=vt, status_code=200, media_type="application/x-protobuf")


async def fetch_tiles(query, db):
    query_builder = QueryBuilder(query)
    sql = f"""
    WITH
        tile AS (
            SELECT ST_TileEnvelope(:z,:x,:y) AS tile
        ),
        locations AS (
            SELECT
                locations_view_cached.id AS sensor_nodes_id
                , locations_view_cached.ismobile 
                , locations_view_cached.ismonitor 
                , sensors.measurands_id AS parameters_id
                , ST_AsMVTGeom(ST_Transform(locations_view_cached.geom, 3857), tile) AS mvt
                , sensors_rollup.value_latest AS value
                , sensors_rollup.datetime_last > (NOW() - INTERVAL '48 hours' ) AS active
                , (locations_view_cached.provider->'id')::int AS providers_id 
            FROM
                locations_view_cached
            JOIN
                tile
            ON
                TRUE
            JOIN
                sensor_systems
            ON
                sensor_systems.sensor_nodes_id = locations_view_cached.id
            JOIN
                sensors
            ON
                sensors.sensor_systems_id = sensor_systems.sensor_systems_id
            JOIN
                sensors_rollup
            ON
                sensors_rollup.sensors_id = sensors.sensors_id
        ),
        t AS (
            SELECT
                sensor_nodes_id
                , mvt
                , value
                , active
                , providers_id
                , parameters_id
                , ismonitor
                , ismobile
            FROM
                locations
            {query_builder.where()}
        )
        SELECT ST_AsMVT(t, 'default') FROM t;
    """
    response = await db.fetchval(sql, query_builder.params())
    return response


async def fetch_threshold_tiles(query, db):
    query_builder = QueryBuilder(query)
    sql = f"""
    WITH
        tile AS (
            SELECT ST_TileEnvelope(:z,:x,:y) AS tile
        ),
        thresholds AS (
            SELECT 
                sensor_nodes_id
                , measurands_id
                , days AS period
                , threshold_value AS threshold
                , ((exceedance_count / total_count) * 100)::int AS exceedance
            FROM
                sensor_node_range_exceedances
        ),
        locations AS (
            SELECT
                locations_view_cached.id AS sensor_nodes_id
                , locations_view_cached.ismobile 
                , locations_view_cached.ismonitor 
                , sensors.measurands_id AS measurands_id
                , ST_AsMVTGeom(ST_Transform(locations_view_cached.geom, 3857), tile) AS mvt
                , thresholds.exceedance
                , thresholds.period    
                , thresholds.threshold       
                , sensors_rollup.datetime_last > (NOW() - INTERVAL '48 hours' ) AS active
                , (locations_view_cached.provider->'id')::int AS providers_id 
            FROM
                locations_view_cached
            JOIN
                tile
            ON
                TRUE
            JOIN
                sensor_systems
            ON
                sensor_systems.sensor_nodes_id = locations_view_cached.id
            JOIN
                sensors
            ON
                sensors.sensor_systems_id = sensor_systems.sensor_systems_id
            JOIN
                thresholds
            ON
                thresholds.sensor_nodes_id = locations_view_cached.id AND thresholds.measurands_id = sensors.measurands_id
            JOIN
                sensors_rollup
            ON
                sensors_rollup.sensors_id = sensors.sensors_id
        ),
        t AS (
            SELECT
                sensor_nodes_id
                , mvt
                , period
                , threshold
                , exceedance
                , active
                , providers_id
                , measurands_id
                , ismonitor
                , ismobile
            FROM
                locations
            {query_builder.where()}
        )
        SELECT ST_AsMVT(t, 'default') FROM t;
    """
    response = await db.fetchval(sql, query_builder.params())
    return response


@router.get(
    "/locations/tiles/mobile-generalized/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
)
async def get_mobile_gen_tiles(
    tile: Annotated[Tile, Depends(Tile)],
    db: DB = Depends(),
):
    ...


async def fetch_mobile_gen_tiles(where, db):
    sql = f"""
    """
    response = await db.fetchval(sql, where.params())
    return response


@router.get(
    "/locations/tiles/mobile-paths/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
)
async def get_mobile_path_tiles(
    tile: Annotated[Tile, Depends(Tile)],
    db: DB = Depends(),
):
    ...


async def fetch_mobile_path_tiles(where, db):
    sql = f"""
    """
    response = await db.fetchval(sql, where.params())
    return response


@router.get(
    "/locations/tiles/mobile/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
)
async def get_mobiletiles(
    mt: Annotated[MobileTile, Depends(MobileTile)],
    db: DB = Depends(),
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
    "/locations/tiles/tiles.json",
    response_model=TileJSON,
    responses={200: {"description": "Return a tilejson"}},
    response_model_exclude_none=True,
)
async def tilejson(
    request: Request,
):
    return await tilejsonfunc(request, "get_tile", maxzoom=15)
