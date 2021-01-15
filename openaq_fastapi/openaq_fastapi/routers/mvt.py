import logging
import os
import pathlib

from fastapi import APIRouter, Depends, Path, Response, Query
from fastapi.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.templating import Jinja2Templates
from typing import Union
from datetime import datetime, date, timedelta
from typing import List, Optional

from pydantic import BaseModel, Field
from ..models.queries import fix_datetime
from ..db import DB
import urllib

templates = Jinja2Templates(
    directory=os.path.join(
        str(pathlib.Path(__file__).parent.parent), "templates"
    )
)


class TileJSON(BaseModel):
    """
    TileJSON model.
    Based on https://github.com/mapbox/tilejson-spec/tree/master/2.2.0
    """

    tilejson: str = "2.2.0"
    name: Optional[str]
    description: Optional[str]
    version: str = "1.0.0"
    attribution: Optional[str]
    template: Optional[str]
    legend: Optional[str]
    scheme: str = "xyz"
    tiles: List[str]
    grids: List[str] = []
    data: List[str] = []
    minzoom: int = Field(0, ge=0, le=30)
    maxzoom: int = Field(30, ge=0, le=30)
    bounds: List[float] = [-180, -90, 180, 90]


logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


@router.get(
    "/v2/locations/tiles/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
    tags=["v2"],
)
async def get_tile(
    z: int = Path(..., ge=0, le=30, description="Mercator tiles's zoom level"),
    x: int = Path(..., description="Mercator tiles's column"),
    y: int = Path(..., description="Mercator tiles's row"),
    isMobile: bool = None,
    parameter: Union[int, str] = None,
    location: Optional[int] = None,
    lastUpdatedFrom: Union[datetime, date, None] = None,
    lastUpdatedTo: Union[datetime, date, None] = None,
    db: DB = Depends(),
):
    """Return vector tile."""
    params = {"z": z, "x": x, "y": y}

    dffrom = ""
    if lastUpdatedFrom is not None:
        params["lastUpdatedFrom"] = fix_datetime(lastUpdatedFrom)
        dffrom = " AND last_datetime >= :lastUpdatedFrom "

    dfto = ""
    if lastUpdatedTo is not None:
        params["lastUpdatedTo"] = fix_datetime(lastUpdatedTo)
        dfto = " AND last_datetime <= :lastUpdatedTo "

    paramcols = ""
    paramwhere = ""
    paramgroup = "1,2,3,4,5"

    lwhere = " TRUE AND "
    if location is not None:
        params["location"] = location
        lwhere = " location_id = :location AND "

    if parameter is not None:
        params["parameter"] = parameter
        paramcols = """
            measurand as parameter,
            units as unit,
            last(last_value, last_datetime) as "lastValue",
            """
        if isinstance(parameter, int):
            paramwhere = " measurands_id=:parameter AND "
        else:
            paramwhere = " measurand=:parameter AND "
        paramgroup = "1,2,3,4,5,6,7"

    ismobileq = ""
    if isMobile is not None:
        params["ismobile"] = isMobile
        ismobileq = " AND ismobile=:ismobile "

    query = f"""
        WITH
        tile AS (
            SELECT ST_TileEnvelope(:z,:x,:y) as tile
        ),
        t AS (
            SELECT
                location_id as "locationId",
                last_datetime as "lastUpdated",
                country,
                ismobile as "isMobile",
                "sensorType",
                {paramcols}
                sum(count) as count,
                ST_AsMVTGeom(
                    last(geom, last_datetime),
                    tile
                ) as mvt
            FROM locations, tile
            WHERE
            {paramwhere}
            {lwhere}
            geom && tile
            {dffrom}
            {dfto}
            {ismobileq}
            GROUP BY {paramgroup}, tile
        ),
        bounds AS (
            SELECT
                DISTINCT
                location_id as "locationId",
                ST_AsMVTGeom(
                    locations.bounds,
                    tile
                ) as mvt
            FROM locations, tile
            WHERE
            ismobile
            AND {paramwhere}
            {lwhere}
            bounds && tile
            {dffrom}
            {dfto}
        )
        SELECT
            (SELECT ST_AsMVT(t, 'default') FROM t)
            ||
            (SELECT ST_AsMVT(bounds, 'bounds') FROM bounds);
    """

    vt = await db.fetchval(query, params)
    if vt is None:
        raise HTTPException(
            status_code=204, detail="no data found for this tile"
        )

    return Response(
        content=vt, status_code=200, media_type="application/x-protobuf"
    )


@router.get(
    "/v2/locations/tiles/mobile/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
    tags=["v2"],
)
async def get_mobiletile(
    z: int = Path(..., ge=0, le=30, description="Mercator tiles's zoom level"),
    x: int = Path(...,ge=0, lt=99999, description="Mercator tiles's column"),
    y: int = Path(...,ge=0, lt=99999,  description="Mercator tiles's row"),
    parameter: str = None,
    dateFrom: Union[datetime, date] = Query(
        datetime.now() - timedelta(days=7)
    ),
    dateTo: Union[datetime, date] = Query(datetime.now()),
    db: DB = Depends(),
):
    """Return vector tile."""
    params = {"z": z, "x": x, "y": y}

    dffrom = ""
    if dateFrom is not None:
        params["datefrom"] = fix_datetime(dateFrom)
        dffrom = " AND datetime >= :datefrom "

    dfto = ""
    if dateTo is not None:
        params["dateto"] = fix_datetime(dateTo)
        dfto = " AND datetime <= :dateto "

    if (dateTo - dateFrom).total_seconds() > 60 * 60 * 24 * 32:
        raise HTTPException(
            status_code=422,
            detail=[
                {
                    "loc": [],
                    "msg": (
                        "Max date range allowed for "
                        "viewing individual points is 1 month"
                    ),
                    "type": None,
                }
            ],
        )

    paramcols = ""
    paramwhere = ""
    value = " null::float as value, "
    if parameter is not None:
        params["parameter"] = parameter
        paramcols = " measurand as parameter, units as unit, value, "
        value = " value, "
        paramwhere = " AND measurand=:parameter "

    query = f"""
        WITH
        tile AS (
            SELECT st_setsrid(ST_TileEnvelope(:z,:x,:y),3857) as tile
        ),
        tile4326 AS (
            SELECT st_transform(tile, 4326) as tile4326 FROM tile
        ),
        l AS (
            SELECT
                DISTINCT
                location_id,
                sensors_id,
                measurand,
                units
            FROM locations, tile
            WHERE
                ismobile
                AND
                bounds && tile
                 {paramwhere}
        ),
        t AS (
            SELECT
                sensors_id,
                datetime,
                {value}
                ST_ASMVTGeom(
                    pt3857(
                        lon,
                        lat
                    ),
                    tile
                ) as mvt
            FROM
            tile, tile4326,
            measurements
            WHERE
            sensors_id IN (SELECT sensors_id FROM l)
            AND
            lon IS NOT NULL AND lat IS NOT NULL
            AND lon <= st_xmax(tile4326) AND lon >= st_xmin(tile4326)
            AND lat <= st_ymax(tile4326) AND lat >= st_ymin(tile4326)
             {dffrom}
             {dfto}
            ORDER BY 2 DESC LIMIT 1000
        ),
        joined AS (
            SELECT
                location_id as "locationId",
                datetime as "dateTime",
                {paramcols}
                mvt
            FROM
                l JOIN t USING (sensors_id)
        )
        SELECT ST_AsMVT(joined, 'default') FROM joined;
    """

    vt = await db.fetchval(query, params)
    if vt is None:
        raise HTTPException(
            status_code=204, detail="no data found for this tile"
        )

    return Response(
        content=vt, status_code=200, media_type="application/x-protobuf"
    )


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


@router.get(
    "/v2/locations/tiles/tiles.json",
    response_model=TileJSON,
    responses={200: {"description": "Return a tilejson"}},
    response_model_exclude_none=True,
    tags=["v2"],
)
async def tilejson(
    request: Request,
):
    return await tilejsonfunc(request, "get_tile", maxzoom=15)


@router.get(
    "/v2/locations/tiles/mobile/tiles.json",
    response_model=TileJSON,
    responses={200: {"description": "Return a tilejson"}},
    response_model_exclude_none=True,
    tags=["v2"],
)
async def mobiletilejson(request: Request):
    return await tilejsonfunc(request, "get_mobiletile", minzoom=8, maxzoom=15)


@router.get(
    "/v2/locations/tiles/viewer", response_class=HTMLResponse, tags=["v2"]
)
def demo(request: Request):
    params = urllib.parse.unquote(request.url.query)
    """Demo for each table."""
    tile_url = request.url_for("tilejson").replace("\\", "")
    mobiletile_url = request.url_for("mobiletilejson").replace("\\", "")
    if params is not None:
        tile_url = f"{tile_url}?{params}"
        mobiletile_url = f"{mobiletile_url}?{params}"
    context = {
        "endpoint": tile_url,
        "mobileendpoint": mobiletile_url,
        "request": request,
    }
    return templates.TemplateResponse(
        name="vtviewer.html", context=context, media_type="text/html"
    )
