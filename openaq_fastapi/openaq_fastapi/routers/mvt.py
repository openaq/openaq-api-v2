import logging
import os
import pathlib
import urllib
from datetime import date, datetime
from pydantic.typing import List, Union

from fastapi import APIRouter, Depends, Path, Query, Response
from fastapi.exceptions import HTTPException
from pydantic import BaseModel, Field
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.templating import Jinja2Templates

from ..db import DB
from ..models.queries import OBaseModel, fix_datetime

templates = Jinja2Templates(
    directory=os.path.join(str(pathlib.Path(__file__).parent.parent), "templates")
)


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


logger = logging.getLogger("mvt")

router = APIRouter()


class TileBase(OBaseModel):
    z: int = Path(..., ge=0, le=30, description="Mercator tiles's zoom level")
    x: int = Path(..., description="Mercator tiles's column")
    y: int = Path(..., description="Mercator tiles's row")


class MobileTile(TileBase):
    parameter: Union[int, None] = Query(None)
    location: Union[List[int], None] = Query(
        None, description="limit data to location id"
    )
    lastUpdatedFrom: Union[Union[datetime, date], None] = None
    lastUpdatedTo: Union[Union[datetime, date], None] = None
    isMobile: Union[bool, None] = None
    project: Union[int, None] = None
    isAnalysis: Union[bool, None] = None

    def try_cast(self, value: str):
        try:
            int(value)
            self.parameter = int(value)
            return True
        except ValueError:
            return False

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                if f == "location" and all(isinstance(x, int) for x in v):
                    wheres.append(" location_id = ANY(:location::int[]) ")
                elif f == "parameter" and self.try_cast(v):
                    wheres.append(" measurands_id = (:parameter)::int ")
                elif f == "parameter":
                    wheres.append(" measurand = :parameter ")
                elif f == "lastUpdatedFrom":
                    self.lastUpdatedTo = fix_datetime(v)
                    wheres.append(" last_datetime >= :last_updated_from ")
                elif f == "lastUpdatedTo":
                    self.lastUpdatedFrom = fix_datetime(v)
                    wheres.append(" last_datetime >= :last_updated_to ")
                elif f == "isMobile":
                    wheres.append(" ismobile=:is_mobile ")
                elif f == "isAnalysis":
                    wheres.append(" is_analysis=:is_analysis ")
                elif f == "project":
                    wheres.append(
                        "project_in_nodes(ARRAY[location_id],ARRAY[:project::int]) "
                    )
        wheres = list(filter(None, wheres))
        wheres.append(" location_id not in (61485,61505,61506) ")
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "

    def paramcols(self):
        paramcols = ""
        if self.parameter is not None:
            paramcols = """
                measurand as parameter,
                units as unit,
                last(last_value, last_datetime) as "lastValue",
                """
        return paramcols

    def paramgroup(self):
        paramgroup = "1,2,3,4,5,6"
        if self.parameter is not None:
            paramgroup = "1,2,3,4,5,6,7,8"
        return paramgroup


@router.get(
    "/v2/locations/tiles/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
    tags=["v2"],
    include_in_schema=False,
)
async def get_tile(
    z: int = Path(..., ge=0, le=30, description="Mercator tiles's zoom level"),
    x: int = Path(..., description="Mercator tiles's column"),
    y: int = Path(..., description="Mercator tiles's row"),
    db: DB = Depends(),
    m: MobileTile = Depends(),
):
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
                is_analysis as "isAnalysis",
                {t.paramcols()}
                sum(count) as count,
                ST_AsMVTGeom(
                    last(geom, last_datetime),
                    tile
                ) as mvt
            FROM locations, tile
            WHERE
            {t.where()}
            AND
            geom && tile
            GROUP BY {t.paramgroup()}, tile
        )

        SELECT
            (SELECT ST_AsMVT(t, 'default') FROM t);
    """

    vt = await db.fetchval(query, t.params())
    if vt is None:
        raise HTTPException(status_code=204, detail="no data found for this tile")

    return Response(content=vt, status_code=200, media_type="application/x-protobuf")


@router.get(
    "/v2/locations/tiles/mobile/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
    tags=["v2"],
    include_in_schema=False,
)
async def get_mobiletile(
    db: DB = Depends(),
    t: MobileTile = Depends(),
    dateFrom: Union[datetime, date] = Query(...),
    dateTo: Union[datetime, date] = Query(...),
):
    params = t.params()
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
    value = " null::float as value, "
    if t.parameter is not None:
        paramcols = " measurand as parameter, units as unit, value, "
        value = " value, "

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
                {t.where()}
                AND
                ismobile
                AND
                bounds && tile
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
        SELECT ST_AsMVT(joined, 'default') FROM joined
        ;
    """

    vt = await db.fetchval(query, params)
    if vt is None:
        raise HTTPException(status_code=204, detail="no data found for this tile")

    return Response(content=vt, status_code=200, media_type="application/x-protobuf")


@router.get(
    "/v2/locations/tiles/mobile-generalized/{z}/{x}/{y}.pbf",
    responses={200: {"content": {"application/x-protobuf": {}}}},
    response_class=Response,
    tags=["v2"],
    include_in_schema=False,
)
async def get_mobilegentile(
    db: DB = Depends(),
    t: MobileTile = Depends(),
):
    query = f"""
        WITH
        tile AS (
            SELECT
                st_setsrid(ST_TileEnvelope(:z,:x,:y),3857) as tile,
                600000 / 2^:z as precision
        ),
        t AS (
            SELECT
                location_id as "locationId",
                last_datetime as "lastUpdated",
                country,
                ismobile as "isMobile",
                "sensorType",
                is_analysis as "isAnalysis",
                {t.paramcols()}
                sum(count) as count
            FROM locations, tile
            WHERE
            {t.where()}
            AND
            bounds && st_expand(tile,50)
            AND ismobile
            GROUP BY {t.paramgroup()}, tile
        ),
        nodes AS (SELECT array_agg("locationId") as nodes FROM t),
        mobile AS (
            SELECT
                sensor_nodes_id as "locationId",
                ST_AsMVTGeom(
                    st_snaptogrid(geom,precision),
                    tile
                ) as mvt
            FROM
                mobile_generalized, nodes, tile
            WHERE
                geom && tile AND sensor_nodes_id = ANY(nodes.nodes)
            GROUP BY 1,2
        ), bounds AS (
            SELECT
                to_jsonb(t),
                ST_AsMVTGeom(
                    box,
                    tile,
                    NULL,
                    512,
                    false
                ) as mvt
            FROM
                mobile_gen_boxes
                JOIN t on (sensor_nodes_id="locationId")
                , tile
            WHERE box && tile
        )
         SELECT
            (SELECT ST_AsMVT(mobile, 'default') FROM mobile)
            ||
            (SELECT ST_AsMVT(bounds, 'bounds') FROM bounds)
        ;
    """

    vt = await db.fetchval(query, t.params())
    if vt is None:
        raise HTTPException(status_code=204, detail="no data found for this tile")

    return Response(content=vt, status_code=200, media_type="application/x-protobuf")


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
    include_in_schema=False,
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
    include_in_schema=False,
)
async def mobiletilejson(request: Request):
    return await tilejsonfunc(request, "get_mobiletile", minzoom=8, maxzoom=18)


@router.get(
    "/v2/locations/tiles/mobile-generalized/tiles.json",
    response_model=TileJSON,
    responses={200: {"description": "Return a tilejson"}},
    response_model_exclude_none=True,
    tags=["v2"],
    include_in_schema=False,
)
async def mobilegentilejson(request: Request):
    return await tilejsonfunc(request, "get_mobilegentile", maxzoom=24)


@router.get(
    "/v2/locations/tiles/viewer",
    response_class=HTMLResponse,
    tags=["v2"],
    include_in_schema=False,
)
def demo(request: Request):
    params = urllib.parse.unquote(request.url.query)
    """Demo for each table."""
    tile_url = request.url_for("tilejson").replace("\\", "")
    mobiletile_url = request.url_for("mobiletilejson").replace("\\", "")
    mobilegen_url = request.url_for("mobilegentilejson").replace("\\", "")
    if params is not None:
        tile_url = f"{tile_url}?{params}"
        mobiletile_url = f"{mobiletile_url}?{params}"
        mobilegen_url = f"{mobilegen_url}?{params}"
    context = {
        "endpoint": tile_url,
        "mobileendpoint": mobiletile_url,
        "mobilegenendpoint": mobilegen_url,
        "request": request,
    }
    return templates.TemplateResponse(
        name="vtviewer.html", context=context, media_type="text/html"
    )
