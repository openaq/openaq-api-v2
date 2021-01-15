import logging
from typing import Any, List

from fastapi.exceptions import RequestValidationError
import orjson
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from mangum import Mangum
from pydantic import BaseModel, ValidationError
from starlette.responses import JSONResponse, RedirectResponse
from fastapi.encoders import jsonable_encoder
from .middleware import (
    CacheControlMiddleware,
    GetHostMiddleware,
    StripParametersMiddleware,
    TotalTimeMiddleware,
)
from .routers.averages import router as averages_router

from .routers.locations import router as locations_router
from .routers.parameters import router as parameters_router
from .routers.sources import router as sources_router
from .routers.projects import router as projects_router
from .routers.measurements import router as measurements_router
from .routers.mvt import router as mvt_router
from .routers.cities import router as cities_router
from .routers.countries import router as countries_router
from .routers.manufacturers import router as manufacturers_router

from .settings import settings
from .db import db_pool

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)


class ORJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)


app = FastAPI(
    title="OpenAQ",
    description="API for OpenAQ LCS",
    default_response_class=ORJSONResponse,
    docs_url="/",
    servers=[{"url": "/"}],
)


def custom_openapi():
    logger.debug(f"servers -- {app.state.servers}")
    if app.state.servers is not None and app.openapi_schema:
        return app.openapi_schema
    logger.debug(f"Creating OpenApi Docs with server {app.state.servers}")
    openapi_schema = get_openapi(
        title=app.title,
        description=app.description,
        servers=app.state.servers,
        version="2.0.0",
        routes=app.routes,
    )
    # openapi_schema['info']['servers']=app.state.servers
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(StripParametersMiddleware)
app.add_middleware(CacheControlMiddleware, cachecontrol="public, max-age=900")
app.add_middleware(TotalTimeMiddleware)
app.add_middleware(GetHostMiddleware)


class OpenAQValidationResponseDetail(BaseModel):
    loc: List[str] = None
    msg: str = None
    type: str = None


class OpenAQValidationResponse(BaseModel):
    detail: List[OpenAQValidationResponseDetail] = None


@app.exception_handler(RequestValidationError)
@app.exception_handler(ValidationError)
async def openaq_exception_handler(request, exc):
    detail = orjson.loads(exc.json())
    logger.debug(f"{detail}")
    detail = OpenAQValidationResponse(detail=detail)
    return ORJSONResponse(status_code=422, content=jsonable_encoder(detail))


@app.on_event("startup")
async def startup_event():
    """
    Application startup:
    register the database
    """
    logger.info(f"Connecting to {settings.DATABASE_URL}")
    app.state.pool = await db_pool(None)
    logger.info("Connection established")


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown: de-register the database connection."""
    logger.info("Closing connection to database")
    await app.state.pool.close()
    logger.info("Connection closed")


@app.get("/ping")
def pong():
    """
    Sanity check.
    This will let the user know that the service is operational.
    And this path operation will:
    * show a lifesign
    """
    return {"ping": "pong!"}


@app.get("/favicon.ico")
def favico():
    return RedirectResponse(
        "https://openaq.org/assets/graphics/meta/favicon.png"
    )


# app.include_router(nodes_router)
app.include_router(measurements_router)
app.include_router(averages_router)
app.include_router(locations_router)
app.include_router(cities_router)
app.include_router(countries_router)
app.include_router(mvt_router)
app.include_router(projects_router)
app.include_router(sources_router)
app.include_router(parameters_router)
app.include_router(manufacturers_router)

handler = Mangum(app, enable_lifespan=False)


def run():
    try:
        import uvicorn

        uvicorn.run(
            "openaq_fastapi.main:app", host="0.0.0.0", port=8888, reload=True
        )
    except Exception:
        pass


if __name__ == "__main__":
    run()
