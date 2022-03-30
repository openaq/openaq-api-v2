import datetime
import logging
import traceback
from pathlib import Path
import time
from typing import Any, List

import orjson
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from mangum import Mangum
from pydantic import BaseModel, ValidationError, validator
from starlette.responses import JSONResponse, RedirectResponse

from openaq_fastapi.db import db_pool

from openaq_fastapi.models.logging import (
    InfrastructureErrorLog,
    ModelValidationError,
    UnprocessableEntityLog,
    WarnLog,
)

from openaq_fastapi.middleware import (
    CacheControlMiddleware,
    StripParametersMiddleware,
    TotalTimeMiddleware,
    RateLimiterMiddleWare,
    LoggingMiddleware,
)
from openaq_fastapi.routers.averages import router as averages_router
from openaq_fastapi.routers.cities import router as cities_router
from openaq_fastapi.routers.countries import router as countries_router
from openaq_fastapi.routers.locations import router as locations_router
from openaq_fastapi.routers.manufacturers import router as manufacturers_router
from openaq_fastapi.routers.measurements import router as measurements_router
from openaq_fastapi.routers.mvt import router as mvt_router
from openaq_fastapi.routers.parameters import router as parameters_router
from openaq_fastapi.routers.projects import router as projects_router
from openaq_fastapi.routers.sources import router as sources_router
from openaq_fastapi.routers.summary import router as summary_router
from openaq_fastapi.routers.auth import router as auth_router

# V3 routers
from openaq_fastapi.v3.routers import (
    locations,
    measurements,
    trends,
    parameters,
    countries,
    tiles,
    providers,
    sensors,
)


from openaq_fastapi.settings import settings
from os import environ


logging.basicConfig(
    format="[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s",
    level=settings.LOG_LEVEL.upper(),
    force=True,
)
# When debuging we dont want to debug these libraries
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("aiocache").setLevel(logging.WARNING)
logging.getLogger("uvicorn").setLevel(logging.WARNING)
logging.getLogger("mangum").setLevel(logging.WARNING)

logger = logging.getLogger("main")

# Make sure that we are using UTC timezone
# this is required because the datetime class will automatically
# add the env timezone when passing the value to a sql query parameter
environ["TZ"] = "UTC"


# this is instead of importing settings elsewhere
if settings.DOMAIN_NAME is not None:
    environ["DOMAIN_NAME"] = settings.DOMAIN_NAME


def default(obj):
    if isinstance(obj, float):
        return round(obj, 5)
    if isinstance(obj, datetime.datetime):
        return obj.strptime("%Y-%m-%dT%H:%M:%SZ")


class ORJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        # logger.debug(f'rendering content {content}')
        return orjson.dumps(content, default=default)


app = FastAPI(
    title="OpenAQ",
    description="OpenAQ API - https://docs.openaq.org",
    version="2.0.0",
    default_response_class=ORJSONResponse,
    docs_url="/",
    # servers=[{"url": "/"}],
)

redis_client = None  # initialize for generalize_schema.py

# def custom_openapi():
#     logger.debug(f"servers -- {app.state.servers}")
#     if app.state.servers is not None and app.openapi_schema:
#         return app.openapi_schema
#     logger.debug(f"Creating OpenApi Docs with server {app.state.servers}")
#     openapi_schema = get_openapi(
#         title=app.title,
#         description=app.description,
#         servers=app.state.servers,
#         version="2.0.0",
#         routes=app.routes,
#     )
#     # openapi_schema['info']['servers']=app.state.servers
#     app.openapi_schema = openapi_schema
#     return app.openapi_schema


# app.openapi = custom_openapi

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)

# app.add_middleware(StripParametersMiddleware)
app.add_middleware(CacheControlMiddleware, cachecontrol="public, max-age=900")
app.add_middleware(TotalTimeMiddleware)
# app.add_middleware(GetHostMiddleware)


class OpenAQValidationResponseDetail(BaseModel):
    loc: List[str] = None
    msg: str = None
    type: str = None


class OpenAQValidationResponse(BaseModel):
    detail: List[OpenAQValidationResponseDetail] = None


@app.exception_handler(RequestValidationError)
async def openaq_request_validation_exception_handler(
    request: Request, exc: RequestValidationError
):
    detail = orjson.loads(exc.json())
    logger.debug(traceback.format_exc())
    logger.info(UnprocessableEntityLog(request=request, detail=exc.json()).json())
    detail = OpenAQValidationResponse(detail=detail)
    return ORJSONResponse(status_code=422, content=jsonable_encoder(detail))


@app.exception_handler(ValidationError)
async def openaq_exception_handler(request: Request, exc: ValidationError):
    logger.debug(traceback.format_exc())
    logger.error(ModelValidationError(request=request, detail=exc.json()).json())
    return ORJSONResponse(status_code=500, content={"message": "internal server error"})


@app.on_event("startup")
async def startup_event():
    """
    Application startup:
    register the database
    """
    if not hasattr(app.state, "pool"):
        logger.debug("initializing connection pool")
        app.state.pool = await db_pool(None)
        logger.debug("Connection pool established")

    if hasattr(app.state, "counter"):
        app.state.counter += 1
    else:
        app.state.counter = 0


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown: de-register the database connection."""
    if hasattr(app.state, "pool") and not settings.USE_SHARED_POOL:
        logger.debug("Closing connection")
        await app.state.pool.close()
        delattr(app.state, "pool")
        logger.debug("Connection closed")


@app.get("/ping", include_in_schema=False)
def pong():
    """
    health check.
    This will let the user know that the service is operational.
    And this path operation will:
    * show a lifesign
    """
    return {"ping": "pong!"}


@app.get("/favicon.ico", include_in_schema=False)
def favico():
    return RedirectResponse("https://openaq.org/assets/graphics/meta/favicon.png")


app.include_router(locations.router)
app.include_router(parameters.router)
app.include_router(tiles.router)
app.include_router(countries.router)
app.include_router(measurements.router)
app.include_router(trends.router)
app.include_router(providers.router)
app.include_router(sensors.router)

app.include_router(averages_router)
app.include_router(cities_router)
app.include_router(countries_router)
app.include_router(locations_router)
app.include_router(manufacturers_router)
app.include_router(measurements_router)
app.include_router(mvt_router)
app.include_router(parameters_router)
app.include_router(projects_router)
app.include_router(sources_router)
app.include_router(summary_router)


static_dir = Path.joinpath(Path(__file__).resolve().parent, "static")

app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")


def handler(event, context):
    asgi_handler = Mangum(app)
    return asgi_handler(event, context)


def run():
    attempts = 0
    while attempts < 10:
        try:
            import uvicorn

            uvicorn.run(
                "openaq_fastapi.main:app",
                host="0.0.0.0",
                port=8888,
                reload=True,
            )
        except Exception:
            attempts += 1
            logger.debug("waiting for database to start")
            time.sleep(3)
            pass


if __name__ == "__main__":
    run()
