from contextlib import asynccontextmanager
import datetime
import logging
import time
import traceback
from os import environ
from pathlib import Path
from typing import Any

import orjson
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from mangum import Mangum
from pydantic import BaseModel, ValidationError
from starlette.responses import JSONResponse, RedirectResponse

from openaq_api.db import db_pool
from openaq_api.middleware import (
    CacheControlMiddleware,
    LoggingMiddleware,
    PrivatePathsMiddleware,
    RateLimiterMiddleWare,
)
from openaq_api.models.logging import (
    InfrastructureErrorLog,
    ModelValidationError,
    UnprocessableEntityLog,
    WarnLog,
)
#from openaq_api.routers.auth import router as auth_router
from openaq_api.routers.averages import router as averages_router
from openaq_api.routers.cities import router as cities_router
from openaq_api.routers.countries import router as countries_router
from openaq_api.routers.locations import router as locations_router
from openaq_api.routers.manufacturers import router as manufacturers_router
from openaq_api.routers.measurements import router as measurements_router
from openaq_api.routers.mvt import router as mvt_router
from openaq_api.routers.parameters import router as parameters_router
from openaq_api.routers.projects import router as projects_router
from openaq_api.routers.sources import router as sources_router
from openaq_api.routers.summary import router as summary_router
from openaq_api.settings import settings

# V3 routers
from openaq_api.v3.routers import (
    auth,
    countries,
    instruments,
    locations,
    manufacturers,
    measurements,
    owners,
    parameters,
    providers,
    sensors,
    tiles,
    trends,
)

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
    if isinstance(obj, datetime.date):
        return obj.strptime("%Y-%m-%d")


class ORJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        # logger.debug(f'rendering content {content}')
        return orjson.dumps(content, default=default)


redis_client = None  # initialize for generalize_schema.py


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not hasattr(app.state, "pool"):
        logger.debug("initializing connection pool")
        app.state.pool = await db_pool(None)
        logger.debug("Connection pool established")

    if hasattr(app.state, "counter"):
        app.state.counter += 1
    else:
        app.state.counter = 0
    app.state.redis_client = redis_client
    yield
    if hasattr(app.state, "pool") and not settings.USE_SHARED_POOL:
        logger.debug("Closing connection")
        await app.state.pool.close()
        delattr(app.state, "pool")
        logger.debug("Connection closed")


app = FastAPI(
    title="OpenAQ",
    description="OpenAQ API",
    version="2.0.0",
    default_response_class=ORJSONResponse,
    docs_url="/docs",
    lifespan=lifespan,
)


if settings.RATE_LIMITING is True:
    if settings.RATE_LIMITING:
        logger.debug("Connecting to redis")
        from redis.asyncio.cluster import RedisCluster

        try:
            redis_client = RedisCluster(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                decode_responses=True,
                socket_timeout=5,
            )
        except Exception as e:
            logging.error(
                InfrastructureErrorLog(detail=f"failed to connect to redis: {e}")
            )
        print(redis_client)
        logger.debug("Redis connected")
    if redis_client:
        app.add_middleware(
            RateLimiterMiddleWare,
            redis_client=redis_client,
            rate_amount=settings.RATE_AMOUNT,
            rate_amount_key=settings.RATE_AMOUNT_KEY,
            rate_time=datetime.timedelta(minutes=settings.RATE_TIME),
        )
    else:
        logger.warning(
            WarnLog(
                detail="valid redis client not provided but RATE_LIMITING set to TRUE"
            )
        )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(CacheControlMiddleware, cachecontrol="public, max-age=900")
app.add_middleware(LoggingMiddleware)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(PrivatePathsMiddleware)


class OpenAQValidationResponseDetail(BaseModel):
    loc: list[str] | None = None
    msg: str | None = None
    type: str | None = None


class OpenAQValidationResponse(BaseModel):
    detail: list[OpenAQValidationResponseDetail] | None = None


@app.exception_handler(RequestValidationError)
async def openaq_request_validation_exception_handler(
    request: Request, exc: RequestValidationError
):
    return ORJSONResponse(status_code=422, content=jsonable_encoder(str(exc)))
    #return PlainTextResponse(str(exc))
    # print("\n\n\n\n\n")
    # print(str(exc))
    # print("\n\n\n\n\n")
    # detail = orjson.loads(str(exc))
    # logger.debug(traceback.format_exc())
    # logger.info(
    #     UnprocessableEntityLog(request=request, detail=str(exc)).model_dump_json()
    # )
    # detail = OpenAQValidationResponse(detail=detail)
    #return ORJSONResponse(status_code=422, content=jsonable_encoder(detail))


@app.exception_handler(ValidationError)
async def openaq_exception_handler(request: Request, exc: ValidationError):
    return ORJSONResponse(status_code=422, content=jsonable_encoder(str(exc)))
    # detail = orjson.loads(exc.model_dump_json())
    # logger.debug(traceback.format_exc())
    # logger.error(
    #     ModelValidationError(
    #         request=request, detail=exc.jsmodel_dump_jsonon()
    #     ).model_dump_json()
    # )
    #return ORJSONResponse(status_code=422, content=jsonable_encoder(detail))
    # return ORJSONResponse(status_code=500, content={"message": "internal server error"})


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


# v3
app.include_router(auth.router)
app.include_router(instruments.router)
app.include_router(locations.router)
app.include_router(parameters.router)
app.include_router(tiles.router)
app.include_router(countries.router)
app.include_router(manufacturers.router)
app.include_router(measurements.router)
app.include_router(owners.router)
app.include_router(trends.router)
app.include_router(providers.router)
app.include_router(sensors.router)

# app.include_router(auth_router)
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


app.mount("/", StaticFiles(directory=str(static_dir), html=True))


def handler(event, context):
    asgi_handler = Mangum(app)
    return asgi_handler(event, context)


def run():
    attempts = 0
    while attempts < 10:
        try:
            import uvicorn

            uvicorn.run(
                "openaq_api.main:app",
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
