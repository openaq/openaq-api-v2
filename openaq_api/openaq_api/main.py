from contextlib import asynccontextmanager
import datetime
import logging
import time
from os import environ
from pathlib import Path
from typing import Any

import orjson
from fastapi import FastAPI, Request, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from mangum import Mangum
from pydantic import BaseModel, ValidationError
from starlette.responses import JSONResponse, RedirectResponse

from openaq_api.db import db_pool
from openaq_api.dependencies import check_api_key
from openaq_api.middleware import (
    CacheControlMiddleware,
    LoggingMiddleware,
)
from openaq_api.models.logging import InfrastructureErrorLog

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
    licenses,
    latest,
    flags,
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
    dependencies=[Depends(check_api_key)],
    docs_url="/docs",
    lifespan=lifespan,
)


app.redis = None
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
            # attach to the app so it can be retrieved via the request
            app.redis = redis_client
            logger.debug("Redis connected")

        except Exception as e:
            logging.error(
                InfrastructureErrorLog(detail=f"failed to connect to redis: {e}")
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


@app.exception_handler(ValidationError)
async def openaq_exception_handler(request: Request, exc: ValidationError):
    return ORJSONResponse(status_code=422, content=jsonable_encoder(str(exc)))


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
app.include_router(licenses.router)
app.include_router(parameters.router)
app.include_router(tiles.router)
app.include_router(countries.router)
app.include_router(manufacturers.router)
app.include_router(measurements.router)
app.include_router(owners.router)
app.include_router(providers.router)
app.include_router(sensors.router)
app.include_router(latest.router)
app.include_router(flags.router)


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
