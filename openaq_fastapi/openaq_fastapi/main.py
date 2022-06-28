import datetime
import logging
from pathlib import Path
import time
from typing import Any, List

import orjson
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from mangum import Mangum
from pydantic import BaseModel, ValidationError
from starlette.responses import JSONResponse, RedirectResponse

from openaq_fastapi.db import db_pool
from openaq_fastapi.middleware import (
    CacheControlMiddleware,
    StripParametersMiddleware,
    TotalTimeMiddleware,
    RateLimiterMiddleWare
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
from openaq_fastapi.settings import settings
from os import environ

logging.basicConfig(
    format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
    level=settings.LOG_LEVEL.upper(),
    force=True,
)
logger = logging.getLogger('main')

# this is instead of importing settings elsewhere
environ['DOMAIN_NAME'] = settings.DOMAIN_NAME


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
    terms_of_service="https://github.com/openaq/openaq-info/blob/master/DATA-POLICY.md",
    docs_url="/docs",
)

if settings.RATE_LIMITING:
    import redis
    redis_client = redis.RedisCluster(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
        skip_full_coverage_check=True
    )


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

if settings.RATE_LIMITING == True:
    app.add_middleware(
        RateLimiterMiddleWare,
        redis_client=redis_client,
        rate_amount=settings.RATE_AMOUNT,
        rate_amount_key=settings.RATE_AMOUNT_KEY,
        rate_time=datetime.timedelta(minutes=settings.RATE_TIME)
    )

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
    logger.debug(f"Connecting to database")
    app.state.pool = await db_pool(None)
    logger.debug("Connection established")


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown: de-register the database connection."""
    logger.debug("Closing connection to database")
    await app.state.pool.close()
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
    return RedirectResponse(
        "https://openaq.org/assets/graphics/meta/favicon.png"
    )


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


static_dir = Path.joinpath(Path(__file__).resolve().parent, 'static')

app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

handler = Mangum(app)


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
