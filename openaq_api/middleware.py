import logging
import os
import time
from http import HTTPMethod
from fastapi.encoders import jsonable_encoder
from fastapi.responses import ORJSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.types import ASGIApp
from starlette.background import BackgroundTask

from openaq_api.db import DB
from fastapi import Depends

from openaq_api.models.logging import (
    HTTPLog,
    LogType,
)
from openaq_api.v3.models.responses import MethodNotAllowedError

logger = logging.getLogger("middleware")


class CacheControlMiddleware(BaseHTTPMiddleware):
    """MiddleWare to add CacheControl in response headers."""

    def __init__(self, app: ASGIApp, cachecontrol: str | None = None) -> None:
        """Init Middleware."""
        super().__init__(app)
        self.cachecontrol = cachecontrol

    async def dispatch(self, request: Request, call_next):
        """Add cache-control."""
        response = await call_next(request)

        if (
            not response.headers.get("Cache-Control")
            and self.cachecontrol
            and request.method in ["HEAD", "GET"]
            and response.status_code < 500
        ):
            response.headers["Cache-Control"] = self.cachecontrol
        return response


class Timer:
    def __init__(self):
        self.start_time = time.time()
        self.last_mark = self.start_time
        self.marks = []

    def mark(self, key: str, return_time: str = "total") -> float:
        now = time.time()
        mrk = {
            "key": key,
            "since": round((now - self.last_mark) * 1000, 1),
            "total": round((now - self.start_time) * 1000, 1),
        }
        self.last_make = now
        self.marks.append(mrk)
        logger.debug(f"TIMER ({key}): {mrk['since']}")
        return mrk.get(return_time)


async def logEntry(entry: HTTPLog, db: DB):
    await db.post_log(entry)
    ## delete me later
    logger.info(entry.model_dump_json())


class LoggingMiddleware(BaseHTTPMiddleware):
    """MiddleWare to set servers url on App with current url."""

    async def dispatch(
        self,
        request: Request,
        call_next,
    ):
        request.state.timer = Timer()
        response = await call_next(request)
        timing = request.state.timer.mark("process")
        if hasattr(request.state, "rate_limiter"):
            rate_limiter = request.state.rate_limiter
        else:
            rate_limiter = None
        if hasattr(request.app.state, "counter"):
            counter = request.app.state.counter
        else:
            counter = None
        api_key = request.headers.get("x-api-key", None)

        entry = HTTPLog(
            request=request,
            type=LogType.SUCCESS if response.status_code == 200 else LogType.WARNING,
            http_code=response.status_code,
            timing=timing,
            rate_limiter=rate_limiter,
            counter=counter,
            api_key=api_key,
        )

        if os.environ.get("LOGGING_DB"):
            response.background = BackgroundTask(logEntry, entry, DB(request))

        return response
