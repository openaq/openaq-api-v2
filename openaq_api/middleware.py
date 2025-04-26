import logging
import time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.types import ASGIApp
import uuid  # Add at the top

from openaq_api.models.logging import (
    HTTPLog,
    LogType,
)

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
        self.last_mark = now
        self.marks.append(mrk)
        logger.debug(f"TIMER ({key}): {mrk['since']}")
        return mrk.get(return_time)


class LoggingMiddleware(BaseHTTPMiddleware):
    """MiddleWare to set servers url on App with current url."""

    async def dispatch(self, request: Request, call_next):
        request.state.timer = Timer()

        # ✨ Step 1: Get or generate X-Request-ID
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        request.state.request_id = request_id  # Optional: Store in request.state

        # ✨ Step 2: Proceed with the response
        response = await call_next(request)

        # ✨ Step 3: Add to response headers
        response.headers["X-Request-ID"] = request_id
        # ✨ Dynamically infer version from path or default to "v2"
        segments = request.url.path.strip("/").split("/")
        if segments and segments[0].startswith("v") and segments[0][1:].isdigit():
            version = segments[0]
        else:
            version = "v2"  # fallback if path is just "/" or doesn't have a version prefix


        response.headers["X-API-Version"] = version

        timing = request.state.timer.mark("process")
        response.headers["X-Response-Time"] = f"{timing}ms"

        # ➕ New: Aggregate stats by method+path
        stats_key = f"{request.method} {request.url.path}"
        if not hasattr(request.app.state, "endpoint_stats"):
            request.app.state.endpoint_stats = {}

        stats = request.app.state.endpoint_stats.setdefault(stats_key, {"count": 0, "total_time_ms": 0})
        stats["count"] += 1
        stats["total_time_ms"] += timing

        if hasattr(request.state, "rate_limiter"):
            rate_limiter = request.state.rate_limiter
        else:
            rate_limiter = None
        if hasattr(request.app.state, "counter"):
            counter = request.app.state.counter
        else:
            counter = None
        api_key = request.headers.get("x-api-key", None)
        if response.status_code == 200:
            logger.info(
                HTTPLog(
                    request=request,
                    type=LogType.SUCCESS,
                    http_code=response.status_code,
                    timing=timing,
                    rate_limiter=rate_limiter,
                    counter=counter,
                    api_key=api_key,
                ).model_dump_json()
            )
        else:
            logger.info(
                HTTPLog(
                    request=request,
                    type=LogType.WARNING,
                    http_code=response.status_code,
                    timing=timing,
                    rate_limiter=rate_limiter,
                    counter=counter,
                    api_key=api_key,
                ).model_dump_json()
            )
        return response
