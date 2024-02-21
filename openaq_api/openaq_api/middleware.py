import logging
import time
from datetime import timedelta, datetime
from os import environ
from fastapi import Response, status
from fastapi.responses import JSONResponse
from redis.asyncio.cluster import RedisCluster
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.types import ASGIApp

from openaq_api.models.logging import (
    HTTPLog,
    LogType,
    TooManyRequestsLog,
    UnauthorizedLog,
    RedisErrorLog,
)

from .settings import settings

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


class GetHostMiddleware(BaseHTTPMiddleware):
    """MiddleWare to set servers url on App with current url."""

    async def dispatch(self, request: Request, call_next):
        environ["BASE_URL"] = str(request.base_url)
        response = await call_next(request)

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


class LoggingMiddleware(BaseHTTPMiddleware):
    """MiddleWare to set servers url on App with current url."""

    async def dispatch(self, request: Request, call_next):
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


class PrivatePathsMiddleware(BaseHTTPMiddleware):
    """
    Middleware to protect private endpoints with an API key
    """

    async def dispatch(self, request: Request, call_next):
        route = request.url.path
        if "/auth" in route:
            auth = request.headers.get("x-api-key", None)
            if auth != settings.EXPLORER_API_KEY:
                return JSONResponse(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    content={"message": "invalid credentials"},
                )
        response = await call_next(request)
        return response


class RateLimiterMiddleWare(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        redis_client: RedisCluster,
        rate_amount: int,  # number of requests allowed without api key
        rate_amount_key: int,  # number of requests allowed with api key
        rate_time: timedelta,  # timedelta of rate limit expiration
    ) -> None:
        """Init Middleware."""
        super().__init__(app)
        self.redis_client = redis_client
        self.rate_amount = rate_amount
        self.rate_amount_key = rate_amount_key
        self.rate_time = rate_time

    async def request_is_limited(self, key: str, limit: int, request: Request) -> bool:
        value = await self.redis_client.get(key)
        if value is None or int(value) < limit:
            async with self.redis_client.pipeline() as pipe:
                [incr, _] = await pipe.incr(key).expire(key, 60).execute()
                request.state.counter = limit - incr
                return False
        else:
            return True

    async def check_valid_key(self, key: str) -> bool:
        if await self.redis_client.sismember("keys", key):
            return True
        return False

    @staticmethod
    def limited_path(route: str) -> bool:
        allow_list = ["/", "/openapi.json", "/docs", "/register"]
        if route in allow_list:
            return False
        if "/v2/locations/tiles" in route:
            return False
        if "/v3/locations/tiles" in route:
            return False
        if "/assets" in route:
            return False
        if ".css" in route:
            return False
        if ".js" in route:
            return False
        return True

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        print("RATE LIMIT\n\n\n")
        route = request.url.path
        auth = request.headers.get("x-api-key", None)
        if auth == settings.EXPLORER_API_KEY:
            response = await call_next(request)
            return response
        limit = self.rate_amount
        now = datetime.now()
        key = f"{request.client.host}:{now.year}{now.month}{now.day}{now.hour}{now.minute}"

        if auth:
            valid_key = await self.check_valid_key(auth)
            if not valid_key:
                logging.info(
                    UnauthorizedLog(
                        request=request, detail=f"invalid key used: {auth}"
                    ).model_dump_json()
                )
                return JSONResponse(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    content={"message": "invalid credentials"},
                )
            key = f"{auth}:{now.year}{now.month}{now.day}{now.hour}{now.minute}"
            limit = self.rate_amount_key
        request.state.counter = limit
        limited = False
        if self.limited_path(route):
            limited = await self.request_is_limited(key, limit, request)
        if self.limited_path(route) and limited:
            logging.info(
                TooManyRequestsLog(
                    request=request,
                    rate_limiter=f"{key}/{limit}/{request.state.counter}",
                ).model_dump_json()
            )
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"message": "Too many requests"},
            )
        request.state.rate_limiter = f"{key}/{limit}/{request.state.counter}"
        ttl = await self.redis_client.ttl(key)
        response = await call_next(request)
        response.headers["RateLimit-Limit"] = str(limit)
        response.headers["RateLimit-Remaining"] = str(request.state.counter)
        response.headers["RateLimit-Reset"] = str(ttl)
        rate_time_seconds = int(self.rate_time.total_seconds())
        if auth:
            response.headers["RateLimit-Policy"] = (
                f"{self.rate_amount_key};w={rate_time_seconds}"
            )
        else:
            response.headers["RateLimit-Policy"] = (
                f"{self.rate_amount};w={rate_time_seconds}"
            )
        return response
