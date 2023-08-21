from datetime import timedelta
import logging
import time
from os import environ

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.types import ASGIApp
from .settings import settings

from fastapi.responses import JSONResponse
from fastapi import Response, status
from redis import Redis

from openaq_fastapi.models.logging import (
    HTTPLog,
    LogType,
    TooManyRequestsLog,
    UnauthorizedLog,
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


class GetHostMiddleware(BaseHTTPMiddleware):
    """MiddleWare to set servers url on App with current url."""

    async def dispatch(self, request: Request, call_next):
        environ["BASE_URL"] = str(request.base_url)
        response = await call_next(request)

        return response


class LoggingMiddleware(BaseHTTPMiddleware):
    """MiddleWare to set servers url on App with current url."""

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        timing = round(process_time * 1000, 2)
        if hasattr(request.app.state, "rate_limiter"):
            rate_limiter = request.app.state.rate_limiter
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


class RateLimiterMiddleWare(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        redis_client: Redis,
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
        self.counter = 0

    def request_is_limited(self, key: str, limit: int):
        if self.redis_client.setnx(key, limit):
            self.redis_client.expire(key, int(self.rate_time.total_seconds()))
        count = self.redis_client.get(key)
        if count and int(count) > 0:
            self.counter = self.redis_client.decrby(key, 1)
            return False
        return True

    def check_valid_key(self, key: str):
        if self.redis_client.sismember("keys", key):
            return True
        return False

    @staticmethod
    def limited_path(route: str) -> bool:
        allow_list = ["/", "/openapi.json", "/docs", "/register", "/assets"]
        if route in allow_list:
            return False
        if "/v2/locations/tiles" in route:
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
        route = request.url.path
        auth = request.headers.get("x-api-key", None)
        limit = self.rate_amount
        key = request.client.host

        if auth:
            if not self.check_valid_key(auth):
                logging.info(UnauthorizedLog(request=request).model_dump_json())
                return JSONResponse(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    content={"message": "invalid credentials"},
                )
            key = auth
            limit = self.rate_amount_key
        if (
            request.headers.get("Origin", None) == settings.ORIGIN
            and request.headers.get("API-User-Agent", None) == settings.USER_AGENT
        ):
            limit = self.rate_amount_key
        if self.limited_path(route) and self.request_is_limited(key, limit):
            logging.info(
                TooManyRequestsLog(
                    request=request,
                    rate_limiter=f"{key}/{limit}/{self.counter}",
                ).model_dump_json()
            )
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"message": "Too many requests"},
            )

        request.app.state.rate_limiter = f"{key}/{limit}/{self.counter}"
        response = await call_next(request)

        return response
