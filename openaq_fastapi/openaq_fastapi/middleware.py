from datetime import timedelta
import logging
import json
import re
import time
from os import environ
from typing import Union

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.types import ASGIApp

from fastapi.responses import JSONResponse
from fastapi import Response, status
from redis import Redis

from openaq_fastapi.models.logging import HTTPLog, LogType, TooManyRequestsLog, UnauthorizedLog

logger = logging.getLogger("middleware")


class CacheControlMiddleware(BaseHTTPMiddleware):
    """MiddleWare to add CacheControl in response headers."""

    def __init__(
        self, app: ASGIApp, cachecontrol: Union[str, None] = None
    ) -> None:
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


class TotalTimeMiddleware(BaseHTTPMiddleware):
    """MiddleWare to add Total process time in response headers."""

    async def dispatch(self, request: Request, call_next):
        """Add X-Process-Time."""
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        request.app.state.timing = round(process_time * 1000, 2)
        # leaving in case we want to add this for authenticated users
        # timings = response.headers.get("Server-Timing")
        # app_time = "total;dur={}".format(round(process_time * 1000, 2))
        # response.headers["Server-Timing"] = (
        #     f"{timings}, {app_time}" if timings else app_time
        # )
        return response


class StripParametersMiddleware(BaseHTTPMiddleware):
    """MiddleWare to strip [] from parameter names."""

    async def dispatch(self, request: Request, call_next):
        newscope = request.scope
        qs = newscope["query_string"].decode("utf-8")
        newqs = re.sub(r"\[\d*\]", "", qs).encode("utf-8")
        newscope["query_string"] = newqs
        new_request = Request(scope=newscope)

        response = await call_next(new_request)

        return response


class GetHostMiddleware(BaseHTTPMiddleware):
    """MiddleWare to set servers url on App with current url."""

    async def dispatch(self, request: Request, call_next):
        environ['BASE_URL'] = str(request.base_url)
        response = await call_next(request)

        return response


class LoggingMiddleware(BaseHTTPMiddleware):
    """MiddleWare to set servers url on App with current url."""
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        if hasattr(request.app.state, 'timing'):
            timing = request.app.state.timing
        else:
            timing = None

        if hasattr(request.app.state, 'rate_limiter'):
            rate_limiter = request.app.state.rate_limiter
        else:
            rate_limiter = None

        if hasattr(request.app.state, 'counter'):
            counter = request.app.state.counter
        else:
            counter = None

        if response.status_code == 200:
            logger.info(HTTPLog(
                request=request,
                type=LogType.SUCCESS,
                http_code=response.status_code,
                timing=timing,
                rate_limiter=rate_limiter,
                counter=counter,
            ).json())
        else:
            logger.info(HTTPLog(
                request=request,
                type=LogType.WARNING,
                http_code=response.status_code,
                timing=timing,
                rate_limiter=rate_limiter,
                counter=counter,
            ).json())
        return response


class RateLimiterMiddleWare(BaseHTTPMiddleware):

    def __init__(
        self, app: ASGIApp,
        redis_client: Redis,
        rate_amount: int, # number of requests allowed without api key
        rate_amount_key: int, # number of requests allowed with api key
        rate_time: timedelta # timedelta of rate limit expiration
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
        if self.redis_client.sismember('keys', key):
            return True
        return False

    @staticmethod
    def limited_path(route: str) -> bool:
        allow_list = ["/", "/openapi.json", "/docs"]
        if route in allow_list:
            return False
        if "/v2/locations/tiles" in route:
            return False
        return True

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        route = request.url.path
        auth = request.headers.get("X-API-Key", None)
        limit = self.rate_amount
        key = request.client.host

        if auth:
            if not self.check_valid_key(auth):
                logging.info(UnauthorizedLog(
                    request=request
                ).json())
                return JSONResponse(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    content={"message": "invalid credentials"}
                )
            key = auth
            limit = self.rate_amount_key

        if self.limited_path(route) and self.request_is_limited(key, limit):
            logging.info(TooManyRequestsLog(
                request=request,
                rate_limiter=f'{key}/{limit}/{self.counter}',
            ).json())
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"message": "Too many requests"}
            )

        request.app.state.rate_limiter = f'{key}/{limit}/{self.counter}'
        response = await call_next(request)

        return response
