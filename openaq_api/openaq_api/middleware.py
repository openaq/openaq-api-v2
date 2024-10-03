import logging
import time
from datetime import timedelta, datetime
from os import environ
from fastapi import Response, status, Security, HTTPException
from fastapi.responses import JSONResponse
from redis.asyncio.cluster import RedisCluster
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.types import ASGIApp

from fastapi.security import (
    APIKeyHeader,
)

from openaq_api.models.logging import (
    HTTPLog,
    LogType,
    TooManyRequestsLog,
    UnauthorizedLog,
    RedisErrorLog,
)

from .settings import settings

logger = logging.getLogger("middleware")

NOT_AUTHENTICATED_EXCEPTION = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Invalid credentials",
)

TOO_MANY_REQUESTS = HTTPException(
    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
    detail="To many requests",
)

def is_whitelisted_route(route: str) -> bool:
    logger.debug(f"Checking if '{route}' is whitelisted")
    allow_list = ["/", "/auth", "/openapi.json", "/docs", "/register"]
    if route in allow_list:
        return True
    if "/v2/locations/tiles" in route:
        return True
    if "/v3/locations/tiles" in route:
        return True
    if "/assets" in route:
        return True
    if ".css" in route:
        return True
    if ".js" in route:
        return True
    return False


async def check_api_key(
    request: Request,
    response: Response,
    api_key=Security(APIKeyHeader(name='X-API-Key', auto_error=False)),
    ):
    """
    Check for an api key and then to see if they are rate limited. Throws a
    `not authenticated` or `to many reqests` error if appropriate.
    Meant to be used as a dependency either at the app, router or function level
    """
    route = request.url.path
    # no checking or limiting for whitelistted routes
    if is_whitelisted_route(route):
        return api_key
    elif api_key == settings.EXPLORER_API_KEY:
        return api_key
    else:
        # check to see if we are limiting
        redis = request.app.redis

        if redis is None:
            logger.warning('No redis client found')
            return api_key
        elif api_key is None:
            logger.debug('No api key provided')
            raise NOT_AUTHENTICATED_EXCEPTION
        else:
            # check api key
            limit = settings.RATE_AMOUNT_KEY
            limited = False
            # check valid key
            if await redis.sismember("keys", api_key) == 0:
                logger.debug('Api key not found')
                raise NOT_AUTHENTICATED_EXCEPTION

            # check if its limited
            now = datetime.now()
            # Using a sliding window rate limiting algorithm
            # we add the current time to the minute to the api key and use that as our check
            key = f"{api_key}:{now.year}{now.month}{now.day}{now.hour}{now.minute}"
            # if the that key is in our redis db it will return the number of requests
            # that key has made during the current minute
            value = await redis.get(key)
            ttl = await redis.ttl(key)

            if value is None:
                # if the value is none than we need to add that key to the redis db
                # and set it, increment it and set it to timeout/delete is 60 seconds
                logger.debug('redis no key for current minute so not limited')
                async with redis.pipeline() as pipe:
                    [incr, _] = await pipe.incr(key).expire(key, 60).execute()
                    requests_used = limit - incr
            elif int(value) < limit:
                # if that key does exist and the value is below the allowed number of requests
                # wea re going to increment it and move on
                logger.debug(f'redis - has key for current minute value ({value}) < limit ({limit})')
                async with redis.pipeline() as pipe:
                    [incr, _] = await pipe.incr(key).execute()
                    requests_used = limit - incr
            else:
                # otherwise the user is over their limit and so we are going to throw a 429
                # after we set the headers
                logger.debug(f'redis - has key for current minute and value ({value}) >= limit ({limit})')
                limited = True
                requests_used = value

            response.headers["x-ratelimit-limit"] = str(limit)
            response.headers["x-ratelimit-remaining"] = "0"
            response.headers["x-ratelimit-used"] = str(requests_used)
            response.headers["x-ratelimit-reset"] = str(ttl)

            if limited:
                logging.info(
                    TooManyRequestsLog(
                        request=request,
                        rate_limiter=f"{key}/{limit}/{requests_used}",
                    ).model_dump_json()
                )
                raise TOO_MANY_REQUESTS

            # it would be ideal if we were returing the user information right here
            # even it was just an email address it might be useful
            return api_key


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
