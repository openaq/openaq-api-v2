import logging
from datetime import datetime
from openaq_api.settings import settings
from fastapi import Security, Response
from starlette.requests import Request

from fastapi.security import (
    APIKeyHeader,
)

from openaq_api.models.logging import (
    TooManyRequestsLog,
    UnauthorizedLog,
)

from openaq_api.exceptions import (
    TooManyRequestsException,
    UnauthorizedException,
)

logger = logging.getLogger("dependencies")


def in_allowed_list(route: str) -> bool:
    logger.debug(f"Checking if '{route}' is allowed")
    allow_list = ["/", "/openapi.json", "/docs", "/register"]
    if route in allow_list:
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
    api_key=Security(APIKeyHeader(name="X-API-Key", auto_error=False)),
):
    """
    Check for an api key and then to see if they are rate limited. Throws a
    `not authenticated` or `too many reqests` error if appropriate.
    Meant to be used as a dependency either at the app, router or function level
    """
    route = request.url.path
    # no checking or limiting for whitelistted routes
    logger.debug(f"Explorer api key: {settings.EXPLORER_API_KEY}")
    if in_allowed_list(route):
        return api_key
    elif api_key == settings.EXPLORER_API_KEY:
        return api_key
    else:
        # check to see if we are limiting
        redis = request.app.redis

        if redis is None:
            logger.warning("No redis client found")
            return api_key
        elif api_key is None:
            logging.info(
                UnauthorizedLog(
                    request=request, detail="api key not provided"
                ).model_dump_json()
            )
            raise UnauthorizedException()
        else:

            # check valid key
            if await redis.sismember("keys", api_key) == 0:
                logging.info(
                    UnauthorizedLog(
                        request=request, detail="api key not found"
                    ).model_dump_json()
                )
                raise UnauthorizedException()
            # check api key
            limit = await redis.hget(api_key, "rate")
            try:
                limit = int(limit)
            except TypeError:
                limit = 60
            limited = False
            # check if its limited
            now = datetime.now()
            # Using a sliding window rate limiting algorithm
            # we add the current time to the minute to the api key and use that as our check
            key = f"{api_key}:{now.year}{now.month}{now.day}{now.hour}{now.minute}"
            # if the that key is in our redis db it will return the number of requests
            # that key has made during the current minute
            requests_used = await redis.get(key)

            if requests_used is None:
                # if the value is none than we need to add that key to the redis db
                # and set it, increment it and set it to timeout/delete is 60 seconds
                logger.debug("redis no key for current minute so not limited")
                async with redis.pipeline() as pipe:
                    [requests_used, _] = await pipe.incr(key).expire(key, 60).execute()
            elif int(requests_used) < limit:
                # if that key does exist and the value is below the allowed number of requests
                # wea re going to increment it and move on
                logger.debug(
                    f"redis - has key for current minute ({requests_used}) < limit ({limit})"
                )
                async with redis.pipeline() as pipe:
                    [requests_used] = await pipe.incr(key).execute()
            else:
                # otherwise the user is over their limit and so we are going to throw a 429
                # after we set the headers
                logger.debug(
                    f"redis - has key for current minute ({requests_used}) >= limit ({limit})"
                )
                limited = True

            ttl = await redis.ttl(key)
            request.state.rate_limiter = (
                f"{key}/{limit}/{requests_used}/{limit - int(requests_used)}/{ttl}"
            )
            rate_limit_headers = {
                "x-ratelimit-limit": str(limit),
                "x-ratelimit-used": str(requests_used),
                "x-ratelimit-remaining": str(limit - int(requests_used)),
                "x-ratelimit-reset": str(ttl),
            }
            response.headers.update(rate_limit_headers)

            if limited:
                logging.info(
                    TooManyRequestsLog(
                        request=request,
                        rate_limiter=f"{key}/{limit}/{requests_used}",
                    ).model_dump_json()
                )
                raise TooManyRequestsException(rate_limit_headers)

            # it would be ideal if we were returing the user information right here
            # even it was just an email address it might be useful
            return api_key
