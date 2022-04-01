import logging
import re
import time
from os import environ
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.types import ASGIApp
from starlette.responses import JSONResponse

from openaq_fastapi.settings import settings

from fastapi import (
    status,
    HTTPException,
)

logger = logging.getLogger(__name__)



class CacheControlMiddleware(BaseHTTPMiddleware):
    """MiddleWare to add CacheControl in response headers."""

    def __init__(
        self, app: ASGIApp, cachecontrol: Optional[str] = None
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
        timings = response.headers.get("Server-Timing")
        app_time = "total;dur={}".format(round(process_time * 1000, 2))
        response.headers["Server-Timing"] = (
            f"{timings}, {app_time}" if timings else app_time
        )
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

        if (
            not hasattr(request.app.state, "servers")
            or request.app.state.servers is None
        ):
            logger.debug(f"***** Setting Servers to {request.base_url} ****")
            request.app.state.servers = [{"url": str(request.base_url)}]
            environ['APP_HOST'] = str(request.base_url)
        else:
            request.app.state.servers = None

        response = await call_next(request)

        return response


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Very simple API key middleware. Requires at least one route
    to include the securty method for the docs to show a form"""

    async def dispatch(self, request: Request, call_next):
        api_key = settings.API_KEY
        whitelist = ["/openapi.json", "/"]
        route = request.url.path
        if route not in whitelist and api_key is not None:
            token = None
            if 'access_token' in request.headers.keys():
                token = request.headers['access_token']
            if 'Authorization' in request.headers.keys():
                token = request.headers['Authorization']
            if token is None:
                return JSONResponse(
                    {
                        "detail": "No API Key provided"
                    },
                    status_code=401
                )
            if token != api_key:
                return JSONResponse(
                    {
                        "detail": "API Key not authorized"
                    },
                    status_code=401
                )

        response = await call_next(request)
        return response
