from enum import StrEnum

from fastapi import Request, status
from humps import camelize
from pydantic import BaseModel, ConfigDict, Field, computed_field


class LogType(StrEnum):
    SUCCESS = "SUCCESS"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    INFRASTRUCTURE_ERROR = "INFRASTRUCTURE_ERROR"
    UNPROCESSABLE_ENTITY = "UNPROCESSABLE_ENTITY"
    UNAUTHORIZED = "UNAUTHORIZED"
    TOO_MANY_REQUESTS = "TOO_MANY_REQUESTS"
    WARNING = "WARNING"
    INFO = "INFO"
    ERROR = "ERROR"


class BaseLog(BaseModel):
    """Abstract base class for logging.

    Inherits from Pydantic BaseModel
    """

    type: LogType
    detail: str | None = None

    def model_dump_json(self, **kwargs):
        kwargs["by_alias"] = True
        return super().model_dump_json(**kwargs)

    model_config = ConfigDict(
        alias_generator=camelize, arbitrary_types_allowed=True, populate_by_name=True
    )


class InfoLog(BaseLog):
    type: LogType = LogType.INFO


class WarnLog(BaseLog):
    type: LogType = LogType.WARNING


class ErrorLog(BaseLog):
    type: LogType = LogType.ERROR


class InfrastructureErrorLog(BaseLog):
    type: LogType = LogType.INFRASTRUCTURE_ERROR


class AuthLog(BaseLog):
    type: LogType = LogType.INFO


class SESEmailLog(BaseLog):
    type: LogType = LogType.INFO


class HTTPLog(BaseLog):
    """A base class for logging HTTP requests

    inherits from BaseLog

    Attributes:
        request:
        http_code:
        timing:
        rate_limiter:
        counter:
        ip:
        api_key:
        user-agent:
        path:
        params:
        params_obj:
        params_keys:

    """

    request: Request = Field(..., exclude=True)
    http_code: int
    timing: float | None = None
    rate_limiter: str | None = None
    counter: int | None = None

    @computed_field(return_type=str)
    @property
    def ip(self) -> str:
        """str: returns IP address from request client"""
        return self.request.client.host

    @computed_field(return_type=str)
    @property
    def api_key(self) -> str:
        """str: returns API Key from request headers"""
        return self.request.headers.get("X-API-Key", None)

    @computed_field(return_type=str)
    @property
    def user_agent(self) -> str:
        """str: returns User-Agent from request headers"""
        return self.request.headers.get("User-Agent", None)

    @computed_field(return_type=str)
    @property
    def path(self) -> str:
        """str: returns URL path from request"""
        return self.request.url.path

    @computed_field(return_type=str)
    @property
    def params(self) -> str:
        """str: returns URL query params from request"""
        return self.request.url.query

    @computed_field(return_type=dict)
    @property
    def params_obj(self) -> dict:
        """dict: returns URL query params as key values from request"""
        return dict(x.split("=", 1) for x in self.params.split("&") if "=" in x)

    @computed_field(return_type=list)
    @property
    def params_keys(self) -> list:
        """list: returns URL query params keys as list/array from request"""
        return [] if self.params_obj is None else list(self.params_obj.keys())


class HTTPErrorLog(HTTPLog):
    """Log for HTTP 500.

    Inherits from HTTPLog
    """

    http_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR


class UnprocessableEntityLog(HTTPLog):
    """Log for HTTP 422.

    Inherits from HTTPLog
    """

    http_code: int = status.HTTP_422_UNPROCESSABLE_ENTITY
    type: LogType = LogType.UNPROCESSABLE_ENTITY


class TooManyRequestsLog(HTTPLog):
    """Log for HTTP 429.

    Inherits from HTTPLog
    """

    http_code: int = status.HTTP_429_TOO_MANY_REQUESTS
    type: LogType = LogType.TOO_MANY_REQUESTS


class UnauthorizedLog(HTTPLog):
    """Log for HTTP 401.

    Inherits from HTTPLog
    """

    http_code: int = status.HTTP_401_UNAUTHORIZED
    type: LogType = LogType.UNAUTHORIZED


class ModelValidationError(HTTPErrorLog):
    """Log for model validations

    Inherits from ErrorLog
    """

    type: LogType = LogType.VALIDATION_ERROR


class RedisErrorLog(ErrorLog):
    detail: str
