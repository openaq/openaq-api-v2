from enum import Enum
from typing import Union

from pydantic import BaseModel, Field, validator
from fastapi import status, Request

from humps import camelize


class LogType(Enum):
    SUCCESS = "SUCCESS"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    INFRASTRUCTURE_ERROR = "INFRASTRUCTURE_ERROR"
    UNPROCESSABLE_ENTITY = "UNPROCESSABLE_ENTITY"
    UNAUTHORIZED = "UNAUTHORIZED"
    TOO_MANY_REQUESTS = "TOO_MANY_REQUESTS"
    WARNING = "WARNING"
    INFO = "INFO"


class BaseLog(BaseModel):
    """
    abstract base class for logging
    """

    type: LogType
    detail: Union[str, None]

    def json(self, **kwargs):
        kwargs["by_alias"] = True
        return super().json(**kwargs)

    class Config:
        alias_generator = camelize
        arbitrary_types_allowed = True
        allow_population_by_field_name = True


class InfoLog(BaseLog):
    type = LogType.INFO


class WarnLog(BaseLog):
    type = LogType.WARNING


class InfrastructureErrorLog(BaseLog):
    type = LogType.INFRASTRUCTURE_ERROR


class HTTPLog(BaseLog):
    http_code: int
    request: Request = Field(..., exclude=True)
    path: Union[str, None]
    params: Union[str, None]
    params_obj: Union[dict, None]
    params_keys: Union[list, None]
    ip: Union[str, None]
    api_key: Union[str, None]
    timing: Union[float, None]
    rate_limiter: Union[str, None]
    counter: Union[str, None]

    @validator("api_key", always=True)
    def set_api_key(cls, v, values) -> dict:
        request = values["request"]
        api_key = request.headers.get("X-API-Key", None)
        return v or api_key

    @validator("ip", always=True)
    def set_ip(cls, v, values) -> dict:
        request = values["request"]
        ip = request.client.host
        return v or ip

    @validator("path", always=True)
    def set_path(cls, v, values) -> dict:
        request = values["request"]
        path = request.url.path
        return v or path

    @validator("params", always=True)
    def set_params(cls, v, values) -> dict:
        request = values["request"]
        params = request.url.query
        return v or params

    @validator("params_obj", always=True)
    def set_params_obj(cls, v, values) -> dict:
        if "=" in values.get("params", ""):
            return v or dict(x.split("=") for x in values["params"].split("&"))
        else:
            return None

    @validator("params_keys", always=True)
    def set_params_keys(cls, v, values) -> dict:
        params = values.get("params_obj", {})
        return [] if params is None else list(params.keys())


class ErrorLog(HTTPLog):
    http_code = status.HTTP_500_INTERNAL_SERVER_ERROR


class UnprocessableEntityLog(HTTPLog):
    http_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    type = LogType.UNPROCESSABLE_ENTITY


class TooManyRequestsLog(HTTPLog):
    http_code = status.HTTP_429_TOO_MANY_REQUESTS
    type = LogType.TOO_MANY_REQUESTS


class UnauthorizedLog(HTTPLog):
    http_code = status.HTTP_401_UNAUTHORIZED
    type = LogType.UNAUTHORIZED


class ModelValidationError(ErrorLog):
    type = LogType.VALIDATION_ERROR
