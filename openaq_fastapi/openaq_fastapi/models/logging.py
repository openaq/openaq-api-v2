from enum import Enum
from typing import Union

from pydantic import ConfigDict, BaseModel, Field, FieldValidationInfo, field_validator
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
    detail: Union[str, None] = None

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


class InfrastructureErrorLog(BaseLog):
    type: LogType = LogType.INFRASTRUCTURE_ERROR


class HTTPLog(BaseLog):
    http_code: int
    request: Request = Field(..., exclude=True)
    path: Union[str, None] = None
    params: Union[str, None] = None
    params_obj: Union[dict, None] = None
    params_keys: Union[list, None] = None
    ip: Union[str, None] = None
    api_key: Union[str, None] = None
    timing: Union[float, None] = None
    rate_limiter: Union[str, None] = None
    counter: Union[int, None] = None

    @field_validator("api_key")
    def set_api_key(cls, v, info: FieldValidationInfo) -> dict:
        request = info.data["request"]
        api_key = request.headers.get("X-API-Key", None)
        return v or api_key

    @field_validator("ip")
    def set_ip(cls, v, info: FieldValidationInfo) -> dict:
        request = info.data["request"]
        ip = request.client.host
        return v or ip

    @field_validator("path")
    def set_path(cls, v, info: FieldValidationInfo) -> dict:
        request = info.data["request"]
        path = request.url.path
        return v or path

    @field_validator("params")
    def set_params(cls, v, info: FieldValidationInfo) -> dict:
        request = info.data["request"]
        params = request.url.query
        return v or params

    @field_validator("params_obj")
    def set_params_obj(cls, v, info: FieldValidationInfo) -> dict:
        params = info.data.get("params", "")
        if "=" in params:
            return v or dict(x.split("=", 1) for x in params.split("&") if "=" in x)
        else:
            return None

    @field_validator("params_keys")
    def set_params_keys(cls, v, info: FieldValidationInfo) -> dict:
        params = info.data.get("params_obj", {})
        return [] if params is None else list(params.keys())


class ErrorLog(HTTPLog):
    http_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR


class UnprocessableEntityLog(HTTPLog):
    http_code: int = status.HTTP_422_UNPROCESSABLE_ENTITY
    type: LogType = LogType.UNPROCESSABLE_ENTITY


class TooManyRequestsLog(HTTPLog):
    http_code: int = status.HTTP_429_TOO_MANY_REQUESTS
    type: LogType = LogType.TOO_MANY_REQUESTS


class UnauthorizedLog(HTTPLog):
    http_code: int = status.HTTP_401_UNAUTHORIZED
    type: LogType = LogType.UNAUTHORIZED


class ModelValidationError(ErrorLog):
    type: LogType = LogType.VALIDATION_ERROR
