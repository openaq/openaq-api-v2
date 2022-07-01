from enum import Enum
from typing import Union

from pydantic import BaseModel, Field, validator
from fastapi import status

from humps import camelize


class LogType(Enum):
    SUCCESS = "SUCCESS"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    INFRASTRUCTURE_ERROR = "INFRASTRUCTURE_ERROR"
    UNPROCESSABLE_ENTITY = "UNPROCESSABLE_ENTITY"
    UNAUTHORIZED = "UNAUTHORIZED"
    TOO_MANY_REQUESTS = "TOO_MANY_REQUESTS"
    WARNING = "WARNING"


class BaseLog(BaseModel):
    """
    abstract base class for logging
    """
    type: LogType
    detail: Union[str, None]

    class Config:
        alias_generator = camelize
        allow_population_by_field_name = True

class WarnLog(BaseLog):
    type = LogType.WARNING


class HTTPLog(BaseLog):
    path: str
    params: str
    params_obj: Union[dict, None]
    http_code: int

    @validator('params_obj', always=True)
    def set_params_obj(cls, v, values) -> dict:
        if "=" in values.get("params", ""):
            return v or dict(x.split("=") for x in values["params"].split("&"))
        else:
            return None


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
