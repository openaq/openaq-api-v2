from enum import Enum

from pydantic import BaseModel
from fastapi import status

from humps import camelize


class LogType(Enum):
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
    detail: str
    
    class Config:
        alias_generator = camelize


class WarnLog(BaseLog):
    type = LogType.WARNING


class HTTPLog(BaseModel):
    path: str
    params: str
    http_code: int


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
