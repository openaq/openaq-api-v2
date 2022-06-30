from enum import Enum

from pydantic import BaseModel

from humps import camelize


class LogType(Enum):
    VALIDATION_ERROR = "VALIDATION_ERROR"
    INFRASTRUCTURE_ERROR = "INFRASTRUCTURE_ERROR"
    UNPROCESSABLE_ENTITY = "UNPROCESSABLE_ENTITY"
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
    http_code = 500


class UnprocessableEntityLog(HTTPLog):
    http_code = 422
    type = LogType.UNPROCESSABLE_ENTITY


class ModelValidationError(ErrorLog):
    type = LogType.VALIDATION_ERROR


