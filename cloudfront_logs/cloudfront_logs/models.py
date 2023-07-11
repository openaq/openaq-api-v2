from datetime import datetime, date
from typing import Union
from humps import camelize
from pydantic import BaseModel, validator


class CloudwatchLog(BaseModel):
    timestamp: int
    message: str


class HTTPStatusLog(BaseModel):
    status_code: int
    count: int = 1

    class Config:
        alias_generator = camelize
        allow_population_by_field_name = True

    def increment_count(self):
        self.count = self.count + 1


class CloudfrontLog(BaseModel):
    date: Union[date, None]
    time: Union[str, None]
    location: Union[str, None]
    bytes: Union[int, None]
    request_ip: Union[str, None]
    method: Union[str, None]
    host: Union[str, None]
    uri: Union[str, None]
    status: Union[int, None]
    referrer: Union[str, None]
    user_agent: Union[str, None]
    query_string: Union[str, None]
    cookie: Union[str, None]
    result_type: Union[str, None]
    request_id: Union[str, None]
    host_header: Union[str, None]
    request_protocol: Union[str, None]
    request_bytes: Union[int, None]
    time_taken: Union[float, None]
    xforwarded_for: Union[str, None]
    ssl_protocol: Union[str, None]
    ssl_cipher: Union[str, None]
    response_result_type: Union[str, None]
    http_version: Union[str, None]
    fle_status: Union[str, None]
    fle_encrypted_fields: Union[int, None]
    c_port: Union[int, None]
    time_to_first_byte: Union[float, None]
    x_edge_detailed_result_type: Union[str, None]
    sc_content_type: Union[str, None]
    sc_content_len: Union[int, None]
    sc_range_start: Union[int, None]
    sc_range_end: Union[int, None]

    @validator("date", pre=True)
    def parse_date(cls, v):
        return datetime.strptime(v, "%Y-%m-%d").date()

    @validator("*", pre=True)
    def check_null(cls, v):
        if v == "-":
            return None
        return v

    class Config:
        alias_generator = camelize
        arbitrary_types_allowed = True
        allow_population_by_field_name = True
