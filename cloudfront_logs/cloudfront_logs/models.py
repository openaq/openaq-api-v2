from datetime import datetime, date
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
    date: date | None
    time: str | None
    location: str | None
    bytes: int | None
    request_ip: str | None
    method: str | None
    host: str | None
    uri: str | None
    status: int | None
    referrer: str | None
    user_agent: str | None
    query_string: str | None
    cookie: str | None
    result_type: str | None
    request_id: str | None
    host_header: str | None
    request_protocol: str | None
    request_bytes: int | None
    time_taken: float | None
    xforwarded_for: str | None
    ssl_protocol: str | None
    ssl_cipher: str | None
    response_result_type: str | None
    http_version: str | None
    fle_status: str | None
    fle_encrypted_fields: int | None
    c_port: int | None
    time_to_first_byte: float | None
    x_edge_detailed_result_type: str | None
    sc_content_type: str | None
    sc_content_len: int | None
    sc_range_start: int | None
    sc_range_end: int | None

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
