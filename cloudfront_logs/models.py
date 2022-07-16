    
from datetime import datetime

from humps import camelize
from pydantic import BaseModel


class CloudwatchLog(BaseModel):
    timestamp: int
    message: str


class CloudfrontLog(BaseModel):
    data: datetime
    time : str
    location : str
    bytes: int
    request_ip : str
    method : str
    host : str
    uri : str
    status: int
    referrer : str
    user_agent : str
    query_string : str
    cookie : str
    result_type : str
    request_id : str
    host_header : str
    request_protocol : str
    request_bytes: int
    time_taken: float
    xforwarded_for : str
    ssl_protocol : str
    ssl_cipher : str
    response_result_type : str
    http_version : str
    fle_status : str
    fle_encrypted_fields: int
    c_port: int
    time_to_first_byte: float
    x_edge_detailed_result_type : str
    sc_content_type : str
    sc_content_len : int
    sc_range_start : int
    sc_range_end : int


    class Config:
        alias_generator = camelize
        arbitrary_types_allowed = True
        allow_population_by_field_name = True