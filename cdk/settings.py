from typing import List
from pydantic import BaseSettings, validator
from pathlib import Path
from os import environ


class Settings(BaseSettings):
    CDK_ACCOUNT: str
    CDK_REGION: str
    ENV: str = "staging"
    PROJECT: str = "openaq"
    FETCH_BUCKET: str
    ETL_BUCKET: str
    API_CACHE_TIMEOUT: int = 900
    FETCH_ASCENDING: bool = False
    ROLLUP_LAMBDA_TIMEOUT: int = 900
    ROLLUP_LAMBDA_MEMORY_SIZE: int = 1512
    INGEST_LAMBDA_TIMEOUT: int = 900
    INGEST_LAMBDA_MEMORY_SIZE: int = 1512
    PIPELINE_LIMIT: int = 10
    METADATA_LIMIT: int = 10
    REALTIME_LIMIT: int = 10
    LOG_LEVEL: str = 'INFO'
    LOCAL_SAVE_DIRECTORY: str = './openaq_files'
    VPC_ID: str
    HOSTED_ZONE_ID: str = None
    HOSTED_ZONE_NAME: str = None
    WEB_ACL_ID: str = None
    DOMAIN_NAME: str = None
    LOG_BUCKET: str = None
    CERTIFICATE_ARN: str = None
    TOPIC_ARN: str = None
    API_LAMBDA_MEMORY_SIZE: int = 1512
    API_LAMBDA_TIMEOUT: int = 30  # lambda timeout in seconds

    class Config:
        parent = Path(__file__).resolve().parent.parent
        if 'DOTENV' in environ:
            env_file = Path.joinpath(parent, environ['DOTENV'])
        else:
            env_file = Path.joinpath(parent, ".env")


settings = Settings()
