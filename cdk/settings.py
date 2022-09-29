from typing import List
from pydantic import BaseSettings
from pathlib import Path
from os import environ


class Settings(BaseSettings):
    CDK_ACCOUNT: str
    CDK_REGION: str
    VPC_ID: str = None
    ENV: str = "staging"
    PROJECT: str = "openaq"
    API_CACHE_TIMEOUT: int = 900
    ROLLUP_LAMBDA_TIMEOUT: int = 900
    ROLLUP_LAMBDA_MEMORY_SIZE: int = 1536
    LOG_LEVEL: str = 'INFO'
    HOSTED_ZONE_ID: str = None
    HOSTED_ZONE_NAME: str = None
    WEB_ACL_ID: str = None
    DOMAIN_NAME: str = None
    LOG_BUCKET: str = None
    CERTIFICATE_ARN: str = None
    TOPIC_ARN: str = None
    API_LAMBDA_MEMORY_SIZE: int = 1536
    API_LAMBDA_TIMEOUT: int = 30  # lambda timeout in seconds
    CF_LOGS_LAMBDA_MEMORY_SIZE: int = 1792
    CF_LOG_LAMBDA_TIMEOUT: int = 180  # lambda timeout in seconds

    class Config:
        parent = Path(__file__).resolve().parent.parent
        if 'DOTENV' in environ:
            env_file = Path.joinpath(parent, environ['DOTENV'])
        else:
            env_file = Path.joinpath(parent, ".env")


settings = Settings()
