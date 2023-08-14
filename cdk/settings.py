from typing import List, Union
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from os import environ


class Settings(BaseSettings):
    CDK_ACCOUNT: str
    CDK_REGION: str
    VPC_ID: Union[str, None] = None
    ENV: str = "staging"
    PROJECT: str = "openaq"
    API_CACHE_TIMEOUT: int = 900
    ROLLUP_LAMBDA_TIMEOUT: int = 900
    ROLLUP_LAMBDA_MEMORY_SIZE: int = 1536
    LOG_LEVEL: str = "INFO"
    HOSTED_ZONE_ID: Union[str, None] = None
    HOSTED_ZONE_NAME: Union[str, None] = None
    WEB_ACL_ID: Union[str, None] = None
    DOMAIN_NAME: Union[str, None] = None
    LOG_BUCKET: Union[str, None] = None
    CERTIFICATE_ARN: Union[str, None] = None
    API_LAMBDA_MEMORY_SIZE: int = 1536
    API_LAMBDA_TIMEOUT: int = 15  # lambda timeout in seconds
    CF_LOGS_LAMBDA_MEMORY_SIZE: int = 1792
    CF_LOG_LAMBDA_TIMEOUT: int = 15 * 60  # lambda timeout in seconds

    model_config = SettingsConfigDict(
        extra="ignore", env_file=f"../{environ['DOTENV'] or '.env'}", env_file_encoding="utf-8"
    )


settings = Settings()
