from os import environ
from pathlib import Path
from typing import List


from pydantic_settings import BaseSettings, SettingsConfigDict


def get_env():
    parent = Path(__file__).resolve().parent.parent
    env_file = Path.joinpath(parent, environ.get("DOTENV", ".env"))
    return env_file


class Settings(BaseSettings):
    CDK_ACCOUNT: str
    CDK_REGION: str
    VPC_ID: str | None = None
    ENV: str = "staging"
    PROJECT: str = "openaq"
    API_CACHE_TIMEOUT: int = 900
    LOG_LEVEL: str = "INFO"
    REDIS_PORT: int | None = 6379
    REDIS_SECURITY_GROUP_ID: str | None = None
    API_LAMBDA_MEMORY_SIZE: int = 1536
    API_LAMBDA_TIMEOUT: int = 15  # lambda timeout in seconds
    HOSTED_ZONE_ID: str | None = None
    HOSTED_ZONE_NAME: str | None = None
    DOMAIN_NAME: str | None = None
    CERTIFICATE_ARN: str | None = None
    CF_LOGS_LAMBDA_MEMORY_SIZE: int = 1792
    CF_LOG_LAMBDA_TIMEOUT: int = 180  # lambda timeout in seconds
    WAF_RATE_LIMIT_EVALUATION_WINDOW: int = 60  # in seconds
    WAF_RATE_LIMIT: int | None = None
    WAF_BLOCK_IPS: List[str] | None = None

    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=get_env(),
        env_file_encoding="utf-8",
    )


settings = Settings()
