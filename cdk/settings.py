from os import environ
from pathlib import Path


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
    ROLLUP_LAMBDA_TIMEOUT: int = 900
    ROLLUP_LAMBDA_MEMORY_SIZE: int = 1536
    LOG_LEVEL: str = "INFO"
    HOSTED_ZONE_ID: str | None = None
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
        extra="ignore",
        env_file=get_env(),
        env_file_encoding="utf-8",
    )


settings = Settings()
