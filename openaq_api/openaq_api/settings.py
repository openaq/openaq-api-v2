from os import environ
from pathlib import Path

from pydantic import ConfigDict, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def get_env():
    parent = Path(__file__).resolve().parent.parent.parent
    env_file = Path.joinpath(parent, environ.get("DOTENV", ".env"))
    return env_file


class Settings(BaseSettings):
    DATABASE_READ_USER: str
    DATABASE_WRITE_USER: str
    DATABASE_READ_PASSWORD: str
    DATABASE_WRITE_PASSWORD: str
    DATABASE_DB: str
    DATABASE_HOST: str
    DATABASE_PORT: int
    API_CACHE_TIMEOUT: int = 900
    USE_SHARED_POOL: bool = False
    LOG_LEVEL: str = "INFO"
    LOG_BUCKET: str | None = None
    DOMAIN_NAME: str | None = None

    REDIS_HOST: str | None = None
    REDIS_PORT: int | None = 6379

    RATE_LIMITING: bool = False
    RATE_AMOUNT: int | None = None
    RATE_AMOUNT_KEY: int | None = None
    RATE_TIME: int | None = None
    USER_AGENT: str | None = None
    ORIGIN: str | None = None

    EMAIL_SENDER: str | None = None

    @computed_field(return_type=str, alias="DATABASE_READ_URL")
    @property
    def DATABASE_READ_URL(self):
        return f"postgresql://{self.DATABASE_READ_USER}:{self.DATABASE_READ_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_DB}"

    @computed_field(return_type=str, alias="DATABASE_WRITE_URL")
    @property
    def DATABASE_WRITE_URL(self):
        return f"postgresql://{self.DATABASE_WRITE_USER}:{self.DATABASE_WRITE_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_DB}"

    model_config = SettingsConfigDict(extra="ignore", env_file=get_env())


settings = Settings()
