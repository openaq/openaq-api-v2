from typing import Union
from pydantic import ConfigDict, computed_field, field_validator
from pathlib import Path
from os import environ
from pydantic_settings import BaseSettings, SettingsConfigDict


def get_env():
    parent = Path(__file__).resolve().parent.parent.parent
    if "DOTENV" in environ:
        env_file = Path.joinpath(parent, environ["DOTENV"])
    else:
        env_file = Path.joinpath(parent, ".env")
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
    LOG_BUCKET: str = None
    DOMAIN_NAME: str = None

    REDIS_HOST: Union[str, None] = None
    REDIS_PORT: Union[int, None] = 6379

    RATE_LIMITING: bool = False
    RATE_AMOUNT: Union[int, None] = None
    RATE_AMOUNT_KEY: Union[int, None] = None
    RATE_TIME: Union[int, None] = None

    EMAIL_SENDER: str

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
