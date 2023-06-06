from typing import Union
from pydantic import BaseSettings, validator
from pathlib import Path
from os import environ


class Settings(BaseSettings):
    DATABASE_READ_USER: str
    DATABASE_WRITE_USER: str
    DATABASE_READ_PASSWORD: str
    DATABASE_WRITE_PASSWORD: str
    DATABASE_DB: str
    DATABASE_HOST: str
    DATABASE_PORT: int
    DATABASE_READ_URL: Union[str, None]
    DATABASE_WRITE_URL: Union[str, None]
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

    @validator("DATABASE_READ_URL", allow_reuse=True)
    def get_read_url(cls, v, values):
        return (
            v
            or f"postgresql://{values['DATABASE_READ_USER']}:{values['DATABASE_READ_PASSWORD']}@{values['DATABASE_HOST']}:{values['DATABASE_PORT']}/{values['DATABASE_DB']}"
        )

    @validator("DATABASE_WRITE_URL", allow_reuse=True)
    def get_write_url(cls, v, values):
        return (
            v
            or f"postgresql://{values['DATABASE_WRITE_USER']}:{values['DATABASE_WRITE_PASSWORD']}@{values['DATABASE_HOST']}:{values['DATABASE_PORT']}/{values['DATABASE_DB']}"
        )

    class Config:
        parent = Path(__file__).resolve().parent.parent.parent
        if "DOTENV" in environ:
            env_file = Path.joinpath(parent, environ["DOTENV"])
        else:
            env_file = Path.joinpath(parent, ".env")


settings = Settings()
