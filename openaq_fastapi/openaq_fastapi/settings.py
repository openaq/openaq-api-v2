from pydantic import BaseSettings
from pathlib import Path

parent = Path(__file__).resolve().parent.parent.parent
env_file = Path.joinpath(parent, ".env")


class Settings(BaseSettings):
    DATABASE_URL: str
    DATABASE_WRITE_URL: str
    OPENAQ_ENV: str = "staging"
    OPENAQ_FASTAPI_URL: str
    TESTLOCAL: bool = True
    OPENAQ_FETCH_BUCKET: str
    OPENAQ_ETL_BUCKET: str
    OPENAQ_CACHE_TIMEOUT: int = 900

    class Config:
        env_file = env_file


settings = Settings()
