from pydantic import BaseSettings
from pathlib import Path
from os import environ


class Settings(BaseSettings):
    DATABASE_URL: str
    DATABASE_WRITE_URL: str
    OPENAQ_ENV: str = "staging"
    OPENAQ_FASTAPI_URL: str
    DRYRUN: bool = False
    OPENAQ_FETCH_BUCKET: str
    OPENAQ_ETL_BUCKET: str
    OPENAQ_CACHE_TIMEOUT: int = 900
    FETCH_ASCENDING: bool = False
    INGEST_TIMEOUT: int = 900
    PIPELINE_LIMIT: int = 10
    METADATA_LIMIT: int = 10
    VERSIONS_LIMIT: int = 10
    REALTIME_LIMIT: int = 10
    LOG_LEVEL: str = 'INFO'
    LOCAL_SAVE_DIRECTORY: str = './openaq_files'

    class Config:
        parent = Path(__file__).resolve().parent.parent.parent
        if 'DOTENV' in environ:
            env_file = Path.joinpath(parent, environ['DOTENV'])
        elif 'ENV' in environ:
            env_file = Path.joinpath(parent, f".env.{environ['ENV']}")
        else:
            env_file = Path.joinpath(parent, ".env")


settings = Settings()
