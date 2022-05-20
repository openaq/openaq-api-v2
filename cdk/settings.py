from pydantic import BaseSettings
from pathlib import Path
from os import environ


class Settings(BaseSettings):
    # DATABASE_URL: str
    # DATABASE_WRITE_URL: str
    ENV: str = "staging"
    # FASTAPI_URL: str
    # DRYRUN: bool = False
    FETCH_BUCKET: str
    ETL_BUCKET: str
    API_CACHE_TIMEOUT: int = 900
    FETCH_ASCENDING: bool = False
    INGEST_LAMBDA_TIMEOUT: int = 900
    INGEST_LAMBDA_MEMORY_SIZE: int = 1512
    PIPELINE_LIMIT: int = 10
    METADATA_LIMIT: int = 10
    REALTIME_LIMIT: int = 10
    LOG_LEVEL: str = 'INFO'
    LOCAL_SAVE_DIRECTORY: str = './openaq_files'
    HOSTED_ZONE_ID: str = None
    HOSTED_ZONE_NAME: str = None
    WEB_ACL_ID: str = None
    DOMAIN_NAME: str = None
    LOG_BUCKET: str = None
    API_LAMBDA_TIMEOUT: int = 30  # lambda timeout in seconds
    CERTIFICATE_ARN: str = None
    API_LAMBDA_MEMORY_SIZE: int = 1512

    class Config:
        parent = Path(__file__).resolve().parent.parent
        if 'DOTENV' in environ:
            env_file = Path.joinpath(parent, environ['DOTENV'])
        else:
            env_file = Path.joinpath(parent, ".env")


settings = Settings()
