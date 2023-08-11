from pydantic_settings import BaseSettings, SettingsConfigDict
from os import environ


class Settings(BaseSettings):
    ENV: str = "staging"
    CF_LOGS_LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        extra="ignore", env_file="../.env", env_file_encoding="utf-8"
    )


settings = Settings()
