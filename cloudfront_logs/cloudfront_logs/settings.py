from pydantic_settings import BaseSettings, SettingsConfigDict
from os import environ


def get_env():
    parent = Path(__file__).resolve().parent.parent
    env_file = Path.joinpath(parent, environ.get("DOTENV", ".env"))
    return env_file


class Settings(BaseSettings):
    ENV: str = "staging"
    CF_LOGS_LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=get_env(),
        env_file_encoding="utf-8",
    )


settings = Settings()
