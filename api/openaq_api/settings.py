from os import environ, getcwd, path

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


def get_env():
    env_name = environ.get('DOTENV', '.env')
    if not env_name.startswith(".env"):
        env_name = f".env.{env_name}"
    if path.basename(getcwd()) == 'openaq_api':
        env_name = f"../../{env_name}"
    elif path.basename(getcwd()) == 'cdk':
        env_name = f"../{env_name}"
    return env_name



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
    RATE_AMOUNT_KEY: int | None = None
    USER_AGENT: str | None = None
    ORIGIN: str | None = None

    EMAIL_SENDER: str | None = None
    SMTP_EMAIL_HOST: str | None = None
    SMTP_EMAIL_USER: str | None = None
    SMTP_EMAIL_PASSWORD: str | None = None

    EXPLORER_API_KEY: str

    @computed_field(return_type=str, alias="DATABASE_READ_URL")
    @property
    def DATABASE_READ_URL(self):
        return f"postgresql://{self.DATABASE_READ_USER}:{self.DATABASE_READ_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_DB}"

    @computed_field(return_type=str, alias="DATABASE_WRITE_URL")
    @property
    def DATABASE_WRITE_URL(self):
        return f"postgresql://{self.DATABASE_WRITE_USER}:{self.DATABASE_WRITE_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_DB}"

    @computed_field(return_type=str, alias="DATABASE_WRITE_URL")
    @property
    def USE_SMTP_EMAIL(self):
        return None not in [
            self.SMTP_EMAIL_HOST,
            self.SMTP_EMAIL_USER,
            self.SMTP_EMAIL_PASSWORD,
        ]

    model_config = SettingsConfigDict(extra="ignore", env_file=get_env())


settings = Settings()
