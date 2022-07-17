from typing import Union
from pydantic import BaseSettings
from pathlib import Path
from os import environ


class Settings(BaseSettings):
    ENV: str = "staging"
    
    class Config:
        parent = Path(__file__).resolve().parent.parent.parent
        if 'DOTENV' in environ:
            env_file = Path.joinpath(parent, environ['DOTENV'])
        else:
            env_file = Path.joinpath(parent, ".env")


settings = Settings()