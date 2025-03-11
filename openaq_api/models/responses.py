import logging
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger("responses")


class Meta(BaseModel):
    name: str = "openaq-api"
    license: str = ""
    website: str = "/"
    page: int = 1
    limit: int = 100
    found: int | str | None = None


# Abstract class for all responses
class OpenAQResult(BaseModel):
    meta: Meta = Meta()
    results: list[Any] = []
