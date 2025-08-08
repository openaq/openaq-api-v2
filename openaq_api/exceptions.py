from typing import Any
from fastapi import (
    HTTPException,
    status,
)


class UnauthorizedException(HTTPException):
    def __init__(self, detail: Any = None) -> None:
        super().__init__(
            status.HTTP_401_UNAUTHORIZED,
            "Invalid credentials, the API key you supplied is invalid. Check that you are using a valid OpenAQ API key.",
        )


class RequestTimeoutException(HTTPException):
    def __init__(self, detail: Any = None) -> None:
        super().__init__(
            status.HTTP_408_REQUEST_TIMEOUT,
            "Connection timed out: Try to provide more specific query parameters or a smaller time frame.",
        )


class TooManyRequestsException(HTTPException):
    def __init__(
        self, detail: Any = None, headers: dict[str, Any] | None = None
    ) -> None:
        super().__init__(
            status.HTTP_429_TOO_MANY_REQUESTS, "Too many requests.", headers
        )


class NotFoundException(HTTPException):
    def __init__(
        self, resource: str, id: int, headers: dict[str, Any] | None = None
    ) -> None:
        detail = f"{resource} with ID {id} does not exist"
        super().__init__(status.HTTP_404_NOT_FOUND, detail, headers)
