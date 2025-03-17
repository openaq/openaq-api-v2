from fastapi import (
    HTTPException,
    status,
)

NOT_AUTHENTICATED_EXCEPTION = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Invalid credentials",
)

TOO_MANY_REQUESTS = HTTPException(
    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
    detail="Too many requests",
)


def too_many_requests_with_headers(headers):
    return HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Too many requests",
        headers=headers,
    )
