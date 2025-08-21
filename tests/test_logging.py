from fastapi.testclient import TestClient
import pytest
from starlette.middleware.base import BaseHTTPMiddleware

from openaq_api.main import app
from openaq_api.models.logging import HTTPLog, LogType

captured_requests = []


class CaptureMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        captured_requests.append(request)
        return await call_next(request)


app.add_middleware(CaptureMiddleware)


@pytest.fixture
def client():
    with TestClient(app) as c:
        c.captured_requests = captured_requests
        yield c


paths = [
    ("/v3/instruments/3", {"instruments_id": "3"}),
    ("/v3/locations/42", {"locations_id": "42"}),
    ("/v3/locations/42?locations_id=24", {"locations_id": "42"}),
    ("/v3/sensors/42/measurements", {"sensors_id": "42"}),
    (
        "/v3/sensors/42/measurements?limit=100&page=2",
        {"sensors_id": "42", "limit": "100", "page": "2"},
    ),
    ("/v3/sensors/54", {"sensors_id": "54"}),
]


@pytest.mark.parametrize("path,params_obj", paths)
class TestHTTPLog:
    def test_params_obj_property(self, client, path, params_obj):
        client.captured_requests.clear()
        response = client.get(
            path,
        )
        request = client.captured_requests[0]
        log = HTTPLog(
            type=LogType.SUCCESS, request=request, http_code=response.status_code
        )
        assert log.params_obj == params_obj
