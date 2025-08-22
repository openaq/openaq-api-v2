from urllib.parse import urlencode, urlunparse

from fastapi.testclient import TestClient
from openaq_api.main import app

import pytest

# assumes local DB deployment
not_found_endpoints = [
    "/v3/countries/9999",
    "/v3/instruments/9999",
    "/v3/manufacturers/9999/instruments",
    "/v3/sensors/9999/latest",
    "/v3/parameters/9999/latest",
    "/v3/licenses/9999",
    "/v3/locations/9999",
    "/v3/manufacturers/9999",
    "/v3/owners/9999",
    "/v3/parameters/9999",
    "/v3/providers/9999",
    "/v3/sensors/9999",
    "/v3/sensors/9999/measurements",
    "/v3/sensors/9999/hours",
    "/v3/sensors/9999/days",
    "/v3/sensors/9999/years",
    "/v3/sensors/9999/measurements/hourly",
    "/v3/sensors/9999/measurements/daily",
    "/v3/sensors/9999/measurements/yearly",
    "/v3/sensors/9999/hours/daily",
    "/v3/sensors/9999/hours/monthly",
    "/v3/sensors/9999/hours/yearly",
    "/v3/sensors/9999/days/monthly",
    "/v3/sensors/9999/days/yearly",
    "/v3/sensors/9999/hours/hourofday",
    "/v3/sensors/9999/hours/dayofweek",
    "/v3/sensors/9999/hours/monthofyear",
    "/v3/sensors/9999/days/dayofweek",
    "/v3/sensors/9999/days/monthofyear",
]


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


@pytest.mark.parametrize("endpoint", not_found_endpoints)
def test_endpoints_for_not_found(endpoint, client):
    response = client.get(endpoint)
    assert response.status_code == 404
