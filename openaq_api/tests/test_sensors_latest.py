from fastapi.testclient import TestClient
import json
import time
import pytest
from openaq_api.main import app


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


measurands_id = 2
# location 1 is at -10 hrs
# last value is on 2024-08-27 19:30
locations_id = 1

class TestLocations:
    def test_default_good(self, client):
        response = client.get(f"/v3/locations/{locations_id}/latest")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 1

    def test_date_filter(self, client):
        response = client.get(f"/v3/locations/{locations_id}/latest?datetime_min=2024-08-27")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 1

    def test_timestamp_filter(self, client):
        response = client.get(f"/v3/locations/{locations_id}/latest?datetime_min=2024-08-27 19:00:00")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 1

    def test_timestamptz_filter(self, client):
        response = client.get(f"/v3/locations/{locations_id}/latest?datetime_min=2024-08-27 19:00:00-10:00")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 1


class TestMeasurands:
    def test_default_good(self, client):
        response = client.get(f"/v3/parameters/{measurands_id}/latest")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 6

    def test_date_filter(self, client):
        response = client.get(f"/v3/parameters/{measurands_id}/latest?datetime_min=2024-08-27")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 10

    def test_timestamp_filter(self, client):
        response = client.get(f"/v3/parameters/{measurands_id}/latest?datetime_min=2024-08-27 19:00:00")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 1

    def test_timestamptz_filter(self, client):
        response = client.get(f"/v3/parameters/{measurands_id}/latest?datetime_min=2024-08-27 19:00:00-10:00")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 1
