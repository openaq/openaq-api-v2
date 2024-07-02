from fastapi.testclient import TestClient
import json
import time
import pytest
from openaq_api.main import app


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


sensors_id = 7223

class TestMeasurements:
    def test_measurements_raw_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/measurements")
        assert response.status_code == 200

    def test_measurements_raw_aggregated_hourly_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/measurements/hourly")
        assert response.status_code == 200

    def test_measurements_raw_aggregated_daily_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/measurements/daily")
        assert response.status_code == 200

    def test_measurements_hours_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours")
        assert response.status_code == 200

    def test_measurements_hours_aggregated_daily_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/daily?date_to=2024-02-01&date_from=2024-01-01")
        assert response.status_code == 200

    def test_measurements_hours_aggregated_yearly_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/yearly?date_to=2020-01-01&date_from=2024-01-01")
        assert response.status_code == 200

    def test_measurements_days_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/days")
        assert response.status_code == 200

    def test_measurements_days_aggregated_yearly_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/days/yearly")
        assert response.status_code == 200

   # def test_measurements_years_good(self, client):
   #     response = client.get(f"/v3/sensors/{sensors_id}/years")
   #     assert response.status_code == 200
