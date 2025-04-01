from fastapi.testclient import TestClient
import json
import time
import os
import pytest
from main import app
from db import db_pool


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def test_ping(client):
    response = client.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "pong!"}


endpoints = [
    "countries",
    "owners",
    "manufacturers",
    "providers",
    "sensors",
    "locations",
    "parameters",
]


# @pytest.mark.parametrize("endpoint", endpoints)
# class TestEndpointsHealth:
#     def test_endpoint_list_good(self, client, endpoint):
#         response = client.get(f"/v3/{endpoint}")
#         assert response.status_code == 200

#     def test_endpoint_path_good(self, client, endpoint):
#         response = client.get(f"/v3/{endpoint}/1")
#         assert response.status_code == 200

#     def test_endpoint_path_bad(self, client, endpoint):
#         response = client.get(f"/v3/{endpoint}/0")
#         assert response.status_code == 422


dir_path = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(dir_path, "url_list.txt")) as file:
    urls = [line.rstrip() for line in file]


@pytest.mark.parametrize("url", urls)
class TestUrls:
    def test_urls(self, client, url):
        response = client.get(url)
        assert response.status_code == 200


class TestLocations:
    def test_locations_radius_good(self, client):
        response = client.get("/v3/locations?coordinates=38.907,-77.037&radius=1000")
        assert response.status_code == 200

    def test_locations_bbox_good(self, client):
        response = client.get("/v3/locations?bbox=-77.037,38.907,-77.0,39.910")
        assert response.status_code == 200

    def test_locations_query_bad(self, client):
        response = client.get(
            "/v3/locations?coordinates=42,42&radius=1000&bbox=42,42,42,42"
        )
        assert response.status_code == 422

    def test_locations_providers_id_param(self, client):
        response = client.get("/v3/locations?providers_id=1")
        res = json.loads(response.content)
        assert all(result["provider"]["id"] == 1 for result in res["results"])

    def test_locations_is_monitor_param(self, client):
        response = client.get("/v3/locations?monitor=true")
        res = json.loads(response.content)
        assert all(result["isMonitor"] for result in res["results"])
        response = client.get("/v3/locations?monitor=false")
        res = json.loads(response.content)
        assert all(result["isMonitor"] == False for result in res["results"])

    def test_locations_countries_id_param(self, client):
        response = client.get("/v3/locations?countries_id=1")
        res = json.loads(response.content)
        assert all(result["country"]["id"] == 1 for result in res["results"])
