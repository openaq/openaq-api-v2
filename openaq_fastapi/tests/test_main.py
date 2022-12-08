from fastapi.testclient import TestClient
import json
import time
import pytest
from openaq_fastapi.main import app
from openaq_fastapi.db import db_pool

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def test_ping(client):
    response = client.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "pong!"}


def test_locations_path_bad(client):
    response = client.get("/v3/locations/0")
    assert response.status_code == 422


def test_locations_path_good(client):
    response = client.get("/v3/locations/1")
    res = json.loads(response.content)
    print(res)
    assert response.status_code == 200
    assert len(res['results']) == 1
    assert res['results'][0]['id'] == 1


def test_locations_radius_good(client):
    response = client.get("/v3/locations?coordinates=38.907,-77.037&radius=1000")
    assert response.status_code == 200


def test_locations_bbox_good(client):
    response = client.get("/v3/locations?bbox=-77.037,38.907,-77.0,39.910")
    assert response.status_code == 200
