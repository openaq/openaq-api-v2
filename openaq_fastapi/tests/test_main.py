from fastapi.testclient import TestClient
import json
from openaq_fastapi.main import app

client = TestClient(app)


def test_ping():
    response = client.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "pong!"}


def test_locations_path_bad():
    response = client.get("/v3/locations/0")
    assert response.status_code == 422


def test_locations_path_good():
    response = client.get("/v3/locations/1")
    res = json.loads(response.content)
    assert response.status_code == 200
    assert len(res['results']) == 1
    assert res['results'][0]['id'] == 1
