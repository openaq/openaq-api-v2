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
    assert (
        len(res["results"]) == 1
    ), f"should have 1 results, found {len(res['results'])}"
    assert res["results"][0]["id"] == 1


def test_locations_query_bad():
    response = client.get(
        "/v3/locations?coordinates=42,42&radius=1000&bbox=42,42,42,42"
    )
    assert response.status_code == 422


def test_providers_path_bad():
    response = client.get("/v3/providers/0")
    assert response.status_code == 422


def test_providers_path_good():
    response = client.get("/v3/providers/1")
    res = json.loads(response.content)
    assert response.status_code == 200
    assert len(res["results"]) == 1
    assert res["results"][0]["id"] == 1
