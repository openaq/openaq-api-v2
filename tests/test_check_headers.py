from fastapi.testclient import TestClient
from openaq_api.main import app


def test_request_id_is_added():
    client = TestClient(app)
    headers_with_key = {"X-API-Key": "test-key"}

    # Use safe and lightweight root endpoint
    endpoint = "/"

    # Case 1: Custom request ID from client
    custom_id = "test-id-123"
    resp = client.get(endpoint, headers={**headers_with_key, "X-Request-ID": custom_id})
    assert resp.status_code == 200
    assert resp.headers["X-Request-ID"] == custom_id

    # Case 2: Auto-generated request ID from server
    resp2 = client.get(endpoint, headers=headers_with_key)
    assert resp2.status_code == 200
    assert "X-Request-ID" in resp2.headers
    assert len(resp2.headers["X-Request-ID"]) >= 10


def test_api_version_header_present():
    client = TestClient(app)
    headers = {"X-API-Key": "test-key"}

    # Use root endpoint to verify fallback logic
    response = client.get("/", headers=headers)

    assert response.status_code == 200
    assert "X-API-Version" in response.headers
    assert response.headers["X-API-Version"] == "v2"  # fallback/default version

def test_response_time_header():
    client = TestClient(app)
    headers = {"X-API-Key": "test-key"}
    response = client.get("/", headers=headers)

    assert response.status_code == 200
    assert "X-Response-Time" in response.headers
    assert response.headers["X-Response-Time"].endswith("ms")

def test_timing_metrics_available():
    client = TestClient(app)
    headers = {"X-API-Key": "test-key"}

    # Hit a few endpoints to generate timing data
    for _ in range(3):
        client.get("/", headers=headers)

    # Check metrics
    metrics = client.get("/metrics/timing", headers=headers)
    assert metrics.status_code == 200
    body = metrics.json()

    # Flexible assertion: check that at least one key is for "/"
    matching_keys = [k for k in body.keys() if k.endswith("/")]
    assert matching_keys, "Expected a key ending in '/', got: {}".format(body.keys())

    for k in matching_keys:
        assert "count" in body[k]
        assert "avg_ms" in body[k]



