from fastapi.testclient import TestClient
from openaq_api.main import app

client = TestClient(app)  # ✅ spins up a test instance of your FastAPI app

def test_fake_429_headers():
    headers = {"X-API-Key": "test-key"}  # ✅ sets a mock valid API key
    response = client.get("/fake429", headers=headers)  # ⬅️ hits the fake test endpoint you added

    assert response.status_code == 429  # ✅ verifies that the server responds with HTTP 429
    assert response.headers["X-RateLimit-Limit"] == "60"  # ✅ checks the max rate limit value
    assert response.headers["X-RateLimit-Remaining"] == "0"  # ✅ checks that 0 requests remain
    assert "X-RateLimit-Reset" in response.headers  # ✅ ensures a reset timer is present
