import json
import pytest
from fastapi.testclient import TestClient
from contextlib import asynccontextmanager
import openaq_api.dependencies  # must import before patching logger

logs = []

@pytest.fixture(autouse=True)
def patch_logger(monkeypatch):
    monkeypatch.setattr(
        openaq_api.dependencies.logger,
        "error",
        lambda msg, *a, **kw: logs.append(msg.decode() if isinstance(msg, bytes) else str(msg))
    )

class FailingRedis:
    async def sismember(self, key_set, api_key): return 1
    async def hget(self, api_key, field): return "5"
    async def get(self, key): return None
    async def ttl(self, key): return 60
    def pipeline(self):
        @asynccontextmanager
        async def broken_pipeline():
            class Pipe:
                async def incr(self, key): raise RuntimeError("redis error during pipeline")
                def expire(self, key, ttl): return self
                async def execute(self): return [1, True]
            yield Pipe()
        return broken_pipeline()

@pytest.fixture
def test_client_with_failing_redis():
    from openaq_api.main import app
    app.state.redis = FailingRedis()
    return TestClient(app)

def test_redis_error_logging(test_client_with_failing_redis):
    logs.clear()

    response = test_client_with_failing_redis.get("/_test-redis-error", headers={"X-API-Key": "test-key"})
    assert response.status_code == 200

    print("Captured logs:", logs)

    for log in logs:
        try:
            parsed = json.loads(log)
            if "redis error" in parsed.get("detail", "").lower():
                return  # ✅ success
        except Exception as e:
            print("⚠️ Could not parse:", log, e)

    pytest.fail("Redis error was not logged")
