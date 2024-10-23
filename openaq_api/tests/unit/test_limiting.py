from fastapi.testclient import TestClient
import json
import time
import os
import pytest
from openaq_api.main import app
from openaq_api.db import db_pool
import re



class FakePipeline:

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        ...

    def incr(self, key):
        return self

    def expire(self, key, sec):
        return self

    async def execute(self):
        return [1, None]


class FakeRedisClient:
    def __init__(self):
        self.api_keys = [
            "limited-api-key",
            "not-limited-api-key",
            "new-api-key",
            ];
        self.api_key_data = {
        "limited-api-key": {"get":"10","ttl":5},
        "not-limited-api-key": {"get":"9","ttl":30},
        "missing-api-key": {"get":None,"ttl":-1}
        }

    # is this key in the set
    async def sismember(self, scope, key):
        value =  1 if key in self.api_keys else 0
        print(f"redis sismember: {key} = {value}")
        return value

    # number of requests made on this key
    async def get(self, key):
        key = re.sub('[\d+:]', '', key)
        value = self.api_key_data.get(key, {}).get('get')
        print(f"redis get: {key} = {value}")
        return value

    # time to live
    # how many seconds are left for this key
    async def ttl(self, key):
        key = re.sub('[\d+:]', '', key)
        value = self.api_key_data.get(key, {}).get('ttl')
        print(f"redis ttl: {key} = {value}")
        return value

    # just a way to increment the number of requests
    def pipeline(self):
        return FakePipeline()


@pytest.fixture
def client():
    app.redis = FakeRedisClient()
    with TestClient(app) as c:
        yield c


def test_whitelisted_path_returns_200(client):
    response = client.get("/openapi.json")
    assert response.status_code == 200

def test_no_key_returns_401(client):
    response = client.get("/ping")
    assert response.status_code == 401

def test_empty_key_returns_401(client):
    response = client.get("/ping", headers={"X-API-Key":""})
    assert response.status_code == 401

def test_invalid_key_returns_401(client):
    response = client.get("/ping", headers={"X-API-Key":"invalid-key"})
    assert response.status_code == 401

def test_limited_key_returns_429(client):
    response = client.get("/ping", headers={"X-API-Key":"limited-api-key"})
    assert response.status_code == 429

def test_not_limited_key_returns_200(client):
    response = client.get("/ping", headers={"X-API-Key":"not-limited-api-key"})
    assert response.status_code == 200

def test_new_api_key_returns_200(client):
    response = client.get("/ping", headers={"X-API-Key":"not-limited-api-key"})
    assert response.status_code == 200
