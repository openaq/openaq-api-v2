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
        key = re.sub(r"[\d+:]", "", key)
        self.key = self.api_key_data.get(key, {})
        return self

    def expire(self, key, sec):
        return self

    async def execute(self):
        value = self.key.get('get');
        if value:
            # just incrd
            return [int(value) + 1]
        else:
            # incr and then expire
            return [1, None]


class FakeRedisClient:
    def __init__(self):
        self.api_keys = [
            "limited-api-key",
            "not-limited-api-key",
            "new-api-key",
        ]
        self.api_key_data = {
            "limited-api-key": {"get": "60", "ttl": 5, "rate": 60},
            "not-limited-api-key": {"get": "58", "ttl": 2, "rate": 60},
            "new-api-key": {"ttl": -2, "rate": None}, ## this key does not exist
        }

    # is this key in the set
    async def sismember(self, scope, key):
        value = 1 if key in self.api_keys else 0
        print(f"redis sismember: {key} = {value}")
        return value

    # number of requests made on this key
    async def get(self, key):
        key = re.sub(r"[\d+:]", "", key)
        value = self.api_key_data.get(key, {}).get("get")
        print(f"redis get: {key} = {value}")
        return value

    async def hget(self, key, field):
        key = re.sub(r"[\d+:]", "", key)
        value = self.api_key_data.get(key, {}).get(field)
        print(f"redis get: {key} = {value}")
        return value

    # time to live
    # how many seconds are left for this key
    async def ttl(self, key):
        key = re.sub(r"[\d+:]", "", key)
        value = self.api_key_data.get(key, {}).get("ttl")
        print(f"redis ttl: {key} = {value}")
        return value

    # just a way to increment the number of requests
    def pipeline(self):
        pipe = FakePipeline()
        pipe.api_key_data = self.api_key_data;
        return pipe


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
    response = client.get("/ping", headers={"X-API-Key": ""})
    assert response.status_code == 401


def test_invalid_key_returns_401(client):
    response = client.get("/ping", headers={"X-API-Key": "invalid-key"})
    assert response.status_code == 401


def test_limited_key_returns_429(client):
    response = client.get("/ping", headers={"X-API-Key": "limited-api-key"})
    assert response.status_code == 429


def test_not_limited_key_returns_valid_rate_headers(client):
    response = client.get("/ping", headers={"X-API-Key": "not-limited-api-key"})
    assert response.headers.get('x-ratelimit-limit') == '60'
    assert response.headers.get('x-ratelimit-used') == '59'
    assert response.headers.get('x-ratelimit-remaining') == '1'
    assert response.headers.get('x-ratelimit-reset') == '2'


def test_limited_key_returns_valid_rate_headers(client):
    response = client.get("/ping", headers={"X-API-Key": "limited-api-key"})
    assert response.headers.get('x-ratelimit-limit') == '60'
    assert response.headers.get('x-ratelimit-used') == '60'
    assert response.headers.get('x-ratelimit-remaining') == '0'
    assert response.headers.get('x-ratelimit-reset') == '5'

def test_new_key_returns_valid_rate_headers(client):
    response = client.get("/ping", headers={"X-API-Key": "new-api-key"})
    print(response.headers)
    assert response.headers.get('x-ratelimit-limit') == '60'
    assert response.headers.get('x-ratelimit-used') == '1'
    assert response.headers.get('x-ratelimit-remaining') == '59'
    assert response.headers.get('x-ratelimit-reset') == '-2'


def test_not_limited_key_returns_200(client):
    response = client.get("/ping", headers={"X-API-Key": "not-limited-api-key"})
    assert response.status_code == 200


def test_new_api_key_returns_200(client):
    response = client.get("/ping", headers={"X-API-Key": "new-api-key"})
    assert response.status_code == 200
