from fastapi.testclient import TestClient
import json
import time
import os
import pytest
from openaq_api.main import app
from openaq_api.db import db_pool


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


# mock sensor and node
sensor = 1
node = 1

urls = [
    ## v2
    {"path": "/v2/averages?locations_id=:node", "status": 200},
    {"path": "/v2/locations/:node", "status": 200},
    {"path": "/v2/latest/:node", "status": 200},
    {"path": "/v2/measurements?location_id=:node", "status": 200},
    # all of the following have an added where clause
    # and we just want to make sure the sql works
    {"path": "/v2/cities?limit=1", "status": 200},
    {"path": "/v2/countries?limit=1", "status": 200},
    {"path": "/v2/sources?limit=1", "status": 200},
    {"path": "/v3/manufacturers?limit=1", "status": 200},
    {"path": "/v3/locations?limit=1", "status": 200},
    {"path": "/v3/licenses", "status": 200},
    {"path": "/v3/licenses/:node", "status": 200},
    ## v3
    {"path": "/v3/instruments/3", "status": 200},
    {"path": "/v3/locations/:node", "status": 200},  # after
    {"path": "/v3/sensors/:sensor/measurements", "status": 200},  # after
    {"path": "/v3/sensors/:sensor", "status": 200},  # after
]


@pytest.mark.parametrize("url", urls)
class TestUrls:
    def test_urls(self, client, url):
        path = url.get("path")
        path = path.replace(":sensor", str(sensor))
        path = path.replace(":node", str(node))
        response = client.get(path)
        code = url.get("status")
        if code == 404:
            data = json.loads(response.content)
            assert len(data["results"]) == 0
        else:
            assert response.status_code == url.get("status")
