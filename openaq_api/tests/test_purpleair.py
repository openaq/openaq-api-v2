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

# purple air sensor and node
sensor = 393731
node = 62376

urls = [
	## v2
    {"path": "/v3/instruments/3","status": 200},
    {"path": "/v2/averages?locations_id=:node","status": 404},
    {"path": "/v2/locations/:node","status": 404},
    {"path": "/v2/latest/:node","status": 404},
    {"path": "/v2/measurements?location_id=:node","status": 404},
	## v3
    {"path": "/v3/latest?location_id=:node","status": 404},
    {"path": "/v3/locations/:node","status": 404}, # after
    {"path": "/v3/locations/:node/measurements","status": 404}, # after
    {"path": "/v3/sensors/:sensor/measurements","status": 404}, # after
    {"path": "/v3/sensors/:sensor","status": 404}, # after
	# all of the following have an added where clause
	# and we just want to make sure the sql works
    {"path": "/v2/cities?limit=1","status": 200},
    {"path": "/v2/countries?limit=1","status": 200},
    {"path": "/v2/sources?limit=1","status": 200},
    {"path": "/v3/manufacturers?limit=1","status": 200},
    {"path": "/v3/locations?limit=1","status": 200},
]


@pytest.mark.parametrize("url", urls)
class TestUrls:
    def test_urls(self, client, url):
        path = url.get('path')
        path = path.replace(':sensor', str(sensor))
        path = path.replace(':node', str(node))
        response = client.get(path)
        assert response.status_code == url.get('status')
