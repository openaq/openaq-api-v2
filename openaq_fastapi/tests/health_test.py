import pytest
import requests
import schemathesis
import hypothesis

from fastapi.testclient import TestClient

from openaq_fastapi.settings import settings
from openaq_fastapi.main import app

import os

dir_path = os.path.dirname(os.path.realpath(__file__))

schemathesis.fixups.install()
client = None
if settings.TESTLOCAL:
    schema = schemathesis.from_asgi("/openapi.json", app)
    client = TestClient(app)
else:
    schema = schemathesis.from_uri(
        f"{settings.FASTAPI_URL}/openapi.json"
    )


@pytest.fixture
def url_list():
    """
    List of preivously broken URLs to check to insure no regressions
    """

    with open(os.path.join(dir_path, "url_list.txt")) as file:
        urls = [line.rstrip() for line in file]
    return urls


@pytest.fixture
def max_wait():
    """
    The maximum amount of time we want to allow highly-used
    requests to run for before we question of
    there is an index or other type of error
    """
    return 4


def test_ok_status(url_list, max_wait):
    """
    Assert 1 - Confirm that frequently used URLs return
                OK status codes
    Assert 2 - Confirm that frequently used URLs respond
                within our desired time window
    """
    for url in url_list:
        print(url)
        if settings.TESTLOCAL:
            with TestClient(app) as client:
                r = client.get(url)
        else:
            r = requests.get(f"{settings.FASTAPI_URL}{url}")
        assert r.status_code == requests.codes.ok
        assert r.elapsed.total_seconds() < max_wait


@schema.parametrize()
@hypothesis.settings(max_examples=10, deadline=15000)
def test_api(case):
    if settings.TESTLOCAL:
        with TestClient(app):
            response = case.call_asgi()
            case.validate_response(response)
    else:
        case.call_and_validate()
