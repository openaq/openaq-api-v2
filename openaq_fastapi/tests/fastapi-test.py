import pytest
import requests

base_url = "http://127.0.0.1:8000"

# list of endpoint paths
endpoints = [
    "/v2/averages",
    "/v2/cities",
    "/v1/cities",
    "/v1/countries",
    "/v1/countries/13",
    "/v2/countries",
    "/v2/countries/13",
    "/v1/locations",
    "/v1/locations/2178",
    "/v1/latest",
    "/v1/latest/2178",
    "/v2/locations",
    "/v2/locations/2178",
    "/v2/latest",
    "/v2/latest/2178",
    "/v2/manufacturers",
    "/v2/models",
    "/v1/measurements",
    "/v2/measurements",
    "/v2/locations/tiles/2/2/1.pbf",
    "/v1/parameters",
    "/v2/parameters",
    "/v2/projects",
    "/v2/projects/22",
    "/v1/sources",
    "/v2/sources",
    "/v2/summary",
    "/v3/countries",
    "/v3/countries/13",
    "/v3/locations",
    "/v3/locations/2178",
    "/v3/locations/2178/measurements",
    "/v3/parameters",
    "/v3/parameters/2",
    "/v3/providers",
    "/v3/providers/62",
    "/v3/sensors/662",
    "/v3/locations/tiles/2/2/1.pbf",
    "/v3/locations/2178/trends/2",
]


@pytest.mark.parametrize("endpoint", endpoints)
def test_endpoints(endpoint):
    response = requests.get(base_url + endpoint)
    assert response.status_code == 200
