from openaq_fastapi.v3.models.queries import (
    MobileQuery,
    MonitorQuery,
    OwnerQuery,
    ProviderQuery,
    CountryQuery,
)
from openaq_fastapi.v3.routers.locations import (
    LocationQuery,
    LocationsQueries,
)

from buildpg import render
import fastapi
import pytest


class TestPaging:
    ...


class TestMobileQuery:
    def test_has_value(self):
        mobile_query = MobileQuery(mobile=True)
        where = mobile_query.where()
        params = mobile_query.dict()
        assert where == "ismobile = :mobile"
        assert params == {"mobile": True}

    def test_no_value(self):
        mobile_query = MobileQuery()
        where = mobile_query.where()
        params = mobile_query.dict()
        assert where is None
        assert params == {"mobile": None}


class TestMonitorQuery:
    def test_has_value(self):
        mobile_query = MonitorQuery(monitor=True)
        where = mobile_query.where()
        params = mobile_query.dict()
        assert where == "ismonitor = :monitor"
        assert params == {"monitor": True}

    def test_no_value(self):
        mobile_query = MonitorQuery()
        where = mobile_query.where()
        params = mobile_query.dict()
        assert where is None
        assert params == {"monitor": None}


class TestProviderQuery:
    def test_comma_int_values(self):
        provider_query = ProviderQuery(providers_id=[1, 2, 3])
        where = provider_query.where()
        params = provider_query.dict()
        assert where == "(provider->'id')::int = ANY (:providers_id)"
        assert params == {"providers_id": [1, 2, 3]}

    def test_string_value(self):
        provider_query = ProviderQuery(providers_id=["1,2,3"])
        where = provider_query.where()
        params = provider_query.dict()
        assert where == "(provider->'id')::int = ANY (:providers_id)"
        assert params == {"providers_id": [1, 2, 3]}

    def test_no_value(self):
        provider_query = ProviderQuery()
        where = provider_query.where()
        params = provider_query.dict()
        assert where == None
        assert params == {"providers_id": None}


class TestCountryQuery:
    def test_countries_id_comma_int_values(self):
        country_query = CountryQuery(countries_id=[1, 2, 3])
        where = country_query.where()
        params = country_query.dict()
        assert where == "(country->'id')::int = ANY (:countries_id)"
        assert params == {"countries_id": [1, 2, 3], "iso": None}

    def test_countries_id_string_value(self):
        country_query = CountryQuery(countries_id=["1,2,3"])
        where = country_query.where()
        params = country_query.dict()
        assert where == "(country->'id')::int = ANY (:countries_id)"
        assert params == {"countries_id": [1, 2, 3], "iso": None}

    def test_iso(self):
        country_query = CountryQuery(iso="us")
        where = country_query.where()
        params = country_query.dict()
        assert where == "country->>'code' = :iso"
        assert params == {"iso": "us", "countries_id": None}

    def test_countries_id_and_iso(self):
        with pytest.raises(fastapi.exceptions.HTTPException):
            CountryQuery(iso="us", countries_id=["1,2,3"])


class TestOwnerQuery:
    def test_comma_int_values(self):
        owner_query = OwnerQuery(owner_contacts_id=[1, 2, 3])
        where = owner_query.where()
        params = owner_query.dict()
        assert where == "(owner->'id')::int = ANY (:owner_contacts_id)"
        assert params == {"owner_contacts_id": [1, 2, 3]}

    def test_string_value(self):
        owner_query = OwnerQuery(owner_contacts_id=["1,2,3"])
        where = owner_query.where()
        params = owner_query.dict()
        assert where == "(owner->'id')::int = ANY (:owner_contacts_id)"
        assert params == {"owner_contacts_id": [1, 2, 3]}

    def test_no_value(self):
        owner_query = OwnerQuery()
        where = owner_query.where()
        params = owner_query.dict()
        assert where == None
        assert params == {"owner_contacts_id": None}


class TestRadiusQuery:
    ...


class TestBboxQuery:
    ...


class TestQueryBuilder:
    ...


class TestLocationQueries:
    def test_location_queries(self):
        location_queries = LocationQuery(locations_id=42)
        where = render(location_queries.where(), **location_queries.dict())
        assert where[0] == f"id = $1"
        assert where[1] == [42]

    def test_locations_query_radius_bbox(self):
        latitude = 38.9072
        longitude = -77.0369
        radius = 10
        with pytest.raises(ValueError):
            LocationsQueries(
                bbox=[-77.098454, 38.902139, -77.039085, 38.938467],
                coordinates=f"{latitude},{longitude}",
                radius=radius,
            )
