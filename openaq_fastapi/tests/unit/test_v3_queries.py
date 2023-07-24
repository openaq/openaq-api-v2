from pydantic import TypeAdapter
import pydantic_core
from openaq_fastapi.v3.models.queries import (
    truncate_float,
    BboxQuery,
    MobileQuery,
    MonitorQuery,
    OwnerQuery,
    ProviderQuery,
    CountryQuery,
    QueryBuilder,
    RadiusQuery,
    CommaSeparatedList,
)
from openaq_fastapi.v3.routers.locations import (
    LocationQuery,
    LocationsQueries,
)

from buildpg import render
import fastapi
import pytest


class TestTruncateFloat:
    def test_float_default_length(self):
        value = truncate_float(1.123456)
        assert value == 1.1234

    def test_float_custom_length(self):
        value = truncate_float(1.123456, 5)
        assert value == 1.12345

    def test_int(self):
        value = truncate_float(1)
        assert value == 1.0


class TestCommaSeparatedList:
    @pytest.fixture(autouse=True)
    def set_comma_separated_list(self):
        self.comma_separated_list = TypeAdapter(CommaSeparatedList[int])

    def test_single_string_int(self):
        assert self.comma_separated_list.validate_python("1") == [1]

    def test_list_ints(self):
        assert self.comma_separated_list.validate_python([1, 2, 3]) == [1, 2, 3]

    def test_list_string(self):
        assert self.comma_separated_list.validate_python("1,2,3") == [1, 2, 3]

    def test_list_non_int(self):
        with pytest.raises(pydantic_core._pydantic_core.ValidationError):
            self.comma_separated_list.validate_python("1,2,foo")


class TestPaging:
    ...


class TestMobileQuery:
    def test_has_value(self):
        mobile_query = MobileQuery(mobile=True)
        where = mobile_query.where()
        params = mobile_query.model_dump()
        assert where == "ismobile = :mobile"
        assert params == {"mobile": True}

    def test_no_value(self):
        mobile_query = MobileQuery()
        where = mobile_query.where()
        params = mobile_query.model_dump()
        assert where is None
        assert params == {"mobile": None}


class TestMonitorQuery:
    def test_has_value(self):
        mobile_query = MonitorQuery(monitor=True)
        where = mobile_query.where()
        params = mobile_query.model_dump()
        assert where == "ismonitor = :monitor"
        assert params == {"monitor": True}

    def test_no_value(self):
        mobile_query = MonitorQuery()
        where = mobile_query.where()
        params = mobile_query.model_dump()
        assert where is None
        assert params == {"monitor": None}


class TestProviderQuery:
    def test_string_value(self):
        provider_query = ProviderQuery(providers_id="1,2,3")
        where = provider_query.where()
        params = provider_query.model_dump()
        assert where == "(provider->'id')::int = ANY (:providers_id)"
        assert params == {"providers_id": [1, 2, 3]}

    def test_no_value(self):
        provider_query = ProviderQuery()
        where = provider_query.where()
        params = provider_query.model_dump()
        assert where == None
        assert params == {"providers_id": None}


class TestCountryQuery:
    def test_countries_id_string_value(self):
        country_query = CountryQuery(countries_id="1,2,3")
        where = country_query.where()
        params = country_query.model_dump()
        assert where == "(country->'id')::int = ANY (:countries_id)"
        assert params == {"countries_id": [1, 2, 3], "iso": None}

    def test_iso(self):
        country_query = CountryQuery(iso="us")
        where = country_query.where()
        params = country_query.model_dump()
        assert where == "country->>'code' = :iso"
        assert params == {"iso": "us", "countries_id": None}

    def test_countries_id_and_iso(self):
        with pytest.raises(fastapi.exceptions.HTTPException):
            CountryQuery(iso="us", countries_id=["1,2,3"])


class TestOwnerQuery:
    def test_string_value(self):
        owner_query = OwnerQuery(owner_contacts_id="1,2,3")
        where = owner_query.where()
        params = owner_query.model_dump()
        assert where == "(owner->'id')::int = ANY (:owner_contacts_id)"
        assert params == {"owner_contacts_id": [1, 2, 3]}

    def test_no_value(self):
        owner_query = OwnerQuery()
        where = owner_query.where()
        params = owner_query.model_dump()
        assert where == None
        assert params == {"owner_contacts_id": None}


class TestRadiusQuery:
    @pytest.fixture(autouse=True)
    def set_radius(self):
        self.radius = 1000

    @pytest.fixture(autouse=True)
    def set_coords(self):
        self.coordinates = "38.907,-77.037"

    def test_radius_no_coordinates(self):
        with pytest.raises(fastapi.exceptions.HTTPException):
            RadiusQuery(radius=self.radius)

    def test_coordinates_no_radius(self):
        with pytest.raises(fastapi.exceptions.HTTPException):
            RadiusQuery(coordinates=self.coordinates)

    def test_radius_within_range(self):
        radius_query = RadiusQuery(coordinates=self.coordinates, radius=self.radius)
        assert radius_query.radius == self.radius

    def test_radius_out_of_range_lower(self):
        radius = -10
        with pytest.raises(fastapi.exceptions.HTTPException):
            RadiusQuery(coordinates=self.coordinates, radius=radius)

    def test_radius_out_of_range_upper(self):
        radius = 25001
        with pytest.raises(fastapi.exceptions.HTTPException):
            RadiusQuery(coordinates=self.coordinates, radius=radius)

    def test_lat_lon_properties(self):
        radius = 1000
        radius_query = RadiusQuery(coordinates=self.coordinates, radius=radius)
        assert radius_query.lat == 38.907
        assert radius_query.lon == -77.037

    def test_fields_method(self):
        radius_query = RadiusQuery(radius=self.radius, coordinates=self.coordinates)
        assert (
            radius_query.fields()
            == "ST_Distance(geog, ST_MakePoint(:lon, :lat)::geography) as distance"
        )
        assert (
            radius_query.fields("geom")
            == "ST_Distance(geom, ST_MakePoint(:lon, :lat)::geography) as distance"
        )

    def test_fields_method_none(self):
        """
        Test that is none is passed to RadiusQuery fields method returns None
        """
        radius_query = RadiusQuery()
        assert radius_query.fields() == None

    def test_where_method(self):
        radius_query = RadiusQuery(radius=self.radius, coordinates=self.coordinates)
        assert (
            radius_query.where()
            == "ST_DWithin(ST_MakePoint(:lon, :lat)::geography, geog, :radius)"
        )
        assert (
            radius_query.where("geom")
            == "ST_DWithin(ST_MakePoint(:lon, :lat)::geography, geom, :radius)"
        )

    def test_fields_method_none(self):
        """
        Test that is none is passed to RadiusQuery where method returns None
        """
        radius_query = RadiusQuery()
        assert radius_query.where() == None


class TestBboxQuery:
    @pytest.fixture(autouse=True)
    def set_bbox(self):
        self.bbox = "-77.1234,38.7916,-76.9094,38.9955"
        self.bbox_query = BboxQuery(bbox=self.bbox)

    def test_bbox_in_range(self):
        with pytest.raises(fastapi.exceptions.HTTPException):
            BboxQuery(bbox="-181.0,38.7916,-76.9094,38.9955")
        with pytest.raises(fastapi.exceptions.HTTPException):
            BboxQuery(bbox="-179.0,91.7916,-76.9094,38.9955")
        with pytest.raises(fastapi.exceptions.HTTPException):
            BboxQuery(bbox="-179.0,38.7916,-181.9094,38.9955")
        with pytest.raises(fastapi.exceptions.HTTPException):
            BboxQuery(bbox="-179.0,38.7916,-76.9094,-90.9955")

    def test_minx(self):
        assert self.bbox_query.minx == -77.1234

    def test_miny(self):
        assert self.bbox_query.miny == 38.7916

    def test_maxx(self):
        assert self.bbox_query.maxx == -76.9094

    def test_maxy(self):
        assert self.bbox_query.maxy == 38.9955


class QueryContainer(CountryQuery, MonitorQuery):
    ...


class TestQueryBuilder:
    def test_bases_method(self):
        query = QueryContainer(iso="us", monitor=True)
        query_builder = QueryBuilder(query)
        assert query_builder._bases() == [CountryQuery, MonitorQuery, QueryContainer]

    def test_params_method(self):
        query = QueryContainer(iso="us", monitor=True)
        query_builder = QueryBuilder(query)
        assert query_builder.params() == {"monitor": True, "iso": "us"}

    def test_where_method_single(self):
        expected = "WHERE country->>'code' = :iso"
        query = QueryContainer(iso="us")
        query_builder = QueryBuilder(query)
        assert query_builder.where() == expected

    def test_where_method_multiple(self):
        expected = "WHERE country->>'code' = :iso\nAND ismonitor = :monitor"
        query = QueryContainer(iso="us", monitor=True)
        query_builder = QueryBuilder(query)
        assert query_builder.where() == expected

    def test_fields_method_none(self):
        country_query = CountryQuery(iso="us")
        query_builder = QueryBuilder(country_query)
        assert query_builder.fields() == ""

    def test_fields_method(self):
        radius_query = RadiusQuery(coordinates="38.9072,-77.0369", radius=1000)
        query_builder = QueryBuilder(radius_query)
        assert (
            query_builder.fields()
            == "\n,ST_Distance(geog, ST_MakePoint(:lon, :lat)::geography) as distance"
        )


class TestLocationQuery:
    def test_location_query(self):
        location_queries = LocationQuery(locations_id=42)
        where = render(location_queries.where(), **location_queries.model_dump())
        assert where[0] == f"id = $1"
        assert where[1] == [42]

    def test_id(self):
        with pytest.raises(fastapi.exceptions.HTTPException):
            LocationQuery(locations_id=0)


class TestLocationsQueries:
    def test_locations_query_radius_bbox(self):
        latitude = 38.9072
        longitude = -77.0369
        radius = 10
        with pytest.raises(fastapi.exceptions.HTTPException):
            LocationsQueries(
                bbox="-77.0984,38.9021,-77.0390,38.9384",
                coordinates=f"{latitude},{longitude}",
                radius=radius,
            )
