from datetime import datetime, date
from zoneinfo import ZoneInfo

import fastapi
import pydantic_core
import pytest
from buildpg import render
from pydantic import TypeAdapter

from v3.routers.latest import (
    ParameterLatestPathQuery,
    LocationLatestPathQuery,
    DatetimeMinQuery,
)
from v3.models.queries import (
    BboxQuery,
    CommaSeparatedList,
    CountryIdQuery,
    CountryIsoQuery,
    DatetimeFromQuery,
    DatetimeToQuery,
    DateFromQuery,
    DateToQuery,
    ManufacturersQuery,
    InstrumentsQuery,
    LicenseQuery,
    MobileQuery,
    MonitorQuery,
    OwnerQuery,
    Paging,
    ParametersQuery,
    ProviderQuery,
    QueryBuilder,
    RadiusQuery,
    SortingBase,
    truncate_float,
)
from v3.routers.locations import LocationPathQuery, LocationsQueries


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


class TestPaging: ...


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


class TestDatetimeFromQuery:
    def test_has_value_datetime(self):
        datetime_from_query = DatetimeFromQuery(
            datetime_from="2022-10-01T14:47:27-00:00"
        )
        params = datetime_from_query.model_dump()
        assert params == {
            "datetime_from": datetime(2022, 10, 1, 14, 47, 27, tzinfo=ZoneInfo("UTC"))
        }

    def test_has_value_date(self):
        datetime_from_query = DatetimeFromQuery(datetime_from="2022-10-01")
        params = datetime_from_query.model_dump()
        assert params == {"datetime_from": date(2022, 10, 1)}

    def test_no_value(self):
        datetime_from_query = DatetimeFromQuery()
        where = datetime_from_query.where()
        params = datetime_from_query.model_dump()
        assert where is None
        assert params == {"datetime_from": None}

    def test_date_where(self):
        datetime_from_query = DatetimeFromQuery(datetime_from="2022-10-01")
        where = datetime_from_query.where()
        assert where == "datetime > (:datetime_from::timestamp AT TIME ZONE timezone)"

    def test_datetime_tz_where(self):
        datetime_from_query = DatetimeFromQuery(
            datetime_from="2022-10-01T14:47:27-00:00"
        )
        where = datetime_from_query.where()
        assert where == "datetime > :datetime_from"

    def test_datetime_notz_where(self):
        datetime_from_query = DatetimeFromQuery(datetime_from="2022-10-01T14:47:27")
        where = datetime_from_query.where()
        assert where == "datetime > (:datetime_from::timestamp AT TIME ZONE timezone)"


class TestDatetimeToQuery:
    def test_has_value_datetime(self):
        datetime_to_query = DatetimeToQuery(datetime_to="2022-10-01T14:47:27-00:00")
        params = datetime_to_query.model_dump()
        assert params == {
            "datetime_to": datetime(2022, 10, 1, 14, 47, 27, tzinfo=ZoneInfo("UTC"))
        }

    def test_has_value_date(self):
        datetime_to_query = DatetimeToQuery(datetime_to="2022-10-01")
        params = datetime_to_query.model_dump()
        assert params == {"datetime_to": date(2022, 10, 1)}

    def test_no_value(self):
        datetime_to_query = DatetimeToQuery()
        where = datetime_to_query.where()
        params = datetime_to_query.model_dump()
        assert where is None
        assert params == {"datetime_to": None}

    def test_date_where(self):
        datetime_to_query = DatetimeToQuery(datetime_to="2022-10-01")
        where = datetime_to_query.where()
        assert where == "datetime <= (:datetime_to::timestamp AT TIME ZONE timezone)"

    def test_datetime_tz_where(self):
        datetime_to_query = DatetimeToQuery(datetime_to="2022-10-01T14:47:27-00:00")
        where = datetime_to_query.where()
        assert where == "datetime <= :datetime_to"

    def test_datetime_notz_where(self):
        datetime_to_query = DatetimeToQuery(datetime_to="2022-10-01T14:47:27")
        where = datetime_to_query.where()
        assert where == "datetime <= (:datetime_to::timestamp AT TIME ZONE timezone)"


class TestDateFromQuery:
    def test_has_value_date(self):
        date_from_query = DateFromQuery(date_from="2022-10-01")
        params = date_from_query.model_dump()
        assert params == {"date_from": date(2022, 10, 1)}

    def test_has_value_date(self):
        date_from_query = DateFromQuery(date_from="2022-10-01")
        params = date_from_query.model_dump()
        assert params == {"date_from": date(2022, 10, 1)}

    def test_no_value(self):
        date_from_query = DateFromQuery()
        where = date_from_query.where()
        params = date_from_query.model_dump()
        assert where is None
        assert params == {"date_from": None}

    def test_date_where(self):
        date_from_query = DateFromQuery(date_from="2022-10-01")
        where = date_from_query.where()
        assert where == "datetime >= :date_from::date"

    def test_date_tz_where(self):
        date_from_query = DateFromQuery(date_from="2022-10-01T14:47:27-00:00")
        where = date_from_query.where()
        assert where == "datetime >= :date_from::date"

    def test_date_notz_where(self):
        date_from_query = DateFromQuery(date_from="2022-10-01T14:47:27")
        where = date_from_query.where()
        assert where == "datetime >= :date_from::date"


class TestDateToQuery:
    def test_has_value_date(self):
        date_to_query = DateToQuery(date_to="2022-10-01T14:47:27-00:00")
        params = date_to_query.model_dump()
        assert params == {
            "date_to": date(2022, 10, 1, 14, 47, 27, tzinfo=ZoneInfo("UTC"))
        }

    def test_has_value_date(self):
        date_to_query = DateToQuery(date_to="2022-10-01")
        params = date_to_query.model_dump()
        assert params == {"date_to": date(2022, 10, 1)}

    def test_no_value(self):
        date_to_query = DateToQuery()
        where = date_to_query.where()
        params = date_to_query.model_dump()
        assert where is None
        assert params == {"date_to": None}

    def test_date_where(self):
        date_from_query = DateToQuery(date_to="2022-10-01")
        where = date_from_query.where()
        assert where == "datetime <= :date_to::date"

    def test_date_tz_where(self):
        date_to_query = DateToQuery(date_to="2022-10-01T14:47:27-00:00")
        where = date_to_query.where()
        assert where == "datetime <= :date_to::date"

    def test_date_notz_where(self):
        date_to_query = DateToQuery(date_to="2022-10-01T14:47:27")
        where = date_to_query.where()
        assert where == "datetime <= :date_to::date"


class TestParametersQuery:
    def test_has_value(self):
        parameters_query = ParametersQuery(parameters_id="1,2,3")
        where = parameters_query.where()
        params = parameters_query.model_dump()
        assert where == "parameter_ids && :parameters_id"
        assert params == {"parameters_id": [1, 2, 3]}

    def test_no_value(self):
        parameters_query = ParametersQuery()
        where = parameters_query.where()
        params = parameters_query.model_dump()
        assert where is None
        assert params == {"parameters_id": None}


class TestLicensesQuery:
    def test_has_value(self):
        licenses_query = LicenseQuery(licenses_id="1,2,3")
        where = licenses_query.where()
        params = licenses_query.model_dump()
        assert where == "license_ids && :licenses_id"
        assert params == {"licenses_id": [1, 2, 3]}

    def test_no_value(self):
        licenses_query = LicenseQuery()
        where = licenses_query.where()
        params = licenses_query.model_dump()
        assert where is None
        assert params == {"licenses_id": None}


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


class TestCountryIdQuery:
    def test_countries_id_string_value(self):
        country_id_query = CountryIdQuery(countries_id="1,2,3")
        where = country_id_query.where()
        params = country_id_query.model_dump()
        assert where == "(country->'id')::int = ANY (:countries_id)"
        assert params == {"countries_id": [1, 2, 3]}


class TestCountryIsoQuery:
    def test_iso(self):
        country_iso_query = CountryIsoQuery(iso="us")
        where = country_iso_query.where()
        params = country_iso_query.model_dump()
        assert where == "country->>'code' = :iso"
        assert params == {"iso": "us"}


class TestInstrumentQuery:
    def test_instruments_id_string_value(self):
        instruments_id_query = InstrumentsQuery(instruments_id="1,2,3")
        where = instruments_id_query.where()
        params = instruments_id_query.model_dump()
        assert where == "instrument_ids && :instruments_id"
        assert params == {"instruments_id": [1, 2, 3]}


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

    def test_valid_coordinates(self):
        with pytest.raises(fastapi.exceptions.HTTPException):
            RadiusQuery(coordinates="91.907,-77.037")
        with pytest.raises(fastapi.exceptions.HTTPException):
            RadiusQuery(coordinates="-91.907,-77.037")
        with pytest.raises(fastapi.exceptions.HTTPException):
            RadiusQuery(coordinates="-81.907,197.037")
        with pytest.raises(fastapi.exceptions.HTTPException):
            RadiusQuery(coordinates="-81.907,-197.037")

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
        radius_query = RadiusQuery(coordinates=self.coordinates, radius=self.radius)
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

    def test_has_value(self):
        where = self.bbox_query.where()
        params = self.bbox_query.model_dump()
        assert where == "ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326) && geom"
        assert params == {
            "bbox": "-77.1234,38.7916,-76.9094,38.9955",
            "minx": -77.1234,
            "miny": 38.7916,
            "maxx": -76.9094,
            "maxy": 38.9955,
        }

    def test_no_value(self):
        bbox_query = BboxQuery()
        where = bbox_query.where()
        params = bbox_query.model_dump()
        assert where is None
        assert params == {
            "bbox": None,
            "minx": None,
            "miny": None,
            "maxx": None,
            "maxy": None,
        }

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

    def test_where(self):
        assert self.bbox_query.maxy == 38.9955


class QueryContainer(CountryIsoQuery, MonitorQuery): ...


class SortTestClass(SortingBase):
    order_by: str = "id"


class QueryContainerSort(CountryIsoQuery, MonitorQuery, SortTestClass): ...


class TestQueryBuilder:
    def test_bases_method(self):
        query = QueryContainer(iso="us", monitor=True)
        query_builder = QueryBuilder(query)
        assert query_builder._bases() == [CountryIsoQuery, MonitorQuery, QueryContainer]

    def test_sortable_method(self):
        query = QueryContainerSort(iso="us", monitor=True)
        query_builder = QueryBuilder(query)
        assert query_builder._sortable == SortTestClass

    def test_sortable_method_none(self):
        query = QueryContainer(iso="us", monitor=True)
        query_builder = QueryBuilder(query)
        assert query_builder._sortable == None

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
        country_iso_query = CountryIsoQuery(iso="us")
        query_builder = QueryBuilder(country_iso_query)
        assert query_builder.fields() == ""

    def test_fields_method(self):
        radius_query = RadiusQuery(coordinates="38.9072,-77.0369", radius=1000)
        query_builder = QueryBuilder(radius_query)
        assert (
            query_builder.fields()
            == "\n,ST_Distance(geog, ST_MakePoint(:lon, :lat)::geography) as distance"
        )

    def test_total_method(self):
        radius_query = RadiusQuery(coordinates="38.9072,-77.0369", radius=1000)
        query_builder = QueryBuilder(radius_query)
        assert query_builder.total() == ", COUNT(1) OVER() as found"

    def test_pagination_method_no_paging(self):
        radius_query = RadiusQuery(coordinates="38.9072,-77.0369", radius=1000)
        query_builder = QueryBuilder(radius_query)
        assert query_builder.pagination() == ""

    def test_pagination_method_with_paging(self):
        class QueryContainer(Paging, RadiusQuery): ...

        query = QueryContainer(
            coordinates="38.9072,-77.0369", radius=1000, limit=1000, page=42
        )
        query_builder = QueryBuilder(query)
        assert query_builder.pagination() == "\nLIMIT :limit OFFSET :offset"

    def test_order_by_method(self):
        query = QueryContainerSort(iso="us", monitor=True)
        query_builder = QueryBuilder(query)
        expected = "ORDER BY id ASC"
        assert query_builder.order_by() == expected

    def test_order_by_method_none(self):
        query = QueryContainer(iso="us", monitor=True)
        query_builder = QueryBuilder(query)
        assert query_builder.order_by() == ""


class TestLocationPathQuery:
    def test_location_path_query(self):
        location_queries = LocationPathQuery(locations_id=42)
        where = render(location_queries.where(), **location_queries.model_dump())
        assert where[0] == f"id = $1"
        assert where[1] == [42]

    def test_id(self):
        with pytest.raises(fastapi.exceptions.HTTPException):
            LocationPathQuery(locations_id=0)


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


class TestManufacturersQueries:
    def test_has_value(self):
        manufacturers_query = ManufacturersQuery(manufacturers_id="1,2,3")
        where = manufacturers_query.where()
        params = manufacturers_query.model_dump()
        assert where == "manufacturer_ids && :manufacturers_id"
        assert params == {"manufacturers_id": [1, 2, 3]}

    def test_no_value(self):
        manufacturers_query = ManufacturersQuery()
        where = manufacturers_query.where()
        params = manufacturers_query.model_dump()
        assert where is None
        assert params == {"manufacturers_id": None}


class TestParameterLatestPathQuery:
    def test_has_value(self):
        parameter_latest_path_query = ParameterLatestPathQuery(parameters_id=42)
        where = parameter_latest_path_query.where()
        params = parameter_latest_path_query.model_dump()
        assert where == "m.measurands_id = :parameters_id"
        assert params == {"parameters_id": 42}

    def test_no_value(self):
        with pytest.raises(fastapi.exceptions.HTTPException):
            ParameterLatestPathQuery()


class TestLocationLatestPathQuery:
    def test_has_value(self):
        location_latest_path_query = LocationLatestPathQuery(locations_id=42)
        where = location_latest_path_query.where()
        params = location_latest_path_query.model_dump()
        assert where == "n.sensor_nodes_id = :locations_id"
        assert params == {"locations_id": 42}

    def test_no_value(self):
        with pytest.raises(fastapi.exceptions.HTTPException):
            LocationLatestPathQuery()


class TestDatetimeMinQuery:
    def test_has_no_value(self):
        query = DatetimeMinQuery(datetime_min=None)
        params = query.model_dump()
        assert params == {"datetime_min": None}
        assert query.where() == None

    def test_has_date_value(self):
        query = DatetimeMinQuery(datetime_min="2024-01-01")
        params = query.model_dump()
        assert params == {"datetime_min": date(2024, 1, 1)}
        assert (
            query.where()
            == "datetime_last > (:datetime_min::timestamp AT TIME ZONE tzid)"
        )

    def test_has_timestamp_value(self):
        query = DatetimeMinQuery(datetime_min="2024-01-01 01:01:01")
        params = query.model_dump()
        assert params == {"datetime_min": datetime(2024, 1, 1, 1, 1, 1)}
        assert (
            query.where()
            == "datetime_last > (:datetime_min::timestamp AT TIME ZONE tzid)"
        )

    def test_has_timestamptz_value(self):
        query = DatetimeMinQuery(datetime_min="2024-01-01 01:01:01-00:00")
        params = query.model_dump()
        assert params == {
            "datetime_min": datetime(2024, 1, 1, 1, 1, 1, tzinfo=ZoneInfo("UTC"))
        }
        assert query.where() == "datetime_last > :datetime_min"
