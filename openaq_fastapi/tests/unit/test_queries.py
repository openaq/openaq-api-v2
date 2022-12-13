from openaq_fastapi.v3.routers.locations import (
    LocationQueries,
    LocationsQueries,
)

from buildpg import render
import pytest


class TestPaging:
    ...


class TestLocations:
    def test_location_queries(self):
        location_queries = LocationQueries(id=42)
        where = render(location_queries.where(), **location_queries.params())
        assert where[0] == f"WHERE id = $1"
        assert where[1] == [42]

    def test_locations_queries(self):
        latitude = 38.9072
        longitude = -77.0369
        radius = 10
        locations_queries = LocationsQueries(
            mobile=True, coordinates=f"{latitude},{longitude}", radius=radius
        )
        where = render(locations_queries.where(), **locations_queries.params())
        assert (
            where[0]
            == "WHERE TRUE\nAND ismobile = $1\nAND st_dwithin(st_setsrid(st_makepoint($2, $3), 4326), geom, $4)"
        )
        assert where[1][0] == True
        assert where[1][1] == longitude
        assert where[1][2] == latitude
        assert where[1][3] == radius

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
