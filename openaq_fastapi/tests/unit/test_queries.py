from openaq_fastapi.v3.routers.locations import (
    LocationQueries,
    LocationsQueries,
)

from buildpg import render


class TestLocations:
    def test_location_queries(self):
        location_queries = LocationQueries(id=42)
        where = render(location_queries.clause(), **location_queries.params())
        assert where[0] == f"WHERE id = $1"
        assert where[1] == [42]

    def test_locations_queries(self):
        locations_queries = LocationsQueries(
            mobile=True, coordinates="33,33", radius=10
        )
        where = render(locations_queries.clause(), **locations_queries.params())
        assert (
            where[0]
            == "WHERE TRUE\nAND ismobile = $1\nAND st_dwithin(st_setsrid(st_makepoint($2, $3), 4326), geom, $4)"
        )
