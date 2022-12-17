from fastapi import Request
import pytest

from openaq_fastapi.db import DB
from openaq_fastapi.v3.models.queries import MeasurementsQueries
from openaq_fastapi.v3.routers.countries import CountriesQueries, fetch_countries
from openaq_fastapi.v3.routers.locations import LocationsQueries, fetch_locations
from openaq_fastapi.v3.routers.manufacturers import (
    ManufacturersQueries,
    fetch_manufacturers,
)
from openaq_fastapi.v3.routers.measurements import fetch_measurements
from openaq_fastapi.v3.routers.owners import OwnersQueries, fetch_owners
from openaq_fastapi.v3.routers.parameters import ParametersQueries, fetch_parameters
from openaq_fastapi.v3.routers.providers import ProvidersQueries, fetch_providers
from openaq_fastapi.v3.routers.sensors import SensorsQueries, fetch_sensors
from openaq_fastapi.v3.routers.tiles import Tile, fetch_tiles


@pytest.fixture
def db():
    request = Request()
    yield DB(request)


def test_fetch_countries(db):
    query = CountriesQueries()
    fetch_countries(query, db)
    assert False


def test_fetch_locations(db):
    query = LocationsQueries()
    fetch_locations(query, db)
    assert False


def test_fetch_manufacturers(db):
    query = ManufacturersQueries()
    fetch_manufacturers(query, db)
    assert False


def test_fetch_measurements(db):
    query = MeasurementsQueries()
    fetch_measurements(query, db)
    assert False


def test_fetch_owners(db):
    query = OwnersQueries()
    fetch_owners(query, db)
    assert False


def test_fetch_parameters(db):
    query = ParametersQueries()
    fetch_parameters(query, db)
    assert False


def test_fetch_providers(db):
    query = ProvidersQueries()
    fetch_providers(query, db)
    assert False


def test_fetch_sensors(db):
    query = SensorsQueries()
    fetch_sensors(query, db)
    assert False


def test_fetch_tiles(db):
    query = Tile()
    fetch_tiles(query, db)
    assert False
