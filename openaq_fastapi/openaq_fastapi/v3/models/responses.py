from typing import List, Union
from pydantic import BaseModel
from pydantic.typing import Any
from humps import camelize


class JsonBase(BaseModel):
    class Config:
        alias_generator = camelize


class Meta(JsonBase):
    name: str = "openaq-api"
    license: str = "CC BY 4.0d"
    website: str = "/"
    page: int = 1
    limit: int = 100
    found: int = 0


class OpenAQResult(JsonBase):
    meta: Meta = Meta()
    results: List[Any] = []


class CountryBase(JsonBase):
    id: Union[int, None]
    code: str
    name: str


class ParameterBase(JsonBase):
    id: int
    name: str
    units: str


class Parameter(ParameterBase):
    id: int
    name: str
    display_name: str
    description: str
    units: str


class Country(CountryBase):
    id: int
    code: str
    name: str
    locations_count: int
    first_datetime: str
    last_datetime: str
    parameters: List[ParameterBase]
    meaurements_count: int
    cities_count: int
    providers_count: int


class ContactBase(JsonBase):
    id: int
    name: str


class Contact(ContactBase):
    id: int
    name: str


class Source(JsonBase):
    url: str
    name: str
    id: str
    readme: str
    organization: str
    lifecycle_stage: str


class Coordinates(JsonBase):
    latitude: Union[float, None]
    longitude: Union[float, None]


class ProviderBase(JsonBase):
    id: int
    name: str


class Provider(ProviderBase):
    contact: ContactBase


class InstrumentBase(JsonBase):
    id: int
    name: str


class Instrument(InstrumentBase):
    manufacturer: Contact


class Coordinates(JsonBase):
    latitude: float
    longitude: float


class Datetime(JsonBase):
    utc: str
    local: str


class SensorBase(JsonBase):
    id: int
    name: str
    parameter: ParameterBase


class Sensor(SensorBase):
    datetime_first: Datetime
    datetime_last: Datetime
    value_last: float


class Location(JsonBase):
    id: int
    name: str
    locality: Union[str, None]
    country: CountryBase
    owner: ContactBase
    provider: ProviderBase
    is_mobile: bool
    is_monitor: bool
    instruments: List[InstrumentBase]
    sensors: List[SensorBase]
    coordinates: Coordinates
    bounds: List[float, float, float, float]
    distance: Union[float, None]
    datetime_first: Datetime
    datetime_last: Datetime


class LocationsResponse(OpenAQResult):
    results: List[Location]


class MeasurementsResponse(JsonBase):
    parameter: str
    value: float
    date: Datetime
    unit: str
    coordinates: Union[Coordinates, None]


class CountriesResponse(OpenAQResult):
    results: List[Country]
