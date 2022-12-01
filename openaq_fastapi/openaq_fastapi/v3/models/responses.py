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


class Contact(JsonBase):
    id: int
    name: str


class Instrument(JsonBase):
    id: int
    name: str
    manufacturer: Contact


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


class Location(JsonBase):
    id: int
    city: Union[str, None]
    name: str
    country: CountryBase
    owner: Contact
    provider: Contact
    is_mobile: bool
    is_monitor: Union[bool, None]
    instruments: List[Instrument]
    parameters: List[ParameterBase]
    coordinates: Coordinates
    last_updated: str
    first_updated: str
    bounds: List[float]


class Date(JsonBase):
    utc: str
    local: str


class Coordinates(JsonBase):
    latitude: float
    longitude: float


class LocationsResponse(OpenAQResult):
    results: List[Location]


class MeasurementsResponse(JsonBase):
    parameter: str
    value: float
    date: Date
    unit: str
    coordinates: Union[Coordinates, None]


class CountriesResponse(OpenAQResult):
    results: List[Country]
