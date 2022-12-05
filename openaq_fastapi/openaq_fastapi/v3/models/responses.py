from typing import List, Union
from pydantic import BaseModel, Field
from pydantic.typing import Any
from humps import camelize


class JsonBase(BaseModel):
    class Config:
        allow_population_by_field_name = True
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
    timezone: str
    country: CountryBase
    owner: ContactBase
    provider: ProviderBase
    is_mobile: bool
    is_monitor: bool
    instruments: List[InstrumentBase]
    sensors: List[SensorBase]
    coordinates: Coordinates
    bounds: List[float] = Field(..., min_items=4, max_items=4)
    distance: Union[float, None]
    datetime_first: Datetime
    datetime_last: Datetime


class Period(JsonBase):
    label: str
    interval: str
    datetime_from: Datetime
    datetime_to: Datetime


class Summary(JsonBase):
    min: float
    q02: float
    q25: float
    median: float
    q75: float
    q98: float
    max: float
    sd: float


class Coverage(JsonBase):
    observed_count: int
    expected_count: int
    percent_complete: float
    observed_interval: str
    expected_interval: str


class Measurement(JsonBase):
    value: float
    parameter: ParameterBase
    coordinates: Union[Coordinates, None]
    period: Period
    summary: Summary
    coverage: Coverage
    start_datetime: Datetime
    end_datetime: Datetime


class LocationsResponse(OpenAQResult):
    results: List[Location]


class MeasurementsResponse(OpenAQResult):
    results: List[Measurement]


class CountriesResponse(OpenAQResult):
    results: List[Country]
