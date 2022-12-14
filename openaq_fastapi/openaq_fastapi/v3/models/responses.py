from typing import List, Union
from pydantic import BaseModel, Field
from pydantic.typing import Any
from humps import camelize
from datetime import datetime


class JsonBase(BaseModel):
    class Config:
        allow_population_by_field_name = True
        alias_generator = camelize


class Meta(JsonBase):
    name: str = "openaq-api"
    website: str = "/"
    page: int = 1
    limit: int = 100
    found: int = 0


class OpenAQResult(JsonBase):
    meta: Meta = Meta()
    results: List[Any] = []


#


class DatetimeObject(JsonBase):
    utc: str
    local: str


class Coordinates(JsonBase):
    latitude: Union[float, None]
    longitude: Union[float, None]


# Base classes
class GeoJSON(JsonBase):
    type: str
    coordinates: List[Any] = []


class CountryBase(JsonBase):
    id: Union[int, None]
    code: str
    name: str


class EntityBase(JsonBase):
    id: int
    name: str


class OwnerBase(JsonBase):
    id: int
    name: str


class ProviderBase(JsonBase):
    id: int
    name: str


class ManufacturerBase(JsonBase):
    id: int
    name: str
    entity: EntityBase


class InstrumentBase(JsonBase):
    id: int
    name: str


class ParameterBase(JsonBase):
    id: int
    name: str
    units: str


class SensorBase(JsonBase):
    id: int
    name: str
    parameter: ParameterBase


# full classes


class Parameter(ParameterBase):
    display_name: str
    description: str
    locations_count: int
    measurements_count: int


class Country(CountryBase):
    id: int
    code: str
    name: str
    datetime_first: datetime
    datetime_last: datetime
    parameters: List[ParameterBase]
    locations_count: int
    measurements_count: int
    providers_count: int


class Entity(EntityBase):
    type: str


class Provider(ProviderBase):
    source_name: str
    export_prefix: str
    datetime_added: datetime
    datetime_first: datetime
    datetime_last: datetime
    owner_entity: EntityBase
    locations_count: int
    measurements_count: int
    countries_count: int
    parameters: List[ParameterBase]
    bbox: GeoJSON
    datetime_added: datetime
    datetime_first: datetime
    datetime_last: datetime


class Owner(OwnerBase):
    entity: EntityBase


class Instrument(InstrumentBase):
    manufacturer: ManufacturerBase


class Manufacturer(ManufacturerBase):
    ...


class Sensor(SensorBase):
    datetime_first: DatetimeObject
    datetime_last: DatetimeObject
    value_last: float


class Location(JsonBase):
    id: int
    name: str
    locality: Union[str, None]
    timezone: str
    country: CountryBase
    owner: EntityBase
    provider: ProviderBase
    is_mobile: bool
    is_monitor: bool
    instruments: List[InstrumentBase]
    sensors: List[SensorBase]
    coordinates: Coordinates
    bounds: List[float] = Field(..., min_items=4, max_items=4)
    distance: Union[float, None]
    datetime_first: DatetimeObject
    datetime_last: DatetimeObject


class Period(JsonBase):
    label: str
    interval: str
    datetime_from: DatetimeObject
    datetime_to: DatetimeObject


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
    start_datetime: DatetimeObject
    end_datetime: DatetimeObject


# response classes


class LocationsResponse(OpenAQResult):
    results: List[Location]


class MeasurementsResponse(OpenAQResult):
    results: List[Measurement]


class CountriesResponse(OpenAQResult):
    results: List[Country]


class ParametersResponse(OpenAQResult):
    results: List[Parameter]


class SensorsResponse(OpenAQResult):
    results: List[Sensor]


class ProvidersResponse(OpenAQResult):
    results: List[Provider]


class ManufacturersResponse(OpenAQResult):
    results: List[Manufacturer]


class OwnersResponse(OpenAQResult):
    results: List[Owner]
