from datetime import datetime, date
from typing import Any, List

from humps import camelize
from pydantic import BaseModel, ConfigDict, Field


class JsonBase(BaseModel):
    model_config = ConfigDict(populate_by_name=True, alias_generator=camelize)


class Meta(JsonBase):
    name: str = "openaq-api"
    website: str = "/"
    page: int = 1
    limit: int = 100
    found: int | str | None = None


class OpenAQResult(JsonBase):
    meta: Meta = Meta()
    results: list[Any] = []


class DatetimeObject(JsonBase):
    utc: str
    local: str


class Coordinates(JsonBase):
    latitude: float | None = None
    longitude: float | None = None


# Base classes
class GeoJSON(JsonBase):
    type: str
    coordinates: list[Any] = []


class Period(JsonBase):
    label: str
    interval: str
    datetime_from: DatetimeObject | None = None
    datetime_to: DatetimeObject | None = None


class Coverage(JsonBase):
    expected_count: int
    expected_interval: str
    observed_count: int
    observed_interval: str
    percent_complete: float  # percent of expected values
    percent_coverage: float  # percent of time
    datetime_from: DatetimeObject | None = None
    datetime_to: DatetimeObject | None = None


class Factor(JsonBase):
    label: str
    interval: str | None = None
    order: int | None = None


class Summary(JsonBase):
    min: float | None = None
    q02: float | None = None
    q25: float | None = None
    median: float | None = None
    q75: float | None = None
    q98: float | None = None
    max: float | None = None
    avg: float | None = None
    sd: float | None = None


class CountryBase(JsonBase):
    id: int | None = None
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


class InstrumentBase(JsonBase):
    id: int
    name: str


class ManufacturerBase(JsonBase):
    id: int
    name: str


class LicenseBase(JsonBase):
    id: int
    url: str
    date_from: date
    date_to: date | None = None
    description: str | None = None


class Latest(JsonBase):
    datetime: DatetimeObject
    value: float
    coordinates: Coordinates


class InstrumentBase(JsonBase):
    id: int
    name: str


class ParameterBase(JsonBase):
    id: int
    name: str
    units: str
    display_name: str | None = None


class SensorBase(JsonBase):
    id: int
    name: str
    parameter: ParameterBase


# full classes


class Parameter(ParameterBase):
    description: str | None = None


class Country(CountryBase):
    id: int
    code: str
    name: str
    datetime_first: datetime | None = None
    datetime_last: datetime | None = None
    parameters: list[ParameterBase] | None = None


class Entity(EntityBase):
    type: str


class Provider(ProviderBase):
    source_name: str
    export_prefix: str
    license: str | None = None
    datetime_added: datetime
    datetime_first: datetime
    datetime_last: datetime
    owner_entity: EntityBase
    parameters: list[ParameterBase]
    bbox: GeoJSON | None = None


class Owner(OwnerBase): ...


class Instrument(InstrumentBase):
    is_monitor: bool = Field(alias="isMonitor")
    manufacturer: ManufacturerBase


class Manufacturer(ManufacturerBase):
    instruments: List[InstrumentBase]


class Sensor(SensorBase):
    datetime_first: DatetimeObject
    datetime_last: DatetimeObject
    coverage: Coverage
    latest: Latest
    summary: Summary


class Location(JsonBase):
    id: int
    name: str | None = None
    locality: str | None = None
    timezone: str
    country: CountryBase
    owner: EntityBase
    provider: ProviderBase
    is_mobile: bool
    is_monitor: bool
    instruments: list[InstrumentBase]
    sensors: list[SensorBase]
    coordinates: Coordinates
    licenses: list[LicenseBase] | None = None
    bounds: list[float] = Field(..., min_length=4, max_length=4)
    distance: float | None = None
    datetime_first: DatetimeObject
    datetime_last: DatetimeObject


class Measurement(JsonBase):
    # datetime: DatetimeObject
    value: float
    parameter: ParameterBase
    period: Period | None = None
    coordinates: Coordinates | None = None
    summary: Summary | None = None
    coverage: Coverage | None = None


# Similar to measurement but without timestamps
class Trend(JsonBase):
    factor: Factor
    value: float
    parameter: ParameterBase
    # coordinates: Coordinates | None
    summary: Summary
    coverage: Coverage


# response classes


class InstrumentsResponse(OpenAQResult):
    results: list[Instrument]


class LocationsResponse(OpenAQResult):
    results: list[Location]


class MeasurementsResponse(OpenAQResult):
    results: list[Measurement]


class TrendsResponse(OpenAQResult):
    results: list[Trend]


class CountriesResponse(OpenAQResult):
    results: list[Country]


class ParametersResponse(OpenAQResult):
    results: list[Parameter]


class SensorsResponse(OpenAQResult):
    results: list[Sensor]


class ProvidersResponse(OpenAQResult):
    results: list[Provider]


class ManufacturersResponse(OpenAQResult):
    results: list[Manufacturer]


class OwnersResponse(OpenAQResult):
    results: list[Owner]
