from datetime import datetime, date
from typing import Any, List

from humps import camelize
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .utils import fix_date


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
    utc: datetime
    local: datetime


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


class AttributionEntity(JsonBase):
    name: str
    url: str | None = None


class LocationLicense(JsonBase):
    id: int
    name: str
    attribution: AttributionEntity
    date_from: date | None = None
    date_to: date | None = None

    @field_validator("date_from", "date_to", mode="before")
    def check_dates(cls, v):
        return fix_date(v)


class ProviderLicense(LocationLicense):
    id: int
    name: str
    date_from: date
    date_to: date | None = None


class License(JsonBase):
    id: int
    name: str
    commercial_use_allowed: bool
    attribution_required: bool
    share_alike_required: bool
    modification_allowed: bool
    redistribution_allowed: bool
    source_url: str


class LatestBase(JsonBase):
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
    datetime_added: datetime
    datetime_first: datetime
    datetime_last: datetime
    entities_id: int
    parameters: list[ParameterBase]
    bbox: GeoJSON | None = None


class Owner(OwnerBase): ...


class Instrument(InstrumentBase):
    is_monitor: bool = Field(alias="isMonitor")
    manufacturer: ManufacturerBase


class Manufacturer(ManufacturerBase):
    instruments: List[InstrumentBase]


class Sensor(SensorBase):
    datetime_first: DatetimeObject | None = None
    datetime_last: DatetimeObject | None = None
    coverage: Coverage | None = None
    latest: LatestBase | None = None
    summary: Summary | None = None


class Latest(LatestBase):
    sensors_id: int
    locations_id: int


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
    licenses: list[LocationLicense] | None = None
    bounds: list[float] = Field(..., min_length=4, max_length=4)
    distance: float | None = None
    datetime_first: DatetimeObject | None = None
    datetime_last: DatetimeObject | None = None


class FlagType(JsonBase):
    flag_types_id: int = Field(alias='id')
    label: str
    level: str

class LocationFlag(JsonBase):
    #model_config = ConfigDict(exclude_unset=True)
    location_id: int
    flag_type: FlagType
    datetime_from: DatetimeObject
    datetime_to: DatetimeObject
    sensor_ids: list[int] = []
    note: str | None = None


class FlagInfo(JsonBase):
    has_flags: bool

    @model_validator(mode='before')
    @classmethod
    def check_data_type(cls, data: Any):
        if isinstance(data, bool):
            data = { "has_flags": data }
        return data


class Version(JsonBase):
    versions_id: int
    parent_sensor: dict
    sensor: dict
    life_cycle: str
    version_date: date
    version_rank: int

class Measurement(JsonBase):
    value: float
    flag_info: FlagInfo
    parameter: ParameterBase
    period: Period | None = None
    coordinates: Coordinates | None = None
    summary: Summary | None = None
    coverage: Coverage | None = None


class HourlyData(Measurement):
    value: float | None = None  # Nullable to deal with errors


class DailyData(Measurement):
    value: float | None = None  # Nullable to deal with errors


class AnnualData(Measurement):
    value: float | None = None  # Nullable to deal with errors


# Similar to measurement but without timestamps
class Trend(JsonBase):
    factor: Factor
    value: float
    parameter: ParameterBase
    summary: Summary
    coverage: Coverage


# response classes


class InstrumentsResponse(OpenAQResult):
    results: list[Instrument]


class LocationsResponse(OpenAQResult):
    results: list[Location]


class MeasurementsResponse(OpenAQResult):
    results: list[Measurement]


class HourlyDataResponse(OpenAQResult):
    results: list[HourlyData]


class DailyDataResponse(OpenAQResult):
    results: list[DailyData]


class AnnualDataResponse(OpenAQResult):
    results: list[AnnualData]


class TrendsResponse(OpenAQResult):
    results: list[Trend]


class LicensesResponse(OpenAQResult):
    results: list[License]


class CountriesResponse(OpenAQResult):
    results: list[Country]


class ParametersResponse(OpenAQResult):
    results: list[Parameter]


class SensorsResponse(OpenAQResult):
    results: list[Sensor]


class LocationFlagsResponse(OpenAQResult):
    results: list[LocationFlag]


class VersionsResponse(OpenAQResult):
    results: list[Version]


class ProvidersResponse(OpenAQResult):
    results: list[Provider]


class ManufacturersResponse(OpenAQResult):
    results: list[Manufacturer]


class OwnersResponse(OpenAQResult):
    results: list[Owner]


class LatestResponse(OpenAQResult):
    results: list[Latest]
