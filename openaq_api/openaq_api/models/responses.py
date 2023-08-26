import logging
from datetime import date, datetime
from typing import Any

from pydantic import AnyUrl, BaseModel, ConfigDict, Field

logger = logging.getLogger("responses")


class Meta(BaseModel):
    name: str = "openaq-api"
    license: str = ""
    website: str = "/"
    page: int = 1
    limit: int = 100
    found: int | str | None = None


class Date(BaseModel):
    utc: str
    local: str


class Coordinates(BaseModel):
    latitude: float | None = None
    longitude: float | None = None


class Source(BaseModel):
    url: str | None = None
    name: str
    id: str | None = None
    readme: str | None = None
    organization: str | None = None
    lifecycle_stage: str | None = None


class Manufacturer(BaseModel):
    modelname: str = Field(..., alias="modelName")
    manufacturer_name: str = Field(..., alias="manufacturerName")

    model_config = ConfigDict(protected_namespaces=())


class Parameter(BaseModel):
    id: int
    unit: str
    count: int
    average: float
    last_value: float = Field(..., alias="lastValue")
    parameter: str
    display_name: str | None = Field(None, alias="displayName")
    last_updated: str = Field(..., alias="lastUpdated")
    parameter_id: int = Field(..., alias="parameterId")
    first_updated: str = Field(..., alias="firstUpdated")
    manufacturers: list[Manufacturer] | Manufacturer = None


# Abstract class for all responses
class OpenAQResult(BaseModel):
    meta: Meta = Meta()
    results: list[Any] = []


# /v2/averages


class AveragesRow(BaseModel):
    id: list[int] | int
    name: list[str] | str
    hour: datetime | None = None
    day: date | None = None
    month: date | None = None
    year: date | None = None
    hod: int | None = None
    dow: int | None = None
    average: float
    name: list[str] | str
    measurement_count: int  # TODO make camelCase
    parameter: str
    parameter_id: int = Field(..., alias="parameterId")
    display_name: str = Field(..., alias="displayName")
    unit: str | None = None
    first_datetime: datetime
    last_datetime: datetime


class AveragesResponse(OpenAQResult):
    results: list[AveragesRow]


# /v1/countries


class CountriesRowV1(BaseModel):
    code: str
    name: str
    locations: int
    count: int
    cities: int


class CountriesResponseV1(OpenAQResult):
    results: list[CountriesRowV1]


# /v2/countries


class CountriesRow(BaseModel):
    code: str
    name: str
    locations: int
    first_updated: str = Field(..., alias="firstUpdated")
    last_updated: str = Field(..., alias="lastUpdated")
    parameters: list[str]
    count: int
    cities: int
    sources: int
    model_config = ConfigDict(populate_by_name=True)


class CountriesResponse(OpenAQResult):
    results: list[CountriesRow]


# /v1/cities


class CityRowV1(BaseModel):
    country: str
    city: str
    count: int
    locations: int


class CitiesResponseV1(OpenAQResult):
    results: list[CityRowV1]


# /v2/cities


class CityRow(BaseModel):
    country: str
    city: str
    count: int
    locations: int
    first_updated: str = Field(..., alias="firstUpdated")
    last_updated: str = Field(..., alias="lastUpdated")
    parameters: list[str]
    model_config = ConfigDict(populate_by_name=True)


class CitiesResponse(OpenAQResult):
    results: list[CityRow]


# /v1/latest


class AveragingPeriodV1(BaseModel):
    value: int | None = None
    unit: str


class LatestMeasurementRow(BaseModel):
    parameter: str
    value: float
    last_updated: str = Field(..., alias="lastUpdated")
    unit: str
    source_name: str = Field(..., alias="sourceName")
    averaging_period: AveragingPeriodV1 = Field(..., alias="averagingPeriod")


class LatestRowV1(BaseModel):
    location: str
    city: str | None = None
    country: str | None = None
    coordinates: Coordinates
    measurements: list[LatestMeasurementRow]


class LatestResponseV1(OpenAQResult):
    results: list[LatestRowV1]


# /v2/latest


class LatestMeasurement(BaseModel):
    parameter: str
    value: float
    last_updated: str = Field(..., alias="lastUpdated")
    unit: str


class LatestRow(BaseModel):
    location: str | None = None
    city: str | None = None
    country: str | None = None
    coordinates: Coordinates | None = None
    measurements: list[LatestMeasurement]


class LatestResponse(OpenAQResult):
    results: list[LatestRow]


# /v1/locations


class CountsByMeasurementItem(BaseModel):
    parameter: str
    count: int


class LocationsRowV1(BaseModel):
    id: int
    country: str
    city: str | None = None
    cities: list[str | None] | None = None
    location: str
    locations: list[str]
    source_name: str = Field(..., alias="sourceName")
    source_names: list[str] = Field(..., alias="sourceNames")
    source_type: str = Field(..., alias="sourceType")
    source_types: list[str] = Field(..., alias="sourceTypes")
    coordinates: Coordinates
    first_updated: str = Field(..., alias="firstUpdated")
    last_updated: str = Field(..., alias="lastUpdated")
    parameters: list[str]
    counts_by_measurement: list[CountsByMeasurementItem] = Field(
        ..., alias="countsByMeasurement"
    )
    count: int


class LocationsResponseV1(OpenAQResult):
    results: list[LocationsRowV1]


# /v2/locations
def warn_on_null(v):
    logger.debug(v)


class LocationsRow(BaseModel):
    id: int
    city: str | None = None
    name: str | None = None
    entity: str | None = None
    country: str | None = None
    sources: list[Source] | Source = None
    is_mobile: bool = Field(..., alias="isMobile")
    is_analysis: bool | None = Field(None, alias="isAnalysis")
    parameters: list[Parameter]
    sensor_type: str | None = Field(None, alias="sensorType")
    coordinates: Coordinates | None = None
    last_updated: str = Field(..., alias="lastUpdated")
    first_updated: str = Field(..., alias="firstUpdated")
    measurements: int
    bounds: list[float] | float = None
    manufacturers: list[Manufacturer] | Manufacturer = None


class LocationsResponse(OpenAQResult):
    results: list[LocationsRow]


# /v2/manufacturers


class ManufacturersResponse(OpenAQResult):
    results: list[str]


# /v1/measurements


class MeasurementsRowV1(BaseModel):
    location: str
    parameter: str
    value: float
    date: Date
    unit: str
    coordinates: Coordinates
    country: str | None = None
    city: str | None = None


class MeasurementsResponseV1(OpenAQResult):
    results: list[MeasurementsRowV1]


# /v2/measurements


class MeasurementsRow(BaseModel):
    location_id: int = Field(..., alias="locationId")
    location: str
    parameter: str
    value: float
    date: Date
    unit: str
    coordinates: Coordinates | None = None
    country: str | None = None
    city: str | None = None
    is_mobile: bool = Field(..., alias="isMobile")
    is_analysis: bool | None = Field(None, alias="isAnalysis")
    entity: str | None = None
    sensor_type: str = Field(..., alias="sensorType")


class MeasurementsResponse(OpenAQResult):
    results: list[MeasurementsRow]


# /v2/models


class ModelsResponse(OpenAQResult):
    results: list[str]


# /v1/parameters


class ParametersRowV1(BaseModel):
    id: int
    name: str
    display_name: str | None = Field(None, alias="displayName")
    description: str | None = Field(None)
    preferred_unit: str = Field(..., alias="preferredUnit")
    model_config = ConfigDict(populate_by_name=True)


class ParametersResponseV1(OpenAQResult):
    results: list[ParametersRowV1]


# /v2/parameters


class ParametersRow(BaseModel):
    id: int
    name: str
    display_name: str | None = Field(None, alias="displayName")
    description: str
    preferred_unit: str = Field(..., alias="preferredUnit")
    model_config = ConfigDict(populate_by_name=True)


class ParametersResponse(OpenAQResult):
    results: list[ParametersRow]


# /v2/projects

# TODO convert fields to camelCase


class ProjectsSource(BaseModel):
    id: str
    name: str
    readme: str | None = None
    data_avg_dur: str | None = None
    organization: str | None = None
    lifecycle_stage: str | None = None


class ProjectsRow(BaseModel):
    id: int
    name: str
    subtitle: str
    is_mobile: bool = Field(..., alias="isMobile")
    is_analysis: bool | None = Field(None, alias="isAnalysis")
    entity: str | None = None
    sensor_type: str | None = Field(None, alias="sensorType")
    locations: int
    location_ids: list[int] = Field(..., alias="locationIds")
    countries: list[str]
    parameters: list[Parameter]
    bbox: list[float] | None = None
    measurements: int
    first_updated: str = Field(..., alias="firstUpdated")
    last_updated: str = Field(..., alias="lastUpdated")
    sources: list[ProjectsSource]


class ProjectsResponse(OpenAQResult):
    results: list[ProjectsRow]


# /v1/sources


class SourcesRowV1(BaseModel):
    url: str
    adapter: str
    name: str
    city: str | None = None
    country: str
    description: str | None = None
    source_url: AnyUrl = Field(..., alias="sourceURL")
    resolution: str | None = None
    contacts: list[str]
    active: bool


class SourcesResponseV1(OpenAQResult):
    results: list[SourcesRowV1]


# /v2/sources


class Datum(BaseModel):
    url: str | None = None
    data_avg_dur: str | None = None
    organization: str | None = None
    lifecycle_stage: str | None = None


class SourcesRow(BaseModel):
    data: Datum | None = None
    readme: str | None = None
    source_id: int = Field(..., alias="sourceId")
    locations: int
    source_name: str = Field(..., alias="sourceName")
    source_slug: str | None = Field(None, alias="sourceSlug")


class SourcesResponse(OpenAQResult):
    results: list[SourcesRow]


# /v2/summary


class SummaryRow(BaseModel):
    count: int
    cities: int
    sources: int
    countries: int
    locations: int


class SummaryResponse(OpenAQResult):
    results: list[SummaryRow]
