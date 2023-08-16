from datetime import date, datetime
from typing import List, Union, Any

from pydantic import ConfigDict, BaseModel, AnyUrl, Field, validator
import orjson
from starlette.responses import JSONResponse
import logging

logger = logging.getLogger("responses")


class Meta(BaseModel):
    name: str = "openaq-api"
    license: str = ""
    website: str = "/"
    page: int = 1
    limit: int = 100
    found: Union[int, str, None] = None


class Date(BaseModel):
    utc: str
    local: str


class Coordinates(BaseModel):
    latitude: Union[float, None] = None
    longitude: Union[float, None] = None


class Source(BaseModel):
    url: Union[str, None] = None
    name: str
    id: Union[str, None] = None
    readme: Union[str, None] = None
    organization: Union[str, None] = None
    lifecycle_stage: Union[str, None] = None


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
    display_name: Union[str, None] = Field(None, alias="displayName")
    last_updated: str = Field(..., alias="lastUpdated")
    parameter_id: int = Field(..., alias="parameterId")
    first_updated: str = Field(..., alias="firstUpdated")
    manufacturers: Union[List[Manufacturer], None] = None


# Abstract class for all responses
class OpenAQResult(BaseModel):
    meta: Meta = Meta()
    results: List[Any] = []


# /v2/averages


class AveragesRow(BaseModel):
    id: Union[List[int], int]
    name: Union[List[str], str]
    hour: Union[datetime, None] = None
    day: Union[date, None] = None
    month: Union[date, None] = None
    year: Union[date, None] = None
    hod: Union[int, None] = None
    dow: Union[int, None] = None
    average: float
    name: Union[List[str], str]
    measurement_count: int  # TODO make camelCase
    parameter: str
    parameter_id: int = Field(..., alias="parameterId")
    display_name: str = Field(..., alias="displayName")
    unit: Union[str, None] = None
    first_datetime: datetime
    last_datetime: datetime


class AveragesResponse(OpenAQResult):
    results: List[AveragesRow]


# /v1/countries


class CountriesRowV1(BaseModel):
    code: str
    name: str
    locations: int
    count: int
    cities: int


class CountriesResponseV1(OpenAQResult):
    results: List[CountriesRowV1]


# /v2/countries


class CountriesRow(BaseModel):
    code: str
    name: str
    locations: int
    first_updated: str = Field(..., alias="firstUpdated")
    last_updated: str = Field(..., alias="lastUpdated")
    parameters: List[str]
    count: int
    cities: int
    sources: int
    model_config = ConfigDict(populate_by_name=True)


class CountriesResponse(OpenAQResult):
    results: List[CountriesRow]


# /v1/cities


class CityRowV1(BaseModel):
    country: str
    city: str
    count: int
    locations: int


class CitiesResponseV1(OpenAQResult):
    results: List[CityRowV1]


# /v2/cities


class CityRow(BaseModel):
    country: str
    city: str
    count: int
    locations: int
    first_updated: str = Field(..., alias="firstUpdated")
    last_updated: str = Field(..., alias="lastUpdated")
    parameters: List[str]
    model_config = ConfigDict(populate_by_name=True)


class CitiesResponse(OpenAQResult):
    results: List[CityRow]


# /v1/latest


class AveragingPeriodV1(BaseModel):
    value: Union[int, None] = None
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
    city: Union[str, None] = None
    country: Union[str, None] = None
    coordinates: Coordinates
    measurements: List[LatestMeasurementRow]


class LatestResponseV1(OpenAQResult):
    results: List[LatestRowV1]


# /v2/latest


class LatestMeasurement(BaseModel):
    parameter: str
    value: float
    last_updated: str = Field(..., alias="lastUpdated")
    unit: str


class LatestRow(BaseModel):
    location: Union[str, None] = None
    city: Union[str, None] = None
    country: Union[str, None] = None
    coordinates: Union[Coordinates, None] = None
    measurements: List[LatestMeasurement]


class LatestResponse(OpenAQResult):
    results: List[LatestRow]


# /v1/locations


class CountsByMeasurementItem(BaseModel):
    parameter: str
    count: int


class LocationsRowV1(BaseModel):
    id: int
    country: str
    city: Union[str, None] = None
    cities: Union[List[Union[str, None]], None] = None
    location: str
    locations: List[str]
    source_name: str = Field(..., alias="sourceName")
    source_names: List[str] = Field(..., alias="sourceNames")
    source_type: str = Field(..., alias="sourceType")
    source_types: List[str] = Field(..., alias="sourceTypes")
    coordinates: Coordinates
    first_updated: str = Field(..., alias="firstUpdated")
    last_updated: str = Field(..., alias="lastUpdated")
    parameters: List[str]
    counts_by_measurement: List[CountsByMeasurementItem] = Field(
        ..., alias="countsByMeasurement"
    )
    count: int


class LocationsResponseV1(OpenAQResult):
    results: List[LocationsRowV1]


# /v2/locations
def warn_on_null(v):
    logger.debug(v)


class LocationsRow(BaseModel):
    id: int
    city: Union[str, None] = None
    name: Union[str, None] = None
    entity: Union[str, None] = None
    country: Union[str, None] = None
    sources: Union[List[Source], None] = None
    is_mobile: bool = Field(..., alias="isMobile")
    is_analysis: Union[bool, None] = Field(None, alias="isAnalysis")
    parameters: List[Parameter]
    sensor_type: Union[str, None] = Field(None, alias="sensorType")
    coordinates: Union[Coordinates, None] = None
    last_updated: str = Field(..., alias="lastUpdated")
    first_updated: str = Field(..., alias="firstUpdated")
    measurements: int
    bounds: Union[List[float], None] = None
    manufacturers: Union[List[Manufacturer], None] = None


class LocationsResponse(OpenAQResult):
    results: List[LocationsRow]


# /v2/manufacturers


class ManufacturersResponse(OpenAQResult):
    results: List[str]


# /v1/measurements


class MeasurementsRowV1(BaseModel):
    location: str
    parameter: str
    value: float
    date: Date
    unit: str
    coordinates: Coordinates
    country: Union[str, None] = None
    city: Union[str, None] = None


class MeasurementsResponseV1(OpenAQResult):
    results: List[MeasurementsRowV1]


# /v2/measurements


class MeasurementsRow(BaseModel):
    location_id: int = Field(..., alias="locationId")
    location: str
    parameter: str
    value: float
    date: Date
    unit: str
    coordinates: Union[Coordinates, None] = None
    country: Union[str, None] = None
    city: Union[str, None] = None
    is_mobile: bool = Field(..., alias="isMobile")
    is_analysis: Union[bool, None] = Field(None, alias="isAnalysis")
    entity: Union[str, None] = None
    sensor_type: str = Field(..., alias="sensorType")


class MeasurementsResponse(OpenAQResult):
    results: List[MeasurementsRow]


# /v2/models


class ModelsResponse(OpenAQResult):
    results: List[str]


# /v1/parameters


class ParametersRowV1(BaseModel):
    id: int
    name: str
    description: str
    preferred_unit: str = Field(..., alias="preferredUnit")
    model_config = ConfigDict(populate_by_name=True)


class ParametersResponseV1(OpenAQResult):
    results: List[ParametersRowV1]


# /v2/parameters


class ParametersRow(BaseModel):
    id: int
    name: str
    display_name: Union[str, None] = Field(None, alias="displayName")
    description: str
    preferred_unit: str = Field(..., alias="preferredUnit")
    model_config = ConfigDict(populate_by_name=True)


class ParametersResponse(OpenAQResult):
    results: List[ParametersRow]


# /v2/projects

# TODO convert fields to camelCase


class ProjectsSource(BaseModel):
    id: str
    name: str
    readme: Union[str, None] = None
    data_avg_dur: Union[str, None] = None
    organization: Union[str, None] = None
    lifecycle_stage: Union[str, None] = None


class ProjectsRow(BaseModel):
    id: int
    name: str
    subtitle: str
    is_mobile: bool = Field(..., alias="isMobile")
    is_analysis: Union[bool, None] = Field(None, alias="isAnalysis")
    entity: Union[str, None] = None
    sensor_type: Union[str, None] = Field(None, alias="sensorType")
    locations: int
    location_ids: List[int] = Field(..., alias="locationIds")
    countries: List[str]
    parameters: List[Parameter]
    bbox: Union[List[float], None] = None
    measurements: int
    first_updated: str = Field(..., alias="firstUpdated")
    last_updated: str = Field(..., alias="lastUpdated")
    sources: List[ProjectsSource]


class ProjectsResponse(OpenAQResult):
    results: List[ProjectsRow]


# /v1/sources


class SourcesRowV1(BaseModel):
    url: str
    adapter: str
    name: str
    city: Union[str, None] = None
    country: str
    description: Union[str, None] = None
    source_url: AnyUrl = Field(..., alias="sourceURL")
    resolution: Union[str, None] = None
    contacts: List[str]
    active: bool


class SourcesResponseV1(OpenAQResult):
    results: List[SourcesRowV1]


# /v2/sources


class Datum(BaseModel):
    url: Union[str, None] = None
    data_avg_dur: Union[str, None] = None
    organization: Union[str, None] = None
    lifecycle_stage: Union[str, None] = None


class SourcesRow(BaseModel):
    data: Union[Datum, None] = None
    readme: Union[str, None] = None
    source_id: int = Field(..., alias="sourceId")
    locations: int
    source_name: str = Field(..., alias="sourceName")
    source_slug: Union[str, None] = Field(None, alias="sourceSlug")


class SourcesResponse(OpenAQResult):
    results: List[SourcesRow]


# /v2/summary


class SummaryRow(BaseModel):
    count: int
    cities: int
    sources: int
    countries: int
    locations: int


class SummaryResponse(OpenAQResult):
    results: List[SummaryRow]
