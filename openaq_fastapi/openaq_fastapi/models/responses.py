from datetime import date, datetime
from typing import List, Optional, Union

from pydantic import AnyUrl, Field
from pydantic.main import BaseModel
from pydantic.typing import Any
import orjson
from starlette.responses import JSONResponse
import logging

logger = logging.getLogger("responses")


def converter(meta, data, jq):
    ret = jq.input(data).all()
    ret_str = orjson.dumps(ret).decode()
    ret_str = str.replace(ret_str, "+00:00", "Z")
    out_data = orjson.loads(ret_str)
    output = {"meta": meta.dict(), "results": out_data}
    return JSONResponse(content=output)


class Meta(BaseModel):
    name: str = "openaq-api"
    license: str = "CC BY 4.0d"
    website: str = "/"
    page: int = 1
    limit: int = 100
    found: int = 0


class Date(BaseModel):
    utc: str
    local: str


class Coordinates(BaseModel):
    latitude: float
    longitude: float


class Source(BaseModel):
    id: str
    url: Optional[str] = None
    name: str


class Parameter(BaseModel):
    id: int
    unit: str
    count: int
    average: float
    last_value: float = Field(..., alias='lastValue')
    parameter: str
    display_name: str = Field(..., alias='displayName')
    first_updated: str = Field(..., alias='firstUpdated')
    last_updated: str = Field(..., alias='lastUpdated')
    parameter_id: int = Field(..., alias='parameterId')

# Abstract class for all responses
class OpenAQResult(BaseModel):
    meta: Meta = Meta()
    results: List[Any] = []


# /v2/averages

class AveragesRow(BaseModel):
    id: int
    hour: Optional[datetime]
    day: Optional[datetime]
    month: Optional[date]
    year: Optional[date]
    hod: Optional[int]
    dom: Optional[int]
    name: str
    average: float
    subtitle: str
    measurement_count: int # TODO make camelCase
    parameter: str
    parameter_id: int = Field(..., alias='parameterId')
    display_name: str = Field(..., alias='displayName')
    unit: Optional[str]


class AveragesResponse(OpenAQResult):
    results: List[AveragesRow]


# /v2/countries
 
class CountriesRow(BaseModel):
    code: str
    name: str
    locations: int
    first_updated: str = Field(..., alias='firstUpdated')
    last_updated: str = Field(..., alias='lastUpdated')
    parameters: List[str]
    count: int
    cities: int
    sources: int

class CountriesResponse(OpenAQResult):
    results: List[CountriesRow]

# /v2/cities

class CityRow(BaseModel):
    country: str
    city: str
    count: int
    locations: int
    first_updated: str = Field(..., alias='firstUpdated')
    last_updated: str = Field(..., alias='lastUpdated')
    parameters: List[str] 

class CitiesResponse(OpenAQResult):
    results: List[CityRow]


# /v1/latest

class AveragingPeriodV1(BaseModel):
    value: int
    unit: str


class LatestMeasurementRow(BaseModel):
    parameter: str
    value: float
    last_updated: str = Field(..., alias='lastUpdated')
    unit: str
    source_name: str = Field(..., alias='sourceName')
    averaging_period: AveragingPeriodV1 = Field(..., alias='averagingPeriod')


class LatestRowV1(BaseModel):
    location: str
    city: str
    country: str
    coordinates: Coordinates
    measurements: List[LatestMeasurementRow]

class LatestResponseV1(OpenAQResult):
    results: List[LatestRowV1]


# /v2/latest 

class LatestMeasurement(BaseModel):
    parameter: str
    value: float
    last_updated: str = Field(..., alias='lastUpdated')
    unit: str


class LatestRow(BaseModel):
    location: str
    city: str
    country: str
    coordinates: Coordinates
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
    city: str
    cities: List[str]
    location: str
    locations: List[str]
    source_name: str = Field(..., alias='sourceName')
    source_names: List[str] = Field(..., alias='sourceNames')
    source_type: str = Field(..., alias='sourceType')
    source_types: List[str] = Field(..., alias='sourceTypes')
    coordinates: Coordinates
    first_updated: str = Field(..., alias='firstUpdated')
    last_updated: str = Field(..., alias='lastUpdated')
    parameters: List[str]
    counts_by_measurement: List[CountsByMeasurementItem] = Field(..., alias='countsByMeasurement')
    count: int

class LocationsResponseV1(OpenAQResult):
    results: List[LocationsRowV1]

# /v2/locations

class LocationsRow(BaseModel):
    id: int
    city: str
    name: str
    entity: str
    country: str
    sources: List[Source]
    is_mobile: bool = Field(..., alias='isMobile')
    is_analysis: bool = Field(..., alias='isAnalysis')
    parameters: List[Parameter]
    sensor_type: str = Field(..., alias='sensorType')
    coordinates: Coordinates
    last_updated: str = Field(..., alias='lastUpdated')
    first_updated: str = Field(..., alias='firstUpdated')
    measurements: int


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
    country: str
    city: str


class MeasurementsResponseV1(OpenAQResult):
    results: List[MeasurementsRowV1]


# /v2/measurements

class MeasurementsRow(BaseModel):
    location_id: int = Field(..., alias='locationId')
    location: str
    parameter: str
    value: float
    date: Date
    unit: str
    coordinates: Coordinates
    country: str
    city: str
    is_mobile: bool = Field(..., alias='isMobile')
    is_analysis: bool = Field(..., alias='isAnalysis')
    entity: str
    sensor_type: str = Field(..., alias='sensorType')


class MeasurementsResponse(OpenAQResult):
    results: List[MeasurementsRow]

# /v2/parameters

class ModelsResponse(OpenAQResult):
    results: List[str]


# /v2/parameters

class ParametersRow(BaseModel):
    id: int
    name: str
    display_name: str = Field(..., alias='displayName')
    description: str
    preferred_unit: str = Field(..., alias='preferredUnit')
    is_core: Optional[bool] = Field(..., alias='isCore')
    max_color_value: Optional[Union[float, None]] = Field(..., alias='maxColorValue')


class ParametersResponse(OpenAQResult):
    results: List[ParametersRow]



# /v2/projects

# TODO convert fields to camelCase

class ProjectsSource(BaseModel):
    id: str
    name: str
    readme: Optional[str] = None
    data_avg_dur: Optional[str] = None
    organization: Optional[str] = None
    lifecycle_stage: Optional[str] = None


class ProjectsRow(BaseModel):
    id: int
    name: str
    subtitle: str
    is_mobile: bool = Field(..., alias='isMobile')
    is_analysis: bool = Field(..., alias='isAnalysis')
    entity: Optional[str]
    sensor_type: Optional[str] = Field(..., alias='sensorType')
    locations: int
    location_ids: List[int] = Field(..., alias='locationIds')
    countries: List[str]
    parameters: List[Parameter]
    bbox: Optional[List[float]]
    measurements: int
    first_updated: str = Field(..., alias='firstUpdated')
    last_updated: str  = Field(..., alias='lastUpdated')
    sources: List[ProjectsSource]

class ProjectsResponse(OpenAQResult):
    results: List[ProjectsRow]


# /v1/sources


class SourcesRowV1(BaseModel):
    url: str
    adapter: str
    name: str
    city: Optional[str] = None
    country: str
    description: Optional[str] = None
    source_url: AnyUrl = Field(..., alias='sourceURL')
    resolution: Optional[str] = None
    contacts: List[str]
    active: bool


class SourcesResponseV1(OpenAQResult):
    results: List[SourcesRowV1]

# /v2/sources


class SourcesRow(BaseModel):
    url: AnyUrl
    name: str
    count: int
    active: bool
    adapter: str
    country: str
    contacts: List[str]
    locations: int
    source_url: AnyUrl = Field(..., alias='sourceURL')
    parameters: List[str]
    description: str
    lastUpdated: str
    firstUpdated: str


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