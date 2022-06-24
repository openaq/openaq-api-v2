from datetime import date, datetime
from typing import List, Union

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
    url: Union[str, None] = None
    name: str
    id: Union[str, None] = None
    readme: Union[str, None] = None
    organization: Union[str, None] = None
    lifecycle_stage: Union[str, None] = None

class Manufacturer(BaseModel):
    model_name: str = Field(..., alias='modelName')
    manufacturer_name: str = Field(..., alias='manufacturerName')

class Parameter(BaseModel):
    id: int
    unit: str
    count: int
    average: float
    last_value: float = Field(..., alias='lastValue')
    parameter: str
    display_name: str = Field(..., alias='displayName')
    last_updated: str = Field(..., alias='lastUpdated')
    parameter_id: int = Field(..., alias='parameterId')
    first_updated: str = Field(..., alias='firstUpdated')
    manufacturers: Union[List[Manufacturer], None] = None


# Abstract class for all responses
class OpenAQResult(BaseModel):
    meta: Meta = Meta()
    results: List[Any] = []


# /v2/averages

class AveragesRow(BaseModel):
    id: Union[List[int], int]
    hour: Union[datetime, None]
    day: Union[date, None]
    month: Union[date, None]
    year: Union[date, None]
    hod: Union[int, None]
    dom: Union[int, None]
    name: Union[List[str],str]
    average: float
    name: Union[List[str],str]
    measurement_count: int # TODO make camelCase
    parameter: str
    parameter_id: int = Field(..., alias='parameterId')
    display_name: str = Field(..., alias='displayName')
    unit: Union[str, None]


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
    coordinates: Union[Coordinates, None]
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
    city: Union[str, None]
    name: str
    entity: str
    country: str
    sources: Union[List[Source], None]
    is_mobile: bool = Field(..., alias='isMobile')
    is_analysis: Union[bool, None] = Field(..., alias='isAnalysis')
    parameters: List[Parameter]
    sensor_type: str = Field(..., alias='sensorType')
    coordinates: Union[Coordinates, None] = None
    last_updated: str = Field(..., alias='lastUpdated')
    first_updated: str = Field(..., alias='firstUpdated')
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
    city: Union[str, None]
    is_mobile: bool = Field(..., alias='isMobile')
    is_analysis: Union[bool, None] = Field(..., alias='isAnalysis')
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
    is_core: Union[bool, None] = Field(..., alias='isCore')
    max_color_value: Union[float, None] # not camel case in output


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
    is_mobile: bool = Field(..., alias='isMobile')
    is_analysis: bool = Field(..., alias='isAnalysis')
    entity: Union[str, None]
    sensor_type: Union[str, None] = Field(..., alias='sensorType')
    locations: int
    location_ids: List[int] = Field(..., alias='locationIds')
    countries: List[str]
    parameters: List[Parameter]
    bbox: Union[List[float], None]
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
    city: Union[str, None] = None
    country: str
    description: Union[str, None] = None
    source_url: AnyUrl = Field(..., alias='sourceURL')
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
    data: Union[Datum, None]
    readme: Union[str, None]
    source_id: int = Field(..., alias='sourceId')
    locations: int
    source_name: str = Field(..., alias='sourceName')
    source_slug: Union[str, None] = Field(..., alias='sourceSlug')

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