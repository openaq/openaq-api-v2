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


# Abstract class for all responses
class OpenAQResult(BaseModel):
    meta: Meta = Meta()
    results: List[Any] = []


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
    measurement_count: int
    parameter: str
    parameterId: int
    displayName: str
    unit: Optional[str]


class OpenAQAveragesResult(OpenAQResult):
    results: List[AveragesRow]


class CitiesRow(BaseModel):
    country: Optional[str]
    city: str
    count: int
    locations: int
    firstUpdated: datetime
    lastUpdated: datetime
    parameters: List[str]


class OpenAQCitiesResult(OpenAQResult):
    results: List[CitiesRow]


class CountriesRow(BaseModel):
    code: str
    name: str
    locations: int
    firstUpdated: datetime
    lastUpdated: datetime
    parameters: List[str]
    count: int
    cities: int
    sources: int


class OpenAQCountriesResult(OpenAQResult):
    results: List[CountriesRow]







class ProjectParameterDetails(BaseModel):
    unit: str
    count: int
    average: float
    lastValue: float
    locations: int
    parameter: str
    lastUpdated: datetime
    firstUpdated: datetime
    parameterId: int
    displayName: Optional[str]


class ProjectsRow(BaseModel):
    id: int
    name: str
    subtitle: str
    isMobile: Optional[bool]
    isAnalysis: Optional[bool]
    entity: Optional[str]
    sensorType: Optional[str]
    locations: int
    locationIds: List[int]
    countries: Optional[List[str]]
    parameters: List[ProjectParameterDetails]
    bbox: Optional[List[float]]
    measurements: int
    firstUpdated: datetime
    lastUpdated: datetime
    sources: Optional[List[Any]]


class OpenAQProjectsResult(OpenAQResult):
    results: List[ProjectsRow]


class Source(BaseModel):
    id: str
    url: Optional[str] = None
    name: str


class Parameter(BaseModel):
    id: int
    unit: str
    count: int
    average: float
    lastValue: float
    parameter: str
    displayName: str
    lastUpdated: str
    parameterId: int
    firstUpdated: str


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




class ManufacturersResponse(OpenAQResult):
    results: List[str]


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


class ModelsResponse(OpenAQResult):
    results: List[str]


# /v2/parameters

class ParametersRow(BaseModel):
    id: int
    name: str
    displayName: str
    description: str
    preferredUnit: str
    isCore: Optional[bool]
    maxColorValue: Optional[Union[float, None]]


class ParametersResponse(OpenAQResult):
    results: List[ParametersRow]



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
    sourceURL: AnyUrl
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