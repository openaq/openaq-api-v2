from datetime import date, datetime
from typing import List, Optional, Union

from pydantic import AnyUrl
from pydantic.main import BaseModel
from pydantic.typing import Any
import jq
import orjson
from starlette.responses import JSONResponse
import logging

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)


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


class OpenAQResult(BaseModel):
    meta: Meta = Meta()
    results: List[Any] = []


class CoordinatesDict(BaseModel):
    latitude: Optional[float]
    longitude: Optional[float]


class DateDict(BaseModel):
    utc: datetime
    local: datetime


class MeasurementsRow(BaseModel):
    locationId: int
    location: str
    parameter: str
    date: DateDict
    unit: str
    coordinates: CoordinatesDict
    country: Optional[str]
    city: Optional[str]
    isMobile: bool


class OpenAQMeasurementsResult(OpenAQResult):
    results: List[MeasurementsRow]


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


class OpenAQSourcesResult(OpenAQResult):
    results: List[SourcesRow]


class ParametersRow(BaseModel):
    id: int
    name: str
    displayName: str
    description: str
    preferredUnit: str
    isCore: Optional[bool]
    maxColorValue: Optional[Union[float, None]]


class OpenAQParametersResult(OpenAQResult):
    results: List[ParametersRow]


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


class SourceDetails(BaseModel):
    url: str
    city: str
    name: str
    active: bool
    adapter: str
    country: str
    contacts: List[str]
    sourceURL: AnyUrl
    description: str


class LocationParameterDetails(BaseModel):
    id: int
    unit: str
    count: int
    average: float
    lastValue: float
    measurand: str
    lastUpdated: datetime
    firstUpdated: datetime
    displayName: str


class LocationsRow(BaseModel):
    id: int
    city: Optional[str]
    name: str
    country: Optional[str]
    sources: List[Any]
    isMobile: bool
    isAnalysis: bool
    parameters: List[LocationParameterDetails]
    sourceType: str
    coordinates: CoordinatesDict
    lastUpdated: datetime
    firstUpdated: datetime
    measurements: int
    rawData: Optional[Any]


class OpenAQLocationsResult(OpenAQResult):
    results: List[LocationsRow]


class MeasurementDetails(BaseModel):
    parameter: str
    value: float
    lastUpdated: str
    unit: str


class LatestRow(BaseModel):
    location: str
    city: str
    country: str
    coordinates: CoordinatesDict
    measurements: List[MeasurementDetails]


class OpenAQLatestResult(OpenAQResult):
    results: List[LatestRow]
