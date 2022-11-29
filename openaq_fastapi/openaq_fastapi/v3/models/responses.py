from typing import List, Union
from pydantic import BaseModel
from pydantic.typing import Any


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


class CountryBase(BaseModel):
    id: Union[int, None]
    code: str
    name: str


class ParameterBase(BaseModel):
    id: int
    name: str
    units: str


class Parameter(ParameterBase):
    id: int
    name: str
    displayName: str
    description: str
    units: str


class Country(CountryBase):
    id: int
    code: str
    name: str
    locationsCount: int
    firstDatetime: str
    lastDatetime: str
    parameters: List[ParameterBase]
    meaurementsCount: int
    citiesCount: int
    providersCount: int


class Contact(BaseModel):
    id: int
    name: str


class Instrument(BaseModel):
    id: int
    name: str
    manufacturer: Contact


class Source(BaseModel):
    url: str
    name: str
    id: str
    readme: str
    organization: str
    lifecycle_stage: str


class Coordinates(BaseModel):
    latitude: Union[float, None]
    longitude: Union[float, None]


class Location(BaseModel):
    id: int
    city: Union[str, None]
    name: str
    country: CountryBase
    owner: Contact
    # provider: Contact
    isMobile: bool
    isMonitor: Union[bool, None]
    instruments: List[Instrument]
    parameters: List[ParameterBase]
    #sensorType: str
    coordinates: Coordinates
    # lastUpdated: str
    # firstUpdated: str
    # bounds: List[float]


class Date(BaseModel):
    utc: str
    local: str


class Coordinates(BaseModel):
    latitude: float
    longitude: float


class LocationsResponse(OpenAQResult):
    results: List[Location]


class MeasurementsResponse(BaseModel):
    parameter: str
    value: float
    date: Date
    unit: str
    coordinates: Union[Coordinates, None]


class CountriesResponse(OpenAQResult):
    results: List[Country]
