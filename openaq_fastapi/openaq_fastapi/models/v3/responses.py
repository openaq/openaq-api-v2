from typing import List, Union
from pydantic import BaseModel


class Country(BaseModel):
    id: int
    countryCode: str
    label: str


class Source(BaseModel):
    url: str
    name: str
    id: str
    readme: str
    organization: str
    lifecycle_stage: str


class Parameter(BaseModel):
    unit: str
    parameter: str
    displayName: str
    parameterId: int


class Coordinates(BaseModel):
    latitude: float
    longitude: float


class LocationsResponse(BaseModel):
    id: int
    city: str
    name: str
    entity: str
    country: Country
    sources: List[Source]
    isMobile: bool
    isAnalysis: bool
    parameters: List[Parameter]
    sensorType: str
    coordinates: Coordinates
    lastUpdated: str
    firstUpdated: str
    bounds: List[float]
    manufacturers: str


class Date(BaseModel):
    utc: str
    local: str


class Coordinates(BaseModel):
    latitude: float
    longitude: float


class MeasurementsResponse(BaseModel):
    parameter: str
    value: float
    date: Date
    unit: str
    coordinates: Union[Coordinates, None]
