import inspect
import logging
from datetime import date, datetime, timedelta
from enum import Enum
from types import FunctionType
from typing import Dict, List, Union

import humps
from dateutil.parser import parse
from dateutil.tz import UTC
from fastapi import Query
from pydantic import (
    BaseModel,
    Field,
    confloat,
    conint,
    validator,
    root_validator,
)

logger = logging.getLogger("models")

maxint = 2147483647

ignore_in_docs = [
    "date_from_adj",
    "date_to_adj",
    "measurand",
    "lat",
    "lon",
]


def parameter_dependency_from_model(name: str, model_cls):
    """
    Takes a pydantic model class as input and creates
    a dependency with corresponding
    Query parameter definitions that can be used for GET
    requests.

    This will only work, if the fields defined in the
    input model can be turned into
    suitable query parameters. Otherwise fastapi
    will complain down the road.

    Arguments:
        name: Name for the dependency function.
        model_cls: A ``BaseModel`` inheriting model class as input.
    """
    names = []
    annotations: Dict[str, type] = {}
    defaults = []
    for field_model in model_cls.__fields__.values():
        if field_model.name not in ["self"]:
            field_info = field_model.field_info

            if field_model.name not in ignore_in_docs:
                names.append(field_model.name)
                annotations[field_model.name] = field_model.outer_type_
                defaults.append(
                    Query(
                        field_model.default, description=field_info.description
                    )
                )

    code = inspect.cleandoc(
        """
    def %s(%s):
        return %s(%s)
    """
        % (
            name,
            ", ".join(names),
            model_cls.__name__,
            ", ".join(["%s=%s" % (name, name) for name in names]),
        )
    )

    compiled = compile(code, "string", "exec")
    env = {model_cls.__name__: model_cls}
    env.update(**globals())
    func = FunctionType(compiled.co_consts[0], env, name)
    func.__annotations__ = annotations
    func.__defaults__ = (*defaults,)

    return func


class OBaseModel(BaseModel):
    class Config:
        min_anystr_length = 1
        validate_assignment = True
        allow_population_by_field_name = True
        alias_generator = humps.decamelize
        anystr_strip_whitespace = True

    @classmethod
    def depends(cls):
        logger.debug(f"Depends {cls}")
        return parameter_dependency_from_model("depends", cls)

    def params(self):
        return self.dict(exclude_unset=True, by_alias=True)


class City(OBaseModel):
    city: Union[List[str], None] = Query(
        None,
        description="Limit results by a certain city or cities. (e.g. ?city=Chicago or ?city=Chicago&city=Boston)",
        example="Chicago"
    )


class Country(OBaseModel):
    country_id: Union[str, None] = Query(
        None,
        min_length=2,
        max_length=2,
        regex="[a-zA-Z][a-zA-Z]",
        description="Limit results by a certain country using two letter country code. e.g. US",
        example="US"
    )
    country: Union[List[str],None] = Query(
        None,
        min_length=2,
        max_length=2,
        regex="[a-zA-Z][a-zA-Z]",
        description="Limit results by a certain country using two letter country code. e.g. ?country=US or ?country=US&country=MX",
        example="US"
    )

    @validator("country", check_fields=False)
    def validate_country(cls, v, values):
        logger.debug(f"validating countries {v} {values}")
        cid = values.get("country_id")
        if cid is not None:
            v = [cid]
        if v is not None:
            logger.debug(f"returning countries {v} {values}")
            return [str.upper(val) for val in v]
        return None


class SourceName(OBaseModel):
    sourceName: Union[List[str], None] = None
    sourceId: Union[List[int], None] = None
    sourceSlug: Union[List[str], None] = None


class EntityTypes(str, Enum):
    government = "government"
    community = "community"
    research = "research"


class SensorTypes(str, Enum):
    reference = "reference grade"
    lcs = "low-cost sensor"


def id_or_name_validator(name, v, values):
    ret = None
    logger.debug(f"validating {name} {v} {values}")
    id = values.get(f"{name}_id", None)
    if id is not None:
        ret = [id]
    elif v is not None:
        ret = v
    if isinstance(ret, list):
        if all(isinstance(x, int) for x in ret):
            logger.debug("everything is an int")
            if min(ret) < 1 or max(ret) > maxint:
                raise ValueError(
                    name,
                    f"{name}_id must be between 1 and {maxint}",
                )
    logger.debug(f"returning {ret}")
    return ret


class Project(OBaseModel):
    project_id: Union[int, None] = None
    project: Union[List[Union[int, str]], None] = Query(None, gt=0, le=maxint)

    @validator("project")
    def validate_project(cls, v, values):
        return id_or_name_validator("project", v, values)


class Location(OBaseModel):
    location_id: Union[int, None] = None
    location: Union[List[Union[int, str]], None] = None

    @validator("location")
    def validate_location(cls, v, values):
        return id_or_name_validator("location", v, values)


class HasGeo(OBaseModel):
    has_geo: bool = None


class Geo(OBaseModel):
    coordinates: Union[str, None] = Query(
        None,
        regex=r"^-?\d{1,2}\.?\d{0,8},-?1?\d{1,2}\.?\d{0,8}$",
        description="Coordinate pair in form lat,lng. Up to 8 decimal points of precision e.g. 38.907,-77.037",
        example="38.907,-77.037"
    )
    lat: Union[confloat(ge=-90, le=90), None] = None
    lon: Union[confloat(ge=-180, le=180), None] = None
    radius: conint(gt=0, le=100000) = Query(
        1000,
        description="Search radius from coordinates as center in meters. Maximum of 100,000 (100km) defaults to 1000 (1km) e.g. radius=10000",
        example="10000"
    )

    @root_validator(pre=True)
    def addlatlon(cls, values):
        coords = values.get("coordinates", None)
        if coords is not None and "," in coords:
            lat, lon = coords.split(",")
            if lat and lon:
                values["lat"] = float(lat)
                values["lon"] = float(lon)
        return values

    def where_geo(self):
        if self.lat is not None and self.lon is not None:
            return (
                " st_dwithin(st_makepoint(:lon, :lat)::geography,"
                " geog, :radius) "
            )
        return None


class Measurands(OBaseModel):
    parameter_id: Union[int, None] = Query(
        None,
        description="(optional) A parameter ID to filter measurement results. e.g. parameter_id=2 (i.e. PM2.5) will limit measurement results to only PM2.5 measurements",
        example="2"
    )
    parameter: Union[List[Union[int, str]], None] = Query(
        None,
        gt=0,
        le=maxint,
        description="(optional) A parameter name or ID by which to filter measurement results. e.g. parameter=pm25 or parameter=pm25&parameter=pm10",
        example="pm25"
    )
    measurand: Union[List[str], None] = Query(
        None,
        description=""
    )
    unit: Union[List[str], None] = Query(
        None,
        description="",
    )

    @validator("measurand", check_fields=False)
    def check_measurand(cls, v, values):
        if v is None:
            return values.get("parameter")
        return v

    @validator("parameter", check_fields=False)
    def validate_parameter(cls, v, values):
        if v is None:
            v = values.get("measurand")
        return id_or_name_validator("project", v, values)


class Paging(OBaseModel):
    limit: int = Query(
        100,
        gt=0,
        le=100000,
        description="Limit the number of results returned. e.g. limit=1000 will return up to 1000 results",
        example="1000"
    )
    page: int = Query(
        1,
        gt=0,
        le=6000,
        description="Paginate through results. e.g. page=1 will return first page of results",
        example="1"
    )
    offset: int = Query(0, ge=0, le=10000)

    @validator("offset", check_fields=False)
    def check_offset(cls, v, values, **kwargs):
        logger.debug("checking offset")
        offset = values["limit"] * (values["page"] - 1)
        if offset + values["limit"] > 100000:
            raise ValueError("offset + limit must be < 100000")
        return offset


class Sort(str, Enum):
    asc = "asc"
    desc = "desc"


class Spatial(str, Enum):
    country = "country"
    location = "location"
    project = "project"
    total = "total"


class Temporal(str, Enum):
    day = "day"
    month = "month"
    year = "year"
    moy = "moy"
    dow = "dow"
    hour = "hour"
    hod = "hod"


class APIBase(Paging):
    sort: Union[Sort, None] = Query(
        "asc",
        description="Define sort order. e.g. ?sort=asc",
        exmaple="asc"
    )


def fix_datetime(
    d: Union[datetime, date, str, int, None],
    minutes_to_round_to: Union[int, None] = 1,
):
    # Make sure that date/datetime is turned into timzone
    # aware datetime optionally rounding to
    # given number of minutes
    if d is None:
        d = datetime.utcnow()
    elif isinstance(d, str):
        d = parse(d)
    elif isinstance(d, int):
        d = datetime.fromtimestamp(d)
    elif isinstance(d, datetime):
        pass
    elif isinstance(d, date):
        d = datetime(
            *d.timetuple()[:-6],
        )
    else:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    if minutes_to_round_to is not None:
        d -= timedelta(
            minutes=d.minute % minutes_to_round_to,
            seconds=d.second,
            microseconds=d.microsecond,
        )
    return d


class DateRange(OBaseModel):
    date_from: Union[datetime, date, None] = fix_datetime("2000-01-01")
    date_to: Union[datetime, date, None] = fix_datetime(datetime.utcnow())
    date_from_adj: Union[datetime, date, None] = None
    date_to_adj: Union[datetime, date, None] = None

    @validator(
        "date_from",
        "date_to",
        "date_from_adj",
        "date_to_adj",
        check_fields=False,
    )
    def check_dates(cls, v, values):
        return fix_datetime(v)
