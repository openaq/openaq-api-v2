import inspect
import logging
from datetime import date, datetime, timedelta
from enum import Enum
from types import FunctionType
from typing import Dict, List, Optional, Union

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

logger = logging.getLogger("base")
logger.setLevel(logging.DEBUG)

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
    city: Optional[List[str]] = Query(
        None,
        description="""
        Limit results by a certain city or cities.
        (ex. ?city=Chicago or ?city=Chicago&city=Boston)
        """,
    )


class Country(OBaseModel):
    country_id: Optional[str] = Query(
        None,
        min_length=2,
        max_length=2,
        regex="[a-zA-Z][a-zA-Z]",
        description="""
        Limit results by a certain country using two letter country code.
        (ex. /US)
        """,
    )
    country: Optional[List[str]] = Query(
        None,
        min_length=2,
        max_length=2,
        regex="[a-zA-Z][a-zA-Z]",
        description="""
        Limit results by a certain country using two letter country code.
        (ex. ?country=US or ?country=US&country=MX)
        """,
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
    sourceName: Optional[List[str]] = None
    sourceId: Optional[List[int]] = None
    sourceSlug: Optional[List[str]] = None


class EntityTypes(str, Enum):
    government = "government"
    community = "community"
    research = "research"


class SensorTypes(str, Enum):
    reference = "reference"
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
    project_id: Optional[int] = None
    project: Optional[List[Union[int, str]]] = Query(None, gt=0, le=maxint)

    @validator("project")
    def validate_project(cls, v, values):
        return id_or_name_validator("project", v, values)


class Location(OBaseModel):
    location_id: Optional[int] = None
    location: Optional[List[Union[int, str]]] = None

    @validator("location")
    def validate_location(cls, v, values):
        return id_or_name_validator("location", v, values)


class HasGeo(OBaseModel):
    has_geo: bool = None


class Geo(OBaseModel):
    coordinates: Optional[str] = Field(
        None, regex=r"^-?\d{1,2}\.?\d{0,8},-?1?\d{1,2}\.?\d{0,8}$"
    )
    lat: Optional[confloat(ge=-90, le=90)] = None
    lon: Optional[confloat(ge=-180, le=180)] = None
    radius: conint(gt=0, le=100000) = 1000

    @root_validator(pre=True)
    def addlatlon(cls, values):
        coords = values.get("coordinates", None)
        if coords is not None and "," in coords:
            lat, lon = coords.split(",")
            if lat and lon:
                values["lat"] = lat
                values["lon"] = lon
        return values

    def where_geo(self):
        if self.lat is not None and self.lon is not None:
            return (
                " st_dwithin(st_makepoint(:lon, :lat)::geography,"
                " geog, :radius) "
            )
        return None


class Measurands(OBaseModel):
    parameter_id: Optional[int] = None
    parameter: Optional[List[Union[int, str]]] = Query(None, gt=0, le=maxint)
    measurand: Optional[List[str]] = None
    unit: Optional[List[str]] = None

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
        description="Change the number of results returned.",
    )
    page: int = Query(
        1, gt=0, le=6000, description="Paginate through results."
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
    sort: Optional[Sort] = Query("asc", description="Define sort order.")


def fix_datetime(
    d: Union[datetime, date, str, int, None],
    minutes_to_round_to: Optional[int] = 1,
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
