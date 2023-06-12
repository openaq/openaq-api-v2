import inspect
import logging
from types import FunctionType, GenericAlias
from enum import Enum
from typing import (
    Dict,
    List,
    Union,
    Optional,
)
import weakref
import itertools

import humps
from fastapi import Query
from datetime import date, datetime
from pydantic import BaseModel, conint, confloat, root_validator
from inspect import signature
from fastapi.exceptions import ValidationError, HTTPException

logger = logging.getLogger("queries")

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
                    Query(field_model.default, description=field_info.description)
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


class TypeParametersMemoizer(type):
    """
    https://github.com/tiangolo/fastapi/issues/50#issuecomment-1267068112
    """

    _generics_cache = weakref.WeakValueDictionary()

    def __getitem__(cls, typeparams):
        # prevent duplication of generic types
        if typeparams in cls._generics_cache:
            return cls._generics_cache[typeparams]

        # middleware class for holding type parameters
        class TypeParamsWrapper(cls):
            __type_parameters__ = (
                typeparams if isinstance(typeparams, tuple) else (typeparams,)
            )

            @classmethod
            def _get_type_parameters(cls):
                return cls.__type_parameters__

        return GenericAlias(TypeParamsWrapper, typeparams)


class CommaSeparatedList(list, metaclass=TypeParametersMemoizer):
    """
    adapted from
    https://github.com/tiangolo/fastapi/issues/50#issuecomment-1267068112
    but reworked and simplified to only handled ints since we will only
    support comma chaining for id query parameters
    """

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v: List[Union[str, int]]):
        if v:
            if isinstance(v[0], str):
                v = [int(item) for x in v for item in x.split(",")]
            elif isinstance(v[0], int):
                pass
            else:
                v = list(itertools.chain.from_iterable((x.split(",") for x in v)))
            return v
        else:
            return v

    @classmethod
    def _get_type_parameters(cls):
        raise NotImplementedError("should be overridden in metaclass")


def make_dependable(cls):
    """
    https://github.com/tiangolo/fastapi/issues/1474#issuecomment-1160633178
    """

    def init_cls_and_handle_errors(*args, **kwargs):
        try:
            signature(init_cls_and_handle_errors).bind(*args, **kwargs)
            return cls(*args, **kwargs)
        except ValidationError as e:
            for error in e.errors():
                error["loc"] = ["query"] + list(error["loc"])
            raise HTTPException(422, detail=e.errors())

    init_cls_and_handle_errors.__signature__ = signature(cls)
    return init_cls_and_handle_errors


class QueryBuilder(object):
    def __init__(self, query):
        """
        take a query object which can have multiple
        QueryBaseModel ancestors
        """
        self.query = query

    def _bases(self) -> List[type]:
        bases = list(inspect.getmro(self.query.__class__))[
            :-4
        ]  # removes object primitives
        return bases

    def fields(self) -> str:
        """
        loops through all ancestor classes and calls
        their respective fields() methods to concatenate
        into additional fields for select
        """
        fields = []
        bases = self._bases()
        for base in bases:
            if callable(getattr(base, "fields", None)):
                if base.fields(self.query):
                    fields.append(base.fields(self.query))
        if len(fields):
            fields = list(set(fields))
            return "\n," + ("\n,").join(fields)
        else:
            return ""

    def pagination(self) -> str:
        pagination = []
        bases = self._bases()
        for base in bases:
            if callable(getattr(base, "pagination", None)):
                if base.pagination(self.query):
                    pagination.append(base.pagination(self.query))
        if len(pagination):
            pagination = list(set(pagination))
            return "\n" + ("\n,").join(pagination)
        else:
            return ""

    def params(self) -> dict:
        return self.query.dict(exclude_unset=True, by_alias=True)

    @staticmethod
    def total() -> str:
        return ", COUNT(1) OVER() as found"

    def where(self) -> str:
        """
        loops through all ancestor classes and calls
        their respective where() methods to concatenate
        into a full where statement
        """
        where = []
        bases = self._bases()
        for base in bases:
            if callable(getattr(base, "where", None)):
                if base.where(self.query):
                    where.append(base.where(self.query))
        if len(where):
            where = list(set(where))
            return "WHERE " + ("\nAND ").join(where)
        else:
            return ""


class QueryBaseModel(BaseModel):
    """
    # Using this to catch valididation errors that should be 422s
    https://github.com/tiangolo/fastapi/issues/318#issuecomment-1075020514
    """

    def __init__(self, **kwargs):
        try:
            super().__init__(**kwargs)
        except ValidationError as e:
            errors = e.errors()
            for error in errors:
                error["loc"] = ("query",) + error["loc"]
            raise HTTPException(422, detail=errors)

    class Config:
        min_anystr_length = 1
        validate_assignment = True
        allow_population_by_field_name = True
        alias_generator = humps.decamelize
        anystr_strip_whitespace = True

    @classmethod
    def depends(cls):
        return parameter_dependency_from_model("depends", cls)

    def has(self, field_name: str) -> bool:
        return hasattr(self, field_name) and getattr(self, field_name) is not None

    def where(self):
        return None

    def fields(self):  # for additional fields
        return None

    def pagination(self):
        return


# Thinking about how the paging should be done
# we should not let folks pass an offset if we also include
# a page parameter. And until pydantic supports computed
# values (v2) we have to calculate the offset ourselves
# see the db.py method
class Paging(BaseModel):
    limit: int = Query(
        100,
        gt=0,
        le=1000,
        description="""Change the number of results returned.
        e.g. limit=100 will return up to 100 results""",
        example="100",
    )
    page: int = Query(
        1,
        gt=0,
        description="Paginate through results. e.g. page=1 will return first page of results",
        example="1",
    )

    def pagination(self) -> str:
        return "LIMIT :limit OFFSET :offset"


class ParametersQuery(QueryBaseModel):
    parameters_id: Union[CommaSeparatedList[int], None] = Query(description="")

    def where(self) -> Union[str, None]:
        if self.has("parameters_id"):
            return "parameters_id = ANY (:parameters_id)"


class MobileQuery(QueryBaseModel):
    mobile: Union[bool, None] = Query(
        description="Is the location considered a mobile location?"
    )

    def where(self) -> Union[str, None]:
        if self.has("mobile"):
            return "ismobile = :mobile"


class MonitorQuery(QueryBaseModel):
    monitor: Union[bool, None] = Query(
        description="Is the location considered a reference monitor?"
    )

    def where(self) -> Union[str, None]:
        if self.has("monitor"):
            return "ismonitor = :monitor"


class ProviderQuery(QueryBaseModel):
    providers_id: Union[CommaSeparatedList[int], None] = Query(
        description="Limit the results to a specific provider"
    )

    def where(self) -> Union[str, None]:
        if self.has("providers_id"):
            return "(provider->'id')::int = ANY (:providers_id)"


class OwnerQuery(QueryBaseModel):
    owner_contacts_id: Union[CommaSeparatedList[int], None] = Query(
        description="Limit the results to a specific owner", ge=1
    )

    def where(self) -> Union[str, None]:
        if self.owner_contacts_id is not None:
            return "(owner->'id')::int = ANY (:owner_contacts_id)"


class CountryQuery(QueryBaseModel):
    """
    countries_id supports comma chaining but iso does not because it
    is a string parameter. countries_id is the preferred and more
    powerful query method
    """

    countries_id: Union[CommaSeparatedList[int], None] = Query(
        description="Limit the results to a specific country or countries",
    )
    iso: Union[str, None] = Query(
        description="Limit the results to a specific country using ISO code",
    )

    @root_validator(pre=True)
    def check_only_one(cls, values):
        countries_id = values.get("countries_id", None)
        iso = values.get("iso", None)
        if countries_id is not None and iso is not None:
            raise ValueError("Cannot pass both countries_id and iso code")
        return values

    def where(self) -> Union[str, None]:
        if self.countries_id is not None:
            return "(country->'id')::int = ANY (:countries_id)"
        elif self.iso is not None:
            return "country->>'code' = :iso"


class DateFromQuery(QueryBaseModel):
    date_from: Optional[Union[datetime, date]] = Query(
        "2022-10-01", description="From when?"
    )

    def where(self) -> str:
        if self.date_from is None:
            return None
        elif isinstance(self.date_from, datetime):
            if self.date_from.tzinfo is None:
                return "datetime > (:date_from::timestamp AT TIME ZONE tzid)"
            else:
                return "datetime > :date_from"
        elif isinstance(self.date_from, date):
            return "datetime > (:date_from::timestamp AT TIME ZONE tzid)"


class DateToQuery(QueryBaseModel):
    date_to: Optional[Union[datetime, date]] = Query(
        datetime.utcnow(), description="To when?"
    )

    def where(self) -> str:
        if self.date_to is None:
            return None
        elif isinstance(self.date_to, datetime):
            if self.date_to.tzinfo is None:
                return "datetime <= (:date_to::timestamp AT TIME ZONE tzid)"
            else:
                return "datetime <= :date_to"
        elif isinstance(self.date_to, date):
            return "datetime <= (:date_to::timestamp AT TIME ZONE tzid)"


class PeriodNames(str, Enum):
    hour = "hour"
    day = "day"
    month = "month"
    year = "year"
    hod = "hod"
    dow = "dow"
    moy = "moy"


class PeriodNameQuery(QueryBaseModel):
    period_name: Union[PeriodNames, None] = Query(
        "hour", description="Period to aggregate. Month, day, hour"
    )


# Some spatial helper queries
class RadiusQuery(QueryBaseModel):
    coordinates: Union[str, None] = Query(
        None,
        regex=r"^(-)?(?:90(?:\.0{1,4})?|((?:|[1-8])[0-9])(?:\.[0-9]{1,4})?)\,(-)?(?:180(?:\.0{1,4})?|((?:|[1-9]|1[0-7])[0-9])(?:\.[0-9]{1,4})?)$",
        description="Coordinate pair in form latitude,longitude. Up to 4 decimal points of precision e.g. 38.907,-77.037",
        example="38.907,-77.037",
    )
    radius: conint(gt=0, le=25000) = Query(
        None,
        description="Search radius from coordinates as center in meters. Maximum of 25,000 (25km) defaults to 1000 (1km) e.g. radius=1000",
        example="1000",
    )
    lat: Union[confloat(ge=-90, le=90), None] = None
    lon: Union[confloat(ge=-180, le=180), None] = None

    @root_validator(pre=True)
    def addlatlon(cls, values):
        coords = values.get("coordinates", None)
        if coords is not None and "," in coords:
            lat, lon = coords.split(",")
            if lat and lon:
                values["lat"] = float(lat)
                values["lon"] = float(lon)
        return values

    @root_validator(pre=True)
    def check_spatial_inputs(cls, values):
        bbox = values.get("bbox", None)
        coordinates = values.get("coordinates", None)
        radius = values.get("radius", None)
        if bbox is not None and (coordinates is not None or radius is not None):
            raise ValueError(
                "Cannot pass both bounding box and coordinate/radius query in the same URL"
            )
        if coordinates is not None and radius is None:
            raise ValueError("Coordinates must be passed with a radius")
        if coordinates is None and radius is not None:
            raise ValueError("Radius must be passed with a coordinate pair")
        return values

    def fields(self, geometry_field: str = "geog"):
        if self.lat is not None and self.lon is not None and self.radius is not None:
            return f"ST_Distance({geometry_field}, ST_MakePoint(:lon, :lat)::geography) as distance"

    def where(self, geometry_field: str = "geog"):
        if self.lat is not None and self.lon is not None and self.radius is not None:
            return f"ST_DWithin(ST_MakePoint(:lon, :lat)::geography, {geometry_field}, :radius)"
        return None


class BboxQuery(QueryBaseModel):
    bbox: Union[str, None] = Query(
        None,
        regex=r"^(-)?(?:180(?:\.0{1,4})?|((?:|[1-9]|1[0-7])[0-9])(?:\.[0-9]{1,4})?)\,(-)?(?:90(?:\.0{1,4})?|((?:|[1-8])[0-9])(?:\.[0-9]{1,4})?)\,(-)?(?:180(?:\.0{1,4})?|((?:|[1-9]|1[0-7])[0-9])(?:\.[0-9]{1,4})?)\,(-)?(?:90(?:\.0{1,4})?|((?:|[1-8])[0-9])(?:\.[0-9]{1,4})?)$",
        description="Min X, min Y, max X, max Y, up to 4 decimal points of precision e.g. -77.037,38.907,-77.0,39.910",
        example="-77.1200,38.7916,-76.9094,38.9955",
    )
    miny: Union[confloat(ge=-90, le=90), None] = None
    minx: Union[confloat(ge=-180, le=180), None] = None
    maxy: Union[confloat(ge=-90, le=90), None] = None
    maxx: Union[confloat(ge=-180, le=180), None] = None

    @root_validator(pre=True)
    def addminmax(cls, values):
        bbox = values.get("bbox", None)
        if bbox is not None and "," in bbox:
            minx, miny, maxx, maxy = bbox.split(",")
            if minx and miny and maxx and maxy:
                values["minx"] = float(minx)
                values["miny"] = float(miny)
                values["maxx"] = float(maxx)
                values["maxy"] = float(maxy)
        return values

    def where(self) -> Union[str, None]:
        if self.has("bbox"):
            return "st_makeenvelope(:minx, :miny, :maxx, :maxy, 4326) && geom"


class MeasurementsQueries(Paging, ParametersQuery):
    ...
