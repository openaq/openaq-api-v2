import inspect
import itertools
import logging
import operator
import types
import weakref
from datetime import date, datetime
from enum import StrEnum, auto
from types import FunctionType
from typing import Annotated, Any
from abc import ABC

import fastapi
import humps
from annotated_types import Interval
from fastapi import Path, Query
from fastapi.exceptions import HTTPException
from pydantic import (
    BaseModel,
    ConfigDict,
    GetCoreSchemaHandler,
    TypeAdapter,
    ValidationError,
    computed_field,
    field_validator,
    model_validator,
)
from pydantic_core import CoreSchema, core_schema


logger = logging.getLogger("queries")

maxint = 2147483647

ignore_in_docs = [
    "date_from_adj",
    "date_to_adj",
    "measurand",
    "lat",
    "lon",
]


def truncate_float(value: float, length: int = 4) -> float:
    """Truncates a float to a given number of decimal points

    Args:
        value: The float value to truncate
        length: the number of decimal point to truncate to. Defaults to 4.

    Returns:
        The float value truncated
    """
    parts = str(float(value)).split(".")
    return float(".".join([parts[0], parts[1][:length]]))


def parameter_dependency_from_model(name: str, model_class):
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
    annotations: dict[str, type] = {}
    defaults = []
    for field_model in model_class.model_fields.values():
        if field_model.alias not in ["self"]:
            if field_model.alias not in ignore_in_docs:
                names.append(field_model.alias)
                annotations[field_model.alias] = field_model.annotation
                if isinstance(field_model, fastapi.params.Path):
                    defaults.append(Path(description=field_model.description))
                if isinstance(field_model, fastapi.params.Query):
                    defaults.append(
                        Query(
                            field_model.default,
                            description=field_model.description,
                            examples=field_model.examples,
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
            model_class.__name__,
            ", ".join(["%s=%s" % (name, name) for name in names]),
        )
    )

    compiled = compile(code, "string", "exec")
    env = {model_class.__name__: model_class}
    env.update(**globals())
    func = FunctionType(compiled.co_consts[0], env, name)
    func.__annotations__ = annotations
    func.__defaults__ = (*defaults,)

    return func


class TypeParametersMemoizer(type):
    """
    adapted from https://github.com/tiangolo/fastapi/discussions/8225
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
            __type_adapter__ = TypeAdapter(list[__type_parameters__])

            @classmethod
            def _get_type_parameters(cls):
                return cls.__type_parameters__

            @classmethod
            def _get_type_adapter(cls):
                return cls.__type_adapter__

        wrapper = cls._generics_cache[typeparams] = types.GenericAlias(
            TypeParamsWrapper, typeparams
        )
        return wrapper


class CommaSeparatedList(list, metaclass=TypeParametersMemoizer):
    """
    adapted from https://github.com/tiangolo/fastapi/discussions/8225
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls, _source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_before_validator_function(
            cls.validate, handler(list[cls._get_type_parameters()])
        )

    @classmethod
    def validate(cls, v: Any):
        adapter = cls._get_type_adapter()
        if isinstance(v, str):
            v = list(map(str.strip, v.split(",")))
        elif isinstance(v, list) and all(isinstance(x, str) for x in v):
            v = list(
                map(str.strip, itertools.chain.from_iterable(x.split(",") for x in v))
            )
        return adapter.validate_python(v)

    @classmethod
    def _get_type_parameters(cls):
        raise NotImplementedError("should be overridden in metaclass")

    @classmethod
    def _get_type_adapter(cls) -> TypeAdapter:
        raise NotImplementedError("should be overridden in metaclass")


class QueryBaseModel(ABC, BaseModel):
    """Base class for building query objects.

    All query objects should inherit this model and can implement
    the `where`, `fields` and `pagination` methods


    """

    def __init__(self, **kwargs):
        """
        https://github.com/tiangolo/fastapi/issues/318#issuecomment-1075020514

        Args:
             **kwargs: Arbitrary keyword arguments.
        """
        try:
            super().__init__(**kwargs)
        except ValidationError as e:
            errors = e.errors()
            for error in errors:
                error["loc"] = ("query",) + error["loc"]
            raise HTTPException(422, detail=errors)

    model_config = ConfigDict(
        str_min_length=1,
        validate_assignment=True,
        populate_by_name=True,
        alias_generator=humps.decamelize,
        str_strip_whitespace=True,
    )

    @classmethod
    def depends(cls):
        """ """
        return parameter_dependency_from_model("depends", cls)

    def has(self, field_name: str) -> bool:
        """ """
        return hasattr(self, field_name) and getattr(self, field_name) is not None

    def where(self):
        """abstract method for returning the SQL WHERE clause."""
        ...

    def fields(self):
        """abstract method for returning the fields for SQL prepared statement."""
        ...

    def pagination(self):
        """abstract method for"""
        ...


class SortOrder(StrEnum):
    ASC = "asc"
    DESC = "desc"


class SortingBase(ABC, BaseModel):
    order_by: str
    sort_order: SortOrder | None = Query(
        SortOrder.ASC,
        description="Sort results ascending or descending. Default ASC",
        examples=["sort=desc"],
    )


# Thinking about how the paging should be done
# we should not let folks pass an offset if we also include
# a page parameter. And until pydantic supports computed
# values (v2) we have to calculate the offset ourselves
# see the db.py method
class Paging(QueryBaseModel):
    limit: int = Query(
        100,
        gt=0,
        le=1000,
        description="""Change the number of results returned.
        e.g. limit=100 will return up to 100 results""",
        examples=["100"],
    )
    page: int = Query(
        1,
        gt=0,
        description="Paginate through results. e.g. page=1 will return first page of results",
        examples=["1"],
    )

    def pagination(self) -> str:
        return "LIMIT :limit OFFSET :offset"


class ParametersQuery(QueryBaseModel):
    """Pydantic query model for the parameters query parameter

    Inherits from QueryBaseModel

    Attributes:
        parameters_id: parameters_id or comma separated list of parameters_id
            for filtering results to a parameter or parameeters
    """

    parameters_id: CommaSeparatedList[int] | None = Query(None, description="")

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def where(self) -> str | None:
        """ """
        if self.has("parameters_id"):
            return "parameter_ids && :parameters_id"


class ManufacturersQuery(QueryBaseModel):
    """Pydantic query model for the manufacturers_id query parameter

    Inherits from QueryBaseModel

    Attributes:
        manufacturers_id: manufacturers_id or comma separated list of manufacturers_id
            for filtering results to a manufacturer or manufacturers
    """

    manufacturers_id: CommaSeparatedList[int] | None = Query(None, description="")

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def where(self) -> str | None:
        """ """
        if self.has("manufacturers_id"):
            return "manufacturer_ids && :manufacturers_id"


class MobileQuery(QueryBaseModel):
    """Pydantic query model for the `mobile` query parameter

    Inherits from QueryBaseModel

    Attributes:
        mobile: boolean for filtering whether to include mobile monitoring
            locations
    """

    mobile: bool | None = Query(
        None, description="Is the location considered a mobile location?"
    )

    def where(self) -> str | None:
        """ """
        if self.has("mobile"):
            return "ismobile = :mobile"


class MonitorQuery(QueryBaseModel):
    """Pydantic query model for the `monitor` query parameter

    Inherits from QueryBaseModel

    Attributes:
        monitor: boolean for filtering whether to include reference monitors
            locations. True indicates only reference monitors, False indicates
            exclude reference monitors
    """

    monitor: bool | None = Query(
        None, description="Is the location considered a reference monitor?"
    )

    def where(self) -> str | None:
        """ """
        if self.has("monitor"):
            return "ismonitor = :monitor"


class ProviderQuery(QueryBaseModel):
    """Pydantic query model for the `providers_id` query parameter

    Inherits from QueryBaseModel

    Attributes:
        providers_id: providers_id or comma separated list of providers_id
            for filtering results to a provider or providers
    """

    providers_id: CommaSeparatedList[int] | None = Query(
        None,
        description="Limit the results to a specific provider or multiple providers  with a single provider ID or a comma delimited list of IDs",
        examples=["1", "1,2,3"],
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def where(self) -> str | None:
        """ """
        if self.has("providers_id"):
            return "(provider->'id')::int = ANY (:providers_id)"


class OwnerQuery(QueryBaseModel):
    """Pydantic query model for the `owner_contacts_id` query parameter

    Inherits from QueryBaseModel

    Attributes:
        owner_contacts_id: owner_contacts_id or comma separated list of
            owner_contacts_id for filtering results to a provider or providers
    """

    owner_contacts_id: CommaSeparatedList[int] | None = Query(
        None,
        description="Limit the results to a specific owner by owner ID with a single owner ID or comma delimited list of IDs",
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def where(self) -> str | None:
        """ """
        if self.owner_contacts_id is not None:
            return "(owner->'id')::int = ANY (:owner_contacts_id)"


class CountryIdQuery(QueryBaseModel):
    """Pydantic query model for the `countries_id` query parameter

    Inherits from QueryBaseModel

    Attributes:
        countries_id: countries_id or comma separated list of countries_id for
        filtering results to a country or countries
    """

    countries_id: CommaSeparatedList[int] | None = Query(
        None,
        description="Limit the results to a specific country or countries by country ID as a single country ID or a comma delimited list of IDs",
        examples=["1", "1,2,3"],
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def where(self) -> str | None:
        """ """
        if self.countries_id is not None:
            return "(country->'id')::int = ANY (:countries_id)"


class CountryIsoQuery(QueryBaseModel):
    """Pydantic query model for the `ise` query parameter.

    Inherits from QueryBaseModel

    Attributes:
        iso: ISO 3166-1 alpha-2 code for filtering to a single country ISO.
    """

    iso: str | None = Query(
        None,
        description="Limit the results to a specific country using ISO 3166-1 alpha-2 code",
        examples=["US"],
    )

    @model_validator(mode="before")
    @classmethod
    def check_only_one(cls, values):
        countries_id = values.get("countries_id", None)
        iso = values.get("iso", None)
        if countries_id is not None and iso is not None:
            raise ValueError("Cannot pass both countries_id and iso code")
        return values

    def where(self) -> str | None:
        """Generates SQL condition for filtering to ISO country code.

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause if `date_from` is set
        """
        if self.iso is not None:
            return "country->>'code' = :iso"


class DateFromQuery(QueryBaseModel):
    """Pydantic query model for the `date_from` query parameter

    Inherits from QueryBaseModel

    Attributes:
        date_from: date or datetime in ISO-8601 format to filter results to a
        date range.
    """

    date_from: datetime | date | None = Query(
        None,
        description="From when?",
        examples=["2022-10-01T11:19:38-06:00", "2022-10-01"],
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to datetime.

        Overrides the base QueryBaseModel `where` method

        If `date_from` is a `date` or `datetime` without a timezone a timezone
        is added as UTC.

        Returns:
            string of WHERE clause if `date_from` is set
        """

        if self.date_from is None:
            return None
        elif isinstance(self.date_from, datetime):
            if self.date_from.tzinfo is None:
                return "datetime > (:date_from::timestamp AT TIME ZONE timezone)"
            else:
                return "datetime > :date_from"
        elif isinstance(self.date_from, date):
            return "datetime > (:date_from::timestamp AT TIME ZONE timezone)"


class DateToQuery(QueryBaseModel):
    """Pydantic query model for the `date_to` query parameter

    Inherits from QueryBaseModel

    Attributes:
        date_to: date or datetime in ISO-8601 format to filter results to a
        date range.
    """

    date_to: datetime | date | None = Query(
        None,
        description="To when?",
        examples=["2022-10-01T11:19:38-06:00", "2022-10-01"],
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to datetime.

        Overrides the base QueryBaseModel `where` method

        If `date_to` is a `date` or `datetime` without a timezone a timezone
        is added as UTC.

        Returns:
            string of WHERE clause if `date_to` is set
        """
        if self.date_to is None:
            return None
        elif isinstance(self.date_to, datetime):
            if self.date_to.tzinfo is None:
                return "datetime <= (:date_to::timestamp AT TIME ZONE timezone)"
            else:
                return "datetime <= :date_to"
        elif isinstance(self.date_to, date):
            return "datetime <= (:date_to::timestamp AT TIME ZONE timezone)"


class PeriodNames(StrEnum):
    hour = "hour"
    day = "day"
    month = "month"
    year = "year"
    hod = "hod"
    dow = "dow"
    moy = "moy"
    raw = "raw"


class PeriodNameQuery(QueryBaseModel):
    """Pydantic query model for the `period_name` query parameter.

    Inherits from QueryBaseModel

    Attributes:
        period_name: value of period to aggregate measurement values.
    """

    period_name: PeriodNames | None = Query(
        "hour",
        description="Period to aggregate. Month, day, hour, hour of day (hod), day of week (dow) and month of year (moy)",
    )


class TemporalQuery(QueryBaseModel):
    """Pydantic query model for the `period_name` query parameter.

    Inherits from QueryBaseModel

    Attributes:
        period_name: value of period to aggregate measurement values.
    """

    temporal: PeriodNames | None = Query(
        "hour", description="Period to aggregate. month, day, hour"
    )


class RadiusQuery(QueryBaseModel):
    """Pydantic query model for the `period_name` query parameter.

    Inherits from QueryBaseModel

    Attributes:
        coordinates: comma separated WGS84 latitude longitude pair.
        radius: an integer value representing the search radius in meters from
            `coordinates`.
    """

    coordinates: str | None = Query(
        None,
        description="WGS 84 Coordinate pair in form latitude,longitude. Supports up to 4 decimal points of precision, additional decimal precision will be truncated in the query e.g. 38.9074,-77.0373",
        examples=["38.907,-77.037"],
    )
    radius: Annotated[int, Interval(le=25000, gt=0)] | None = Query(
        None,
        description="Search radius from coordinates as center in meters. Maximum of 25,000 (25km) defaults to 1000 (1km) e.g. radius=1000",
        examples=["1000"],
    )

    @computed_field(return_type=float | None)
    @property
    def lat(self) -> float | None:
        """Splits `coordinates` into a float representing WGS84 latitude.

        Truncates float to 4 decimal places
        """
        if self.coordinates:
            lat, _ = self.coordinates.split(",")
            return truncate_float(float(lat))

    @computed_field(return_type=float | None)
    @property
    def lon(self) -> float | None:
        """Splits `coordinates` into a float representing WGS84 longitude.

        Truncates float to 4 decimal places
        """
        if self.coordinates:
            _, lon = self.coordinates.split(",")
            return truncate_float(float(lon))

    @field_validator("coordinates")
    def validate_coordinates(cls, v):
        """Vadidates that coordinates are within range [-180,180],[-90,90].

        Raises:
            ValueError: if `coordinates` x value is outside range [-180, 180] or
            or if y value is outside range [-90,90]
        """
        if v:
            errors = []
            lat, lng = [float(x) for x in v.split(",")]
            if lat > 90 or lat < -90:
                errors.append("foo")
            if lng > 180 or lng < -180:
                errors.append("foo")
            if errors:
                raise ValueError(f"Invalid coordinates. Error(s): {' '.join(errors)}")
        return v

    @model_validator(mode="before")
    @classmethod
    def check_spatial_inputs(cls, values):
        """Checks that spatial query parameters are correctly set

        Checks that `radius` and `coordinates` are set together. Ensures that if
        `radius`/`coordinates` is set `bbox` is not also set.

        Raises:
            ValueError: if `bbox` is set and `coordinates` and `radius` are
            set or if `coordinates` is set but `radius` is not set or if
            `coordinates` is not set but `radius` is set.
        """
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

    def fields(self, geometry_field: str = "geog") -> str | None:
        """

        Args:
            geometry_field:

        Returns:
            SQL string representing fields
        """
        if self.radius and self.coordinates:
            return f"ST_Distance({geometry_field}, ST_MakePoint(:lon, :lat)::geography) as distance"

    def where(self, geometry_field: str = "geog") -> str | None:
        """Generates SQL condition for filtering to ISO country code.

        Overrides the base QueryBaseModel `where` method

        Args:
            geometry_field:

        Returns:
            string of WHERE clause if `date_from` is set
        """
        if self.radius and self.coordinates:
            return f"ST_DWithin(ST_MakePoint(:lon, :lat)::geography, {geometry_field}, :radius)"


class BboxQuery(QueryBaseModel):
    """Pydantic query model for the `bbox` query parameter.

    Inherits from QueryBaseModel

    Attributes:
        bbox:
    """

    bbox: str | None = Query(
        None,
        pattern=r"-?\d{1,3}\.?\d*,-?\d{1,2}\.?\d*,-?\d{1,3}\.?\d*,-?\d{1,2}\.?\d*",
        description="geospatial bounding box of Min X, min Y, max X, max Y in WGS 84 coordinates. Up to 4 decimal points of precision, addtional decimal precision will be truncated to 4 decimal points precision e.g. -77.037,38.907,-77.0,39.910",
        examples=["-77.1200,38.7916,-76.9094,38.9955"],
    )

    @field_validator("bbox")
    def validate_bbox_in_range(cls, v):
        """Validates `bbox` values are in correct order and within range.

        Raises:
            ValueError: if `bbox` is not in correct order or values fall outside
            coordinate range.
        """
        if v:
            errors = []
            bbox = [float(x) for x in v.split(",")]
            minx, miny, maxx, maxy = bbox
            if not (minx >= -180 and minx <= 180):
                errors.append("X min must be between -180 and 180")
            if not (miny >= -90 and miny <= 90):
                errors.append("Y min must be between -90 and 90")
            if not (maxx >= -180 and maxx <= 180):
                errors.append("X max must be between -180 and 180")
            if not (maxy >= -90 and maxy <= 90):
                errors.append("Y max must be between -90 and 90")
            if minx > maxx:
                errors.append("X max must be greater than or equal to X min")
            if miny > maxy:
                errors.append("Y max must be greater than or equal to Y min")
            if errors:
                raise ValueError("Invalid bounding box. Error(s): " + " ".join(errors))
        return v

    @computed_field(return_type=float | None)
    @property
    def minx(self) -> float | None:
        """Splits `bbox` into a float representing minimum x value.

        Truncates float to 4 decimal places
        """
        if self.bbox:
            return truncate_float(float(self.bbox.split(",")[0]))

    @computed_field(return_type=float | None)
    @property
    def miny(self) -> float | None:
        """Splits `bbox` into a float representing minimum y value.

        Truncates float to 4 decimal places
        """
        if self.bbox:
            return truncate_float(float(self.bbox.split(",")[1]))

    @computed_field(return_type=float | None)
    @property
    def maxx(self) -> float | None:
        """Splits `bbox` into a float representing maximum x value.

        Truncates float to 4 decimal places
        """
        if self.bbox:
            return truncate_float(float(self.bbox.split(",")[2]))

    @computed_field(return_type=float | None)
    @property
    def maxy(self) -> float | None:
        """Splits `bbox` into a float representing maximum y value.

        Truncates float to 4 decimal places
        """
        if self.bbox:
            return truncate_float(float(self.bbox.split(",")[3]))

    def where(self) -> str | None:
        """Generates SQL condition for filtering to a bounding box.

        Overrides the base QueryBaseModel `where` method.

        Returns:
            string of WHERE clause if `bbox` is set.
        """
        if self.bbox:
            return "ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326) && geom"


class MeasurementsQueries(Paging, ParametersQuery): ...


class QueryBuilder(object):
    """A utility class to wrap multiple QueryBaseModel classes"""

    def __init__(self, query: type):
        """
        Args:
             query: a class which inherits from one or more pydantic query
             models, QueryBaseModel.
        """
        self.query = query
        self.sort_field = False

    def _bases(self) -> list[type]:
        """inspects the object and returns base classes

        Removes primitive objects in ancestry to only include Pydantic Query
        and Path models

        Returns:
            a sorted list of base classes
        """
        base_classes = inspect.getmro(self.query.__class__)
        bases = [
            x for x in base_classes if not ABC in x.__bases__
        ]  # remove all abstract classes
        bases.remove(object)  # remove <class 'object'>
        bases.remove(ABC)  # <class 'ABC'>
        bases.remove(BaseModel)  # <class 'pydantic.main.BaseModel'>
        bases_sorted = sorted(
            bases, key=operator.attrgetter("__name__")
        )  # sort to ensure consistent order for reliability in testing
        return bases_sorted

    @property
    def _sortable(self) -> SortingBase | None:
        base_classes = inspect.getmro(self.query.__class__)
        sort_class = [x for x in base_classes if issubclass(x, SortingBase)]
        if len(sort_class) > 0:
            sort_class.remove(self.query.__class__)
            sort_class.remove(SortingBase)
            return sort_class[0]
        else:
            return None

    def fields(self) -> str:
        """
        loops through all ancestor classes and calls
        their respective fields() methods to concatenate
        into additional fields for select

        Returns:

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
        return self.query.model_dump(exclude_unset=True, by_alias=True)

    @staticmethod
    def total() -> str:
        """Generates the SQL for the count of total records found.

        Returns:
            SQL string for the count of total records found
        """
        return ", COUNT(1) OVER() as found"

    def where(self) -> str:
        """Introspects object ancestors and calls respective where() methods.

        Returns:
            SQL string of all ancestor WHERE clauses.
        """
        where = []
        bases = self._bases()
        for base in bases:
            if callable(getattr(base, "where", None)):
                if base.where(self.query):
                    where.append(base.where(self.query))
        if len(where):
            where = list(set(where))
            where.sort()  # ensure the order is consistent for testing
            return "WHERE " + ("\nAND ").join(where)
        else:
            return ""

    def order_by(self) -> str:
        if self._sortable:
            return f"ORDER BY {self.query.order_by.lower()} {self.query.sort_order.upper()}"
        else:
            return ""
