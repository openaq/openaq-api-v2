import inspect
import logging
from types import FunctionType
from typing import (
    Dict,
    Union,
)

import humps
from fastapi import Query
from pydantic import (
    BaseModel,
    conint,
    confloat,
    root_validator,
)
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

    def params(self):
        return self.dict(exclude_unset=True, by_alias=True)

    def pagination(self):
        return "LIMIT 1"

    def fields(self):
        return ""

    def total(self):
        return ", COUNT(1) OVER() as found"

    def has(self, field_name: str):
        return hasattr(self, field_name) and getattr(self, field_name) is not None

    def where(self):
        if hasattr(self, "id"):
            return "WHERE id = :id"
        else:
            return ""


# Thinking about how the paging should be done
# we should not let folks pass an offset if we also include
# a page parameter. And until pydantic supports computed
# values (v2) we have to calculate the offset ourselves
# see the db.py method
class Paging(QueryBaseModel):
    limit: int = Query(
        100,
        gt=0,
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

    def pagination(self):
        return "OFFSET :offset\nLIMIT :limit"


class ProviderQuery(QueryBaseModel):
    providers_id: Union[int, None] = Query(
        description="Limit the results to a specific provider", ge=1
    )

    def where(self):
        return "(provider->'id')::int = :providers_id"


class OwnerQuery(QueryBaseModel):
    owner_contacts_id: Union[int, None] = Query(
        description="Limit the results to a specific owner", ge=1
    )

    def where(self):
        return "(owner->'id')::int = :owner_contacts_id"


class CountryQuery(QueryBaseModel):
    countries_id: Union[int, None] = Query(
        description="Limit the results to a specific country", ge=1
    )
    iso: Union[str, None] = Query(
        description="Limit the results to a specific country using ISO code",
    )

    def where(self):
        if self.countries_id is not None:
            return "(country->'id')::int = :countries_id"
        elif self.iso is not None:
            return "country->>'code' = :iso"


# Some spatial helper queries
class RadiusQuery(QueryBaseModel):
    coordinates: Union[str, None] = Query(
        None,
        regex=r"^-?\d{1,2}\.?\d{0,8},-?1?\d{1,2}\.?\d{0,8}$",
        description="Coordinate pair in form lat,lng. Up to 8 decimal points of precision e.g. 38.907,-77.037",
        example="38.907,-77.037",
    )
    radius: conint(gt=0, le=100000) = Query(
        None,
        description="Search radius from coordinates as center in meters. Maximum of 100,000 (100km) defaults to 1000 (1km) e.g. radius=1000",
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

    def fields(self, geometry_field: str = "geom"):
        return f"st_distance({geometry_field}, st_setsrid(st_makepoint(:lon, :lat), 4326)) as distance"

    def where(self, geometry_field: str = "geom"):
        if self.lat is not None and self.lon is not None:
            return f"st_dwithin(st_setsrid(st_makepoint(:lon, :lat), 4326), {geometry_field}, :radius)"
        return None


class BboxQuery(QueryBaseModel):
    bbox: Union[str, None] = Query(
        None,
        regex=r"^-?\d{1,2}\.?\d{0,4},-?1?\d{1,2}\.?\d{0,4},-?\d{1,2}\.?\d{0,4},-?\d{1,2}\.?\d{0,4}$",
        description="Min X, min Y, max X, max Y, up to 4 decimal points of precision e.g. -77.037,38.907,-77.0,39.910",
        example="-77.037,38.907,-77.035,38.910",
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

    def where(self):
        if self.bbox is not None:
            return "st_makeenvelope(:minx, :miny, :maxx, :maxy, 4326) && geom"
        return None


class MeasurementsQueries(Paging):
    locations_id: int = Query(
        description="Limit the results to a specific location",
        ge=1,
    )
    limit: int = Query(2)

    def where(self):
        where = ["WHERE sensor_nodes_id = :locations_id"]
        if self.has('providers_id'):
            where.append(ProviderQuery.where(self))
        return ("\nAND ").join(where)



class LocationsQueries(
    Paging,
    RadiusQuery,
    BboxQuery,
    ProviderQuery,
    OwnerQuery,
    CountryQuery,
):
    mobile: Union[bool, None] = Query(
        description="Is the location considered a mobile location?"
    )

    monitor: Union[bool, None] = Query(
        description="Is the location considered a reference monitor?"
    )

    @root_validator(pre=True)
    def check_bbox_radius_set(cls, values):
        bbox = values.get("bbox", None)
        coordinates = values.get("coordinates", None)
        radius = values.get("radius", None)
        print(values)
        if bbox is not None and (coordinates is not None or radius is not None):
            raise ValueError(
                "Cannot pass both bounding box and coordinate/radius query in the same URL"
            )
        return values

    def fields(self):
        fields = []
        if self.has("coordinates"):
            fields.append(RadiusQuery.fields(self))
        return ", " + (",").join(fields) if len(fields) > 0 else ""

    def where(self):
        where = ["WHERE TRUE"]
        if self.has("mobile"):
            where.append("ismobile = :mobile")
        if self.has("mobile"):
            where.append("ismonitor = :monitor")
        if self.has("coordinates"):
            where.append(RadiusQuery.where(self))
        if self.has("bbox"):
            where.append(BboxQuery.where(self))
        if self.has("countries_id"):
            where.append(CountryQuery.where(self))
        if self.has("providers_id"):
            where.append(ProviderQuery.where(self))
        return ("\nAND ").join(where)
