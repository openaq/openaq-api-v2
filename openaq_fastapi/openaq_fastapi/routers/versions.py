import logging
from typing import List, Optional
from pydantic.typing import Any
from fastapi import APIRouter, Depends, Query
from pydantic.main import BaseModel
from datetime import date
from openaq_fastapi.models.responses import Meta
from ..db import DB
# from ..models.queries import Version

logger = logging.getLogger(__name__)

router = APIRouter()


class Version(BaseModel):
    version_date: date
    version_rank: int
    life_cycle: str
    sensor: str
    parent_sensor: str


class LifeCycle(BaseModel):
    life_cycles_id: int
    label: str
    short_code: str
    sort_order: int
    readme: str


class OpenAQVersions(BaseModel):
    meta: Meta = Meta()
    results: List[Version] = []


class OpenAQLifeCycles(BaseModel):
    meta: Meta = Meta()
    results: List[LifeCycle] = []


@router.get(
    "/v2/versions",
    response_model=OpenAQVersions,
    tags=["v2"],
    summary="Simple listing of the available versions",
)
async def versions_get(
        db: DB = Depends(),
        sensors_id: Optional[int] = Query(
            None,
            description="""
            Match to a specific sensors_id
            """,
        ),
        parent_sensors_id: Optional[int] = Query(
            None,
            description="""
            Match to a specific parent_sensors_id
            """,
        ),
        life_cycles_id: Optional[int] = Query(
            None,
            description="""
            Match to a specific life_cycles_id
            """,
        ),
        version_date: Optional[date] = Query(
            None,
            description="""
            Limit to versions matching a specific version date
            """,
        ),
        version_rank: Optional[int] = Query(
            None,
            description="""
            Limit to version rank
            """,
        ),
        page: Optional[int] = 1,
        limit: Optional[int] = 100,
):

    where = []
    params = {
        "page": page,
        "offset": (page-1)*limit,
        "limit": limit
    }

    if life_cycles_id is not None:
        where.append('life_cycles_id = :life_cycles_id')
        params['life_cycles_id'] = life_cycles_id

    if sensors_id is not None:
        where.append('sensors_id = :sensors_id')
        params['sensors_id'] = sensors_id

    if parent_sensors_id is not None:
        where.append('parent_sensors_id = :parent_sensors_id')
        params['parent_sensors_id'] = parent_sensors_id

    if version_date is not None:
        where.append('version_date = :version_date')
        params['version_date'] = version_date

    if version_rank is not None:
        where.append('version_rank = :version_rank')
        params['version_rank'] = version_rank

    if len(where):
        where = 'WHERE ' + (' AND ').join(where)
    else:
        where = ""

    q = f"""
    SELECT versions_id
    , sensor
    , parent_sensor
    , version_date
    , life_cycle
    , version_rank
    , COUNT(1) OVER() as count
    FROM versions_view
    {where}
    OFFSET :offset
    LIMIT :limit
    """
    output = await db.fetchOpenAQResult_VERSIONING(q, params)

    return output


@router.get(
    "/v2/lifecycles",
    response_model=OpenAQLifeCycles,
    tags=["v2"],
    summary="Simple listing of the available lifecycles",
)
async def life_cycles_get(
        db: DB = Depends(),
):

    params = {
        "page": 1,
        "offset": 0,
        "limit": 100
    }

    q = """
    SELECT *
    , COUNT(1) OVER() as count
    FROM life_cycles
    """
    output = await db.fetchOpenAQResult_VERSIONING(q, params)

    return output
