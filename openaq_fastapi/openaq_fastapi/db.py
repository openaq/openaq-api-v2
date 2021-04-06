import logging
import time
import os

import asyncpg
import orjson
from aiocache import SimpleMemoryCache, cached
from aiocache.plugins import HitMissRatioPlugin, TimingPlugin
from buildpg import render
from fastapi import HTTPException, Request

from openaq_fastapi.settings import settings

from .models.responses import Meta, OpenAQResult

logger = logging.getLogger("base")
logger.setLevel(logging.DEBUG)


def default(obj):
    return str(obj)


def dbkey(m, f, query, args):
    j = orjson.dumps(
        args, option=orjson.OPT_OMIT_MICROSECONDS, default=default
    ).decode()
    dbkey = f"{query}{j}"
    h = hash(dbkey)
    # logger.debug(f"dbkey: {dbkey} h: {h}")
    return h


cache_config = {
    "key_builder": dbkey,
    "cache": SimpleMemoryCache,
    "noself": True,
    "plugins": [
        HitMissRatioPlugin(),
        TimingPlugin(),
    ],
}


async def db_pool(pool):
    if pool is None:
        pool = await asyncpg.create_pool(
            settings.DATABASE_URL,
            command_timeout=14,
            max_inactive_connection_lifetime=15,
            min_size=1,
            max_size=10,
        )
    return pool


class DB:
    def __init__(self, request: Request):
        self.request = request

    async def acquire(self):
        pool = await self.pool()
        return pool

    async def pool(self):
        self.request.app.state.pool = await db_pool(
            self.request.app.state.pool
        )
        return self.request.app.state.pool

    @cached(settings.OPENAQ_CACHE_TIMEOUT, **cache_config)
    async def fetch(self, query, kwargs):
        pool = await self.pool()
        start = time.time()
        logger.debug("Start time: %s Query: %s Args:%s", start, query, kwargs)
        rquery, args = render(query, **kwargs)
        async with pool.acquire() as con:
            try:
                r = await con.fetch(rquery, *args)
            except asyncpg.exceptions.UndefinedColumnError as e:
                raise ValueError(f"{e}")
            except asyncpg.exceptions.DataError as e:
                raise ValueError(f"{e}")
            except asyncpg.exceptions.CharacterNotInRepertoireError as e:
                raise ValueError(f"{e}")
            except Exception as e:
                logger.debug(f"Database Error: {e}")
                if str(e).startswith("ST_TileEnvelope"):
                    raise HTTPException(status_code=422, detail=f"{e}")
                raise HTTPException(status_code=500, detail=f"{e}")
        logger.debug(
            "query took: %s results_firstrow: %s",
            time.time() - start,
            str(r and r[0])[0:500],
        )
        return r

    async def fetchrow(self, query, kwargs):
        r = await self.fetch(query, kwargs)
        if len(r) > 0:
            return r[0]
        return []

    async def fetchval(self, query, kwargs):
        r = await self.fetchrow(query, kwargs)
        if len(r) > 0:
            return r[0]
        return None

    async def fetchOpenAQResult(self, query, kwargs):
        rows = await self.fetch(query, kwargs)

        if len(rows) == 0:
            found = 0
            results = []
        else:
            found = rows[0]["count"]
            # results = [orjson.dumps(r[1]) for r in rows]
            if len(rows) > 0 and rows[0][1] is not None:
                results = [
                    orjson.loads(r[1]) for r in rows if isinstance(r[1], str)
                ]
            else:
                results = []

        meta = Meta(
            website=os.getenv("APP_HOST", "/"),
            page=kwargs["page"],
            limit=kwargs["limit"],
            found=found,
        )
        output = OpenAQResult(meta=meta, results=results)
        return output
