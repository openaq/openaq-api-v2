import logging
import time
import os

import asyncpg
import orjson
from aiocache import SimpleMemoryCache, cached
from aiocache.plugins import HitMissRatioPlugin, TimingPlugin
from buildpg import render
from fastapi import HTTPException, Request
from asyncio.exceptions import TimeoutError

from openaq_fastapi.settings import settings

from .models.responses import Meta, OpenAQResult

logger = logging.getLogger('db')


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
    # each time we create a connect make sure it can
    # properly convert json/jsonb fields
    async def init(con):
        await con.set_type_codec(
            'jsonb',
            encoder=orjson.dumps,
            decoder=orjson.loads,
            schema='pg_catalog'
        )
        await con.set_type_codec(
            'json',
            encoder=orjson.dumps,
            decoder=orjson.loads,
            schema='pg_catalog'
        )

    if pool is None:
        pool = await asyncpg.create_pool(
            settings.DATABASE_READ_URL,
            command_timeout=14,
            max_inactive_connection_lifetime=15,
            min_size=1,
            max_size=10,
            init=init
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
            getattr(self.request.app.state, 'pool', None)
        )
        return self.request.app.state.pool

    @cached(settings.API_CACHE_TIMEOUT, **cache_config)
    async def fetch(self, query, kwargs):
        pool = await self.pool()
        start = time.time()
        logger.debug("Start time: %s\nQuery: %s \nArgs:%s\n", start, query, kwargs)
        rquery, args = render(query, **kwargs)
        async with pool.acquire() as con:
            try:
                r = await con.fetch(rquery, *args)
            except asyncpg.exceptions.UndefinedColumnError as e:
                raise ValueError(f"{e}") from e
            except asyncpg.exceptions.DataError as e:
                raise ValueError(f"{e}") from e
            except asyncpg.exceptions.CharacterNotInRepertoireError as e:
                raise ValueError(f"{e}") from e
            except TimeoutError:
                raise HTTPException(
                    status_code=500,
                    detail="Connection timed out",
                )
            except Exception as e:
                logger.error(f"Database Error: {e}")
                if str(e).startswith("ST_TileEnvelope"):
                    raise HTTPException(status_code=422, detail=f"{e}")
                raise HTTPException(status_code=500, detail=f"{e}")
        logger.debug(
            "query took: %s and returned:%s\n -- results_firstrow: %s",
            time.time() - start,
            len(r),
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

    async def fetchPage(self, query, kwargs):
        if 'limit' in kwargs.keys():
            page = kwargs.get("page", 1)
            kwargs['offset'] = abs((page - 1) * kwargs.get('limit'))

        data = await self.fetch(query, kwargs)
        if len(data) > 0:
            if 'found' in data[0].keys():
                kwargs['found'] = data[0]["found"]
        else:
            kwargs['found'] = 0

        output = OpenAQResult(
            meta=Meta.parse_obj(kwargs),
            results=data
        )
        return output

    async def fetchOpenAQResult(self, query, kwargs):
        rows = await self.fetch(query, kwargs)
        found = 0
        results = []

        if len(rows) > 0:
            if 'count' in rows[0].keys():
                found = rows[0]["count"]
            # OpenAQResult expects a list for results
            if rows[0][1] is not None:
                if isinstance(rows[0][1], list):
                    results = rows[0][1]
                elif isinstance(rows[0][1], dict):
                    results = [
                        r[1] for r in rows
                    ]
                elif isinstance(rows[0][1], str):
                    results = [
                        r[1] for r in rows
                    ]

        meta = Meta(
            website=os.getenv("DOMAIN_NAME", os.getenv("BASE_URL", "/")),
            page=kwargs["page"],
            limit=kwargs["limit"],
            found=found,
        )
        output = OpenAQResult(meta=meta, results=results)
        return output
