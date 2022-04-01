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

logger = logging.getLogger(__name__)


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
        logger.debug('Creating a new pool')
        pool = await asyncpg.create_pool(
            settings.DATABASE_URL,
            command_timeout=14,
            max_inactive_connection_lifetime=15,
            min_size=1,
            max_size=10,
        )
    else:
        logger.debug('Using existing pool')
    return pool


class DB:
    def __init__(self, request: Request):
        self.request = request

    async def acquire(self):
        pool = await self.pool()
        return pool

    async def pool(self):
        logger.debug(f'Getting pool: {self.request.app.state.pool}')
        self.request.app.state.pool = await db_pool(
            self.request.app.state.pool
        )
        return self.request.app.state.pool

    @cached(settings.OPENAQ_CACHE_TIMEOUT, **cache_config)
    async def fetch(self, query, kwargs):
        try:
            pool = await self.pool()
            logger.debug(f'Pool status - connections:{pool.get_size()}')
        except Exception as e:
            logger.error(e)
            raise HTTPException(
                status_code=500,
                detail="Error getting connection from pool",
                ) from None

        start = time.time()
        logger.info("Start time: %s Query: %s Args:%s", start, query, kwargs)
        rquery, args = render(query, **kwargs)
        try:
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
                    logger.warning(f"Database Error: {e}")
                    if str(e).startswith("ST_TileEnvelope"):
                        raise HTTPException(
                            status_code=422,
                            detail=f"{e}"
                        ) from None
                    raise HTTPException(
                        status_code=500,
                        detail=f"{e}"
                    ) from None
        except Exception as e:
            logger.error(f"Error getting connection from pool: {e}")
            return []

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
            found = rows[0].get("count", 0)

            results = [orjson.dumps(r[1]) for r in rows]
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

    async def fetchOpenAQResult_VERSIONING(self, query, kwargs):
        """
        Special method for the versioning branch so that we
        dont have to write queries that return results in complicated
        CTE expressions. There are easier ways to get the total counts
        for a query
        """
        rows = await self.fetch(query, kwargs)

        if len(rows) == 0:
            found = 0
        else:
            found = rows[0].get("count", 0)

        meta = Meta(
            website=os.getenv("APP_HOST", "/"),
            page=kwargs["page"],
            limit=kwargs["limit"],
            found=found,
        )
        output = OpenAQResult(meta=meta, results=rows)
        return output
