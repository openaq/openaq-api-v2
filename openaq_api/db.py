import logging
import time
import os
import json

import asyncpg
from openaq_api.models.auth import User
import orjson
from aiocache import SimpleMemoryCache, cached
from aiocache.plugins import HitMissRatioPlugin, TimingPlugin
from buildpg import render
from fastapi import HTTPException, Request
from asyncio.exceptions import TimeoutError
from asyncio import wait_for

from openaq_api.settings import settings

from openaq_api.models.responses import Meta, OpenAQResult
from openaq_api.models.logging import HTTPLog

logger = logging.getLogger("db")

allowed_config_params = ["work_mem"]


DEFAULT_CONNECTION_TIMEOUT = 6
MAX_CONNECTION_TIMEOUT = 15


def default(obj):
    return str(obj)


# config is required as a placeholder here because of this
# function is used in the `cached` decorator and without it
# we will get a number of arguments error


def dbkey(m, f, query, args, timeout=None, config=None):
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
            "jsonb", encoder=orjson.dumps, decoder=orjson.loads, schema="pg_catalog"
        )
        await con.set_type_codec(
            "json", encoder=orjson.dumps, decoder=orjson.loads, schema="pg_catalog"
        )

    logger.debug(f"Checking for existing pool: {pool}")
    logger.debug(settings.DATABASE_READ_URL)
    if pool is None:
        logger.debug("Creating a new pool")
        pool = await asyncpg.create_pool(
            settings.DATABASE_READ_URL,
            command_timeout=MAX_CONNECTION_TIMEOUT,
            max_inactive_connection_lifetime=15,
            min_size=1,
            max_size=10,
            init=init,
        )
    return pool


class DB:
    def __init__(self, request: Request):
        self.request = request
        request.state.timer.mark("db")

    async def acquire(self):
        pool = await self.pool()
        return pool

    async def pool(self):
        self.request.app.state.pool = await db_pool(
            getattr(self.request.app.state, "pool", None)
        )
        return self.request.app.state.pool

    @cached(settings.API_CACHE_TIMEOUT, **cache_config)
    async def fetch(
        self, query, kwargs, timeout=DEFAULT_CONNECTION_TIMEOUT, config=None
    ):
        pool = await self.pool()
        self.request.state.timer.mark("pooled")
        start = time.time()
        logger.debug("Start time: %s\nQuery: %s \nArgs:%s\n", start, query, kwargs)
        rquery, args = render(query, **kwargs)
        async with pool.acquire() as con:
            try:
                # a transaction is required to prevent auto-commit
                tr = con.transaction()
                await tr.start()
                if config is not None:
                    for param, value in config.items():
                        if param in allowed_config_params:
                            q = f"SELECT set_config('{param}', $1, TRUE)"
                            await con.execute(q, str(value))
                if not isinstance(timeout, (str, int)):
                    logger.warning(
                        f"Non int or string timeout value passed - {timeout}"
                    )
                    timeout = DEFAULT_CONNECTION_TIMEOUT
                r = await wait_for(con.fetch(rquery, *args), timeout=timeout)
                await tr.commit()
            except asyncpg.exceptions.UndefinedColumnError as e:
                logger.error(f"Undefined Column Error: {e}\n{rquery}\n{args}")
                raise ValueError(f"{e}") from e
            except asyncpg.exceptions.CharacterNotInRepertoireError as e:
                raise ValueError(f"{e}") from e
            except asyncpg.exceptions.DataError as e:
                logger.error(f"Data Error: {e}\n{rquery}\n{args}")
                raise ValueError(f"{e}") from e
            except TimeoutError:
                raise HTTPException(
                    status_code=408,
                    detail="Connection timed out: Try to provide more specific query parameters or a smaller time frame.",
                )
            except Exception as e:
                logger.error(f"Unknown database error: {e}\n{rquery}\n{args}")
                if str(e).startswith("ST_TileEnvelope"):
                    raise HTTPException(status_code=422, detail=f"{e}")
                raise HTTPException(status_code=500, detail=f"{e}")
        logger.debug(
            "query took: %s and returned:%s\n -- results_firstrow: %s",
            self.request.state.timer.mark("fetched", "since"),
            len(r),
            str(r and r[0])[0:1000],
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

    async def fetchPage(
        self, query, kwargs, timeout=DEFAULT_CONNECTION_TIMEOUT, config=None
    ) -> OpenAQResult:
        page = kwargs.get("page", 1)
        limit = kwargs.get("limit", 1000)
        kwargs["offset"] = abs((page - 1) * limit)

        data = await self.fetch(query, kwargs, timeout, config)
        if len(data) > 0:
            if "found" in data[0].keys():
                kwargs["found"] = data[0]["found"]
            elif len(data) == limit:
                kwargs["found"] = f">{limit}"
            else:
                kwargs["found"] = len(data)
        else:
            kwargs["found"] = 0

        output = OpenAQResult(
            meta=Meta.model_validate(kwargs), results=[dict(x) for x in data]
        )
        return output

    async def create_user(self, user: User) -> str:
        """
        calls the create_user plpgsql function to create a new user and entity records
        """
        query = """
        SELECT * FROM create_user(:full_name, :email_address, :password_hash, :ip_address, :entity_type)
        """
        conn = await asyncpg.connect(settings.DATABASE_WRITE_URL)
        rquery, args = render(query, **user.model_dump())
        verification_token = await conn.fetch(rquery, *args)
        await conn.close()
        return verification_token[0][0]

    async def get_user(self, users_id: int) -> str:
        """
        gets user info from users table and entities table
        """
        query = """
        SELECT
            e.full_name
            , u.email_address
            , u.verification_code
        FROM
            users u
        JOIN
            users_entities USING (users_id)
        JOIN
            entities e USING (entities_id)
        WHERE
            u.users_id = :users_id
        """
        conn = await asyncpg.connect(settings.DATABASE_READ_URL)
        rquery, args = render(query, **{"users_id": users_id})
        user = await conn.fetch(rquery, *args)
        await conn.close()
        return user

    async def generate_verification_code(self, email_address: str) -> str:
        """
        gets user info from users table and entities table
        """
        query = """
        UPDATE
            users
        SET
            verification_code = generate_token()
            , expires_on = (timestamptz (NOW() + INTERVAL '30min'))
        WHERE
            email_address = :email_address
        RETURNING verification_code as "verificationCode"
        """
        conn = await asyncpg.connect(settings.DATABASE_WRITE_URL)
        rquery, args = render(query, **{"email_address": email_address})
        row = await conn.fetch(rquery, *args)
        await conn.close()
        return row[0][0]

    async def regenerate_user_token(self, users_id: int, token: str) -> str:
        """
        calls the get_user_token plpgsql function to verify user email and generate API token
        """
        query = """
        UPDATE
            user_keys
        SET
            token = generate_token()
        WHERE
            users_id = :users_id
        AND
            token = :token
        """
        conn = await asyncpg.connect(settings.DATABASE_WRITE_URL)
        rquery, args = render(query, **{"users_id": users_id, "token": token})
        await conn.fetch(rquery, *args)
        await conn.close()

    async def get_user_token(self, users_id: int) -> str:
        """ """
        query = """
        SELECT token FROM user_keys WHERE users_id = :users_id
        """
        conn = await asyncpg.connect(settings.DATABASE_WRITE_URL)
        rquery, args = render(query, **{"users_id": users_id})
        api_token = await conn.fetch(rquery, *args)
        await conn.close()
        return api_token[0][0]

    async def generate_user_token(self, users_id: int) -> str:
        """
        calls the get_user_token plpgsql function to verify user email and generate API token
        """
        query = """
        SELECT * FROM get_user_token(:users_id)
        """
        conn = await asyncpg.connect(settings.DATABASE_WRITE_URL)
        rquery, args = render(query, **{"users_id": users_id})
        api_token = await conn.fetch(rquery, *args)
        await conn.close()
        return api_token[0][0]

    async def fetchOpenAQResult(self, query, kwargs):
        rows = await self.fetch(query, kwargs)
        found = 0
        results = []

        if len(rows) > 0:
            if "count" in rows[0].keys():
                found = rows[0]["count"]
            # OpenAQResult expects a list for results
            if rows[0][1] is not None:
                if isinstance(rows[0][1], list):
                    results = rows[0][1]
                elif isinstance(rows[0][1], dict):
                    results = [r[1] for r in rows]
                elif isinstance(rows[0][1], str):
                    results = [r[1] for r in rows]

        meta = Meta(
            website=os.getenv("DOMAIN_NAME", os.getenv("BASE_URL", "/")),
            page=kwargs["page"],
            limit=kwargs["limit"],
            found=found,
        )
        output = OpenAQResult(meta=meta, results=results)
        return output

    async def post_log(self, entry: HTTPLog) -> bool:
        """
        Long information about api use and errors in the logs
        """
        query = """
        INSERT INTO api_logs (api_key, status_code, endpoint, params)
        VALUES
        (:api_key, :status_code, :endpoint, :params)
        """
        conn = None
        try:
            conn = await asyncpg.connect(settings.DATABASE_WRITE_URL)
            rquery, args = render(query, api_key=entry.api_key, endpoint=entry.path, params=json.dumps(entry.params_obj), status_code=entry.http_code)
            await conn.fetch(rquery, *args)
            await conn.close()
        except Exception as e:
            logger.error(e)
            if conn is not None:
                await conn.close()

        return True
