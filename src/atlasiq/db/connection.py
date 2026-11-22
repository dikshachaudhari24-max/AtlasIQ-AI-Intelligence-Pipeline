import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg

from atlasiq.config.settings import Settings


async def _configure(connection: asyncpg.Connection) -> None:
    for type_name in ("json", "jsonb"):
        await connection.set_type_codec(
            type_name,
            schema="pg_catalog",
            encoder=lambda value: json.dumps(value, allow_nan=False),
            decoder=json.loads,
        )


@asynccontextmanager
async def database_pool(settings: Settings) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(
        dsn=settings.asyncpg_dsn(),
        min_size=1,
        max_size=10,
        timeout=10,
        command_timeout=30,
        server_settings={"timezone": "UTC", "search_path": "public, pg_catalog"},
        init=_configure,
    )
    try:
        yield pool
    finally:
        try:
            await asyncio.wait_for(pool.close(), timeout=10)
        except TimeoutError:
            pool.terminate()
            raise


@asynccontextmanager
async def transaction(pool: asyncpg.Pool) -> AsyncIterator[asyncpg.Connection]:
    async with pool.acquire() as connection, connection.transaction():
        yield connection
