from dataclasses import dataclass

import asyncpg

from atlasiq.db.migrate import MigrationState, migration_state


@dataclass(frozen=True)
class Health:
    postgres_version: str
    migrations: MigrationState


async def check_database(connection: asyncpg.Connection) -> str:
    return await connection.fetchval("SELECT current_setting('server_version')")


async def check_health(connection: asyncpg.Connection) -> Health:
    return Health(await check_database(connection), await migration_state(connection))
