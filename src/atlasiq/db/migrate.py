from dataclasses import dataclass
from hashlib import sha256
from importlib.resources import files
import re

import asyncpg

from atlasiq.db.connection import transaction

MIGRATION_LOCK = 4_511_872_101_401
MIGRATION_NAME = re.compile(r"[0-9]{4}_[a-z0-9_]+\.sql")


class MigrationError(Exception):
    pass


@dataclass(frozen=True)
class Migration:
    name: str
    sql: str
    checksum: str


@dataclass(frozen=True)
class MigrationState:
    applied: tuple[str, ...]
    pending: tuple[str, ...]

    @property
    def current(self) -> bool:
        return not self.pending


def _available() -> list[Migration]:
    directory = files("atlasiq.db").joinpath("migrations")
    migrations = []
    for file in sorted(directory.iterdir(), key=lambda entry: entry.name):
        if not MIGRATION_NAME.fullmatch(file.name):
            continue
        # Normalize checkout line endings before hashing a migration.
        sql = file.read_text(encoding="utf-8").replace("\r\n", "\n")
        migrations.append(Migration(file.name, sql, sha256(sql.encode("utf-8")).hexdigest()))
    if not migrations or len({item.name[:4] for item in migrations}) != len(migrations):
        raise MigrationError("Migration files are missing or repeat a version.")
    return migrations


async def _read_applied(connection: asyncpg.Connection) -> list[asyncpg.Record]:
    if not await connection.fetchval("SELECT to_regclass('public.atlasiq_schema_migrations')"):
        return []
    return await connection.fetch("""
        SELECT name, checksum FROM public.atlasiq_schema_migrations ORDER BY name
    """)


def _state(available: list[Migration], applied: list[asyncpg.Record]) -> MigrationState:
    expected = [(item.name, item.checksum) for item in available]
    actual = [(row["name"], row["checksum"]) for row in applied]
    if actual != expected[:len(actual)]:
        raise MigrationError("Migration history differs from this installation. Check the deployed version and checksums.")
    return MigrationState(
        tuple(name for name, _ in actual),
        tuple(name for name, _ in expected[len(actual):]),
    )


async def migration_state(connection: asyncpg.Connection) -> MigrationState:
    return _state(_available(), await _read_applied(connection))


async def apply_migrations(pool: asyncpg.Pool) -> tuple[str, ...]:
    available = _available()
    async with transaction(pool) as connection:
        await connection.execute("SELECT pg_advisory_xact_lock($1::bigint)", MIGRATION_LOCK)
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS public.atlasiq_schema_migrations (
                name text PRIMARY KEY,
                checksum text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
        """)
        state = _state(available, await _read_applied(connection))
        for migration in available[len(state.applied):]:
            await connection.execute(migration.sql)
            await connection.execute("""
                INSERT INTO public.atlasiq_schema_migrations (name, checksum)
                VALUES ($1, $2)
            """, migration.name, migration.checksum)
        return state.pending
