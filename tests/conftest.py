import os
from datetime import UTC, datetime
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from pydantic import ValidationError

from atlasiq.config.settings import Settings
from atlasiq.db.connection import database_pool
from atlasiq.db.migrate import apply_migrations
from atlasiq.db.repositories import CrawlRuns, RawDocuments
from atlasiq.sources.registry import SourceConfig, SourceRegistry


@pytest.fixture
def clean_env(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for name in Settings.model_fields:
        monkeypatch.delenv(name.upper(), raising=False)


@pytest.fixture
def source_config():
    return {
        "id": "test-papers",
        "name": "Fixture Papers",
        "category": "RESEARCH_PAPER",
        "base_url": "https://example.org/",
        "adapter_name": "fixture",
        "rate_limit_config": {"requests_per_minute": 4},
    }


@pytest.fixture
def startup_json():
    return {
        "schemaVersion": "1.0",
        "recordType": "STARTUP",
        "source": {"name": "Fixture", "url": "https://example.org/item?q=1#evidence"},
        "content": {"entityName": "Fixture Co.", "data": {"employeeCount": 0}},
        "collectedAt": "2026-01-10T14:00:00+05:30",
    }


@pytest_asyncio.fixture
async def disposable_settings():
    test_dsn = os.environ.get("TEST_DATABASE_URL")
    if not test_dsn:
        pytest.skip("TEST_DATABASE_URL is required for disposable PostgreSQL integration tests")
    try:
        maintenance = Settings(_env_file=None, database_url=test_dsn, app_env="test")
        admin = await asyncpg.connect(maintenance.asyncpg_dsn(), timeout=10, command_timeout=30)
    except (ValidationError, asyncpg.PostgresError, OSError, TimeoutError) as error:
        pytest.fail(f"Could not connect to the test maintenance database ({type(error).__name__})", pytrace=False)

    # The only interpolated SQL identifier is generated here, independently of the DSN.
    database_name = f"atlasiq_test_{uuid4().hex}"
    try:
        await admin.execute(f'CREATE DATABASE "{database_name}"')
        try:
            uri = urlsplit(maintenance.asyncpg_dsn())
            query = urlencode([
                (key, value) for key, value in parse_qsl(uri.query, keep_blank_values=True)
                if key not in {"database", "dbname"}
            ])
            dsn = urlunsplit(uri._replace(path=f"/{database_name}", query=query))
            yield Settings(
                _env_file=None, database_url=dsn, app_env="test", task_max_attempts=3,
                task_lease_seconds=60, task_retry_base_seconds=2, task_retry_max_seconds=3,
            )
        finally:
            await admin.execute(f'DROP DATABASE "{database_name}"')
    finally:
        await admin.close()


@pytest_asyncio.fixture
async def pool(disposable_settings):
    async with database_pool(disposable_settings) as pool:
        await apply_migrations(pool)
        yield pool


@pytest_asyncio.fixture
async def saved_document(pool, source_config):
    async with pool.acquire() as connection:
        source = await SourceRegistry(connection).register(SourceConfig.model_validate(source_config))
        runs = CrawlRuns(connection)
        run = await runs.create(source["id"])
        await runs.start(run["id"])
        return await RawDocuments(connection).save(
            source_id=source["id"], crawl_run_id=run["id"],
            url="https://example.org/item", raw_content="Test fixture input",
            fetched_at=datetime.now(UTC),
        )
