import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import asyncpg
import pytest

from atlasiq.core.health import check_health
from atlasiq.db.connection import database_pool, transaction
from atlasiq.db.migrate import MigrationError, apply_migrations
from atlasiq.db.models import CrawlStatus, SourceCategory
from atlasiq.db.repositories import CanonicalRecords, CrawlRuns, CrawlRunStateError, IngestionEvidence, RawDocuments
from atlasiq.schemas.entities import Job, News, Product, ResearchPaper, Startup
from atlasiq.sources.registry import SourceConfig, SourceRegistry
from atlasiq.tasks.queue import LeaseLost, TaskQueue

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

TABLES = {
    "sources", "crawl_runs", "raw_documents", "ingestion_tasks", "extraction_attempts", "startups", "products",
    "research_papers", "paper_github_metrics", "jobs", "news", "entity_mappings", "quarantine_records",
    "crawl_run_items", "intelligence_records",
}
MIGRATIONS = ("0001_foundation.sql", "0002_crawling.sql", "0003_intelligence.sql")


async def test_connect_health_and_concurrent_repeatable_migrations(disposable_settings):
    async with database_pool(disposable_settings) as pool:
        async with pool.acquire() as connection:
            health = await check_health(connection)
            assert health.postgres_version
            assert not health.migrations.current and health.migrations.pending == MIGRATIONS
            assert not await connection.fetchval("SELECT to_regclass('public.atlasiq_schema_migrations')")
            assert await connection.fetchval("SHOW TIMEZONE") == "UTC"
        first, second = await asyncio.gather(apply_migrations(pool), apply_migrations(pool))
        assert {first, second} == {MIGRATIONS, ()}
        assert await apply_migrations(pool) == ()
        async with pool.acquire() as connection:
            assert (await check_health(connection)).migrations.current
            public_tables = await connection.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            assert {row[0] for row in public_tables} == TABLES | {"atlasiq_schema_migrations"}
            await connection.execute("UPDATE public.atlasiq_schema_migrations SET checksum = 'invalid'")
            with pytest.raises(MigrationError, match="checksums"):
                await check_health(connection)
        with pytest.raises(MigrationError, match="checksums"):
            await apply_migrations(pool)


async def test_migration_rolls_back_if_schema_conflicts(disposable_settings):
    async with database_pool(disposable_settings) as pool:
        async with pool.acquire() as connection:
            await connection.execute("CREATE TABLE public.ingestion_tasks (marker text)")
        with pytest.raises(asyncpg.DuplicateTableError):
            await apply_migrations(pool)
        async with pool.acquire() as connection:
            assert not await connection.fetchval("SELECT to_regclass('public.sources')")
            assert not await connection.fetchval("SELECT to_regclass('public.atlasiq_schema_migrations')")
            assert await connection.fetchval("SELECT to_regclass('public.ingestion_tasks')")


async def test_crawl_checkpoints_raw_identity_and_revisions(pool, saved_document):
    async with pool.acquire() as connection:
        runs = CrawlRuns(connection)
        documents = RawDocuments(connection)
        first_run = saved_document["crawl_run_id"]
        source_id = saved_document["source_id"]
        await runs.checkpoint(first_run, cursor={"page": 2}, discovered_count=1, accepted_count=0, rejected_count=0)
        done = await runs.finish(first_run, CrawlStatus.PARTIAL, failure_summary="UPSTREAM_TIMEOUT")
        assert done["finished_at"] >= done["started_at"] and done["cursor"] == {"page": 2}
        assert done["discovered_count"] == 1
        with pytest.raises(CrawlRunStateError):
            await runs.checkpoint(first_run, cursor=None, discovered_count=0, accepted_count=0, rejected_count=0)
        run = await runs.create(source_id)
        await runs.start(run["id"])
        details = {
            "source_id": source_id, "crawl_run_id": run["id"], "url": "https://EXAMPLE.org:443/item#new",
            "fetched_at": datetime.now(UTC),
        }
        duplicate = await documents.save(**details, raw_content="Test fixture input")
        changed = await documents.save(**details, raw_content="Changed fixture input")
        assert duplicate == saved_document
        assert changed["id"] != duplicate["id"] and changed["content_hash"] != duplicate["content_hash"]
        assert changed["source_url"] == details["url"] and changed["canonical_url"] == duplicate["canonical_url"]
        assert changed["crawl_run_id"] == run["id"]
        assert await runs.get(run["id"]) is not None
        assert (await documents.set_status(changed["id"], "QUARANTINED"))["status"] == "QUARANTINED"
        assert await documents.get(changed["id"]) is not None
        with pytest.raises(ValueError, match="SHA-256"):
            await documents.save(**details, storage_reference="fixture/reference")


async def test_source_run_provenance_cannot_be_mismatched(pool, saved_document, source_config):
    async with pool.acquire() as connection:
        await SourceRegistry(connection).register(
            SourceConfig.model_validate(source_config | {"id": "another", "name": "Another Fixture"})
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await RawDocuments(connection).save(
                source_id="another", crawl_run_id=saved_document["crawl_run_id"], url="https://example.org/other",
                raw_content="Test fixture input", fetched_at=datetime.now(UTC),
            )


async def test_canonical_upsert_and_rollback_when_lease_is_lost(pool, disposable_settings, saved_document, startup_json):
    provenance = {"source_id": saved_document["source_id"], "raw_document_id": saved_document["id"]}
    startup = Startup.model_validate(startup_json)
    async with pool.acquire() as connection:
        queue = TaskQueue(connection, disposable_settings)
        task = await queue.enqueue_task(saved_document["id"], "normalize", "1")
        [claim] = await queue.claim_tasks("worker")
    with pytest.raises(LeaseLost):
        async with transaction(pool) as connection:
            await CanonicalRecords(connection).upsert_startup(startup, canonical_key="fixture", **provenance)
            await TaskQueue(connection, disposable_settings).complete_task(task["id"], uuid4())
    async with pool.acquire() as connection:
        assert await connection.fetchval("SELECT count(*) FROM public.startups") == 0

    async with transaction(pool) as connection:
        saved = await CanonicalRecords(connection).upsert_startup(startup, canonical_key="fixture", **provenance)
        await TaskQueue(connection, disposable_settings).complete_task(task["id"], claim["claim_token"])
    startup_json["content"]["data"]["employeeCount"] = 12
    async with pool.acquire() as connection:
        updated = await CanonicalRecords(connection).upsert_startup(
            Startup.model_validate(startup_json), canonical_key="fixture", **provenance,
        )
        assert updated["id"] == saved["id"] and updated["employee_count"] == 12
        assert updated["source_url"] == startup.source.url
        assert await connection.fetchval("SELECT count(*) FROM public.startups") == 1
        assert (await TaskQueue(connection, disposable_settings).get_task_status(task["id"]))["status"] == "COMPLETED"


async def test_other_canonical_tables_and_evidence(pool, saved_document, startup_json):
    provenance = {"source_id": saved_document["source_id"], "raw_document_id": saved_document["id"]}
    base = {key: startup_json[key] for key in ("source", "schemaVersion")}
    timestamp = startup_json["collectedAt"]
    now = datetime.now(UTC)
    async with pool.acquire() as connection:
        repository = CanonicalRecords(connection)
        product = Product.model_validate(base | {
            "recordType": "PRODUCT", "collectedAt": timestamp,
            "content": {"startupName": "Fixture Co.", "pricingModel": "FREE"},
        })
        product_row = await repository.upsert_product(
            product, canonical_key="fixture-product", product_name="Fixture Product", **provenance,
        )
        assert product_row["pricing_model"] == "FREE" and product_row["startup_id"] is None

        paper = ResearchPaper.model_validate(base | {
            "recordType": "RESEARCH_PAPER", "content": {
                "title": "Fixture Paper", "authors": ["Fixture Author"],
                "paper_url": "https://example.org/paper", "published_date": timestamp,
            },
        })
        paper_row = await repository.upsert_research_paper(
            paper, canonical_key="fixture-paper", arxiv_id="fixture-id", doi="10.0000/fixture", **provenance,
        )
        assert paper_row["authors"] == ["Fixture Author"]
        with pytest.raises(asyncpg.UniqueViolationError):
            await repository.upsert_research_paper(
                paper, canonical_key="second-fixture-paper", doi="10.0000/FIXTURE", **provenance,
            )
        for stars, observed_at in ((0, now), (None, now + timedelta(seconds=5))):
            metric = await repository.record_paper_metric(
                paper_id=paper_row["id"], repository_url="https://github.com/example/example",
                repository_identity="example/example", association_evidence={"kind": "test fixture"},
                github_stars=stars, observed_at=observed_at,
            )
            assert metric["github_stars"] == stars
            assert metric["association_evidence"] == {"kind": "test fixture"}

        job = Job.model_validate(base | {
            "recordType": "JOB",
            "content": {"company": "Fixture", "role_family": "Research", "date": timestamp, "is_remote": False},
        })
        job_row = await repository.upsert_job(
            job, canonical_key="fixture-job", job_title="Researcher", full_text="Fixture job",
            collected_at=now, publication_evidence={"original": timestamp}, **provenance,
        )
        assert job_row["is_remote"] is False and job_row["published_at"].utcoffset() == timedelta(0)
        news = News.model_validate(base | {
            "recordType": "NEWS", "collectedAt": timestamp,
            "content": {"title": "Fixture", "fullText": "Fixture story", "publishedAt": timestamp},
        })
        news_row = await repository.upsert_news(
            news, canonical_key="fixture-news", publication_evidence={"original": timestamp}, **provenance,
        )
        assert news_row["canonical_url"] == "https://example.org/item?q=1"
        assert news_row["source_url"] == startup_json["source"]["url"]

        evidence = IngestionEvidence(connection)
        assert (await evidence.record_extraction_attempt(
            raw_document_id=saved_document["id"], provider="fixture", model_name="fixture",
            chunk_index=0, outcome="FAILED", response_status=429, error_code="RATE_LIMIT",
        ))["response_status"] == 429
        assert (await evidence.record_mapping(
            raw_name="Fixture", canonical_name="Fixture Ltd.", canonical_key="fixture",
            source_url="https://example.org/item", match_method="test", review_status="ACCEPTED",
            confidence=Decimal("1.0"), resolved_at=now,
        ))["canonical_key"] == "fixture"
        assert (await evidence.quarantine(
            raw_document_id=saved_document["id"], record_type=SourceCategory.RESEARCH_PAPER,
            reason_code="MISSING_EVIDENCE", payload={"fixture": True},
        ))["payload"] == {"fixture": True}
