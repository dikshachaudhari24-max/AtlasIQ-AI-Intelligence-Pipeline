import asyncio
import json
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from atlasiq.cli import app
from atlasiq.core.observability import read_dashboard
from atlasiq.crawlers.service import IngestionService
from atlasiq.intelligence import storage
from atlasiq.intelligence.providers import ProviderError, Reply
from atlasiq.intelligence.service import IntelligenceError, IntelligenceService
from test_crawlers import FakeHttp, feed
from test_intelligence import fact, response

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class PureProvider:
    def __init__(self, name, *outcomes):
        self.name, self.model = name, "fixture-model"
        self.outcomes = list(outcomes)
        self.calls = []

    async def generate(self, kind, evidence):
        self.calls.append(evidence)
        item = self.outcomes.pop(0) if self.outcomes else Reply(response(kind=kind))
        if isinstance(item, Exception):
            raise item
        return item


async def ingest(pool, settings, *, paper_id="2601.12345", title="NLP paper", include_repo=False):
    http = FakeHttp(feed(paper_id, title=title, include_repo=include_repo))
    return await IngestionService(pool, settings, http).crawl("papers", 1)


async def test_observable_crawl_to_intelligence_flow_and_independent_replay(pool, disposable_settings, monkeypatch):
    report = await ingest(pool, disposable_settings)
    assert report["inserted"] == 1
    initial = await read_dashboard(pool, "test")
    assert initial["metrics"]["persisted"] == initial["extraction"]["PENDING"] == 1
    assert initial["stages"]["extract"]["state"] == "PENDING"

    provider = PureProvider("fixture", Reply(response(fact(value="NLP", quote="NLP paper")), 21, 7))
    events = []
    worker = IntelligenceService(pool, disposable_settings, [provider], events.append)
    outcome = await worker.run(5)
    assert outcome["enriched"] == outcome["processed"] == 1
    assert "normalize" in {event.stage for event in events}
    assert (await worker.run(5))["processed"] == 0 and len(provider.calls) == 1

    async with pool.acquire() as connection:
        record_id = await connection.fetchval("SELECT id FROM public.research_papers")
        result = await storage.inspect(connection, record_id)
        assert result["status"] == "COMPLETED"
        assert result["usage"] == {"attempts": 1, "input_tokens": 21, "output_tokens": 7}
        assert result["intelligence"]["facts"][0]["normalized"] == "natural language processing"
        assert result["source_url"].startswith("https://arxiv.org/")
    dashboard = await read_dashboard(pool, "test")
    assert dashboard["metrics"]["enriched"] == 1
    assert dashboard["queue"]["COMPLETED"] == 2
    assert dashboard["stages"]["normalize"]["state"] == "COMPLETED"

    monkeypatch.setenv("DATABASE_URL", disposable_settings.asyncpg_dsn())
    runner = CliRunner()
    display = await asyncio.to_thread(runner.invoke, app, ["dashboard", "--json"])
    assert display.exit_code == 0, display.stderr
    assert json.loads(display.stdout)["metrics"]["enriched"] == 1
    inspected = await asyncio.to_thread(runner.invoke, app, ["intelligence", "inspect", "--id", str(record_id), "--json"])
    assert inspected.exit_code == 0, inspected.stderr
    assert json.loads(inspected.stdout)["usage"]["input_tokens"] == 21
    gone = await asyncio.to_thread(runner.invoke, app, ["intelligence", "inspect", "--id", str(uuid4())])
    assert gone.exit_code == 1 and "has that ID" in gone.stderr


async def test_fallback_throttle_then_split_oversize_and_preserve_attempts(pool, disposable_settings):
    title = "Long " * 80 + "source title"
    await ingest(pool, disposable_settings, title=title)
    a = PureProvider("gemini", ProviderError("PROVIDER_HTTP_429", status=429, transient=True, delay=45),
                     ProviderError("PROVIDER_COOLDOWN", transient=True, delay=45))
    b = PureProvider("groq", ProviderError("PROVIDER_HTTP_413", status=413))
    report = await IntelligenceService(pool, disposable_settings, [a, b]).run(1)
    assert report["status"] == "COMPLETED" and report["enriched"] == 1
    async with pool.acquire() as connection:
        result = await connection.fetchrow("SELECT chunk_count, intelligence FROM public.intelligence_records")
        assert result["chunk_count"] >= 2
        codes = await connection.fetch("SELECT error_code FROM public.extraction_attempts WHERE error_code IS NOT NULL")
        assert {"PROVIDER_HTTP_429", "PROVIDER_HTTP_413"} <= {row[0] for row in codes}


async def test_retries_and_quarantines_invalid_extraction(pool, disposable_settings):
    await ingest(pool, disposable_settings)
    settings = disposable_settings.model_copy(update={"llm_provider_attempts": 2, "llm_max_retry_wait_seconds": 1})
    provider = PureProvider("fixture", ProviderError("PROVIDER_TIMEOUT", transient=True),
                            Reply(response(fact(value="invented company"))))
    report = await IntelligenceService(pool, settings, [provider]).run(1)
    assert (report["status"], report["rejected"]) == ("FAILED", 1)
    async with pool.acquire() as connection:
        assert await connection.fetchval("SELECT count(*) FROM public.research_papers") == 1
        assert await connection.fetchval("SELECT count(*) FROM public.intelligence_records") == 0
        assert await connection.fetchval("SELECT reason_code FROM public.quarantine_records") == "UNSUPPORTED_EVIDENCE"
        assert await connection.fetchval("SELECT count(*) FROM public.extraction_attempts") == 2


async def test_parallel_workers_share_claims_and_leave_canonical_unchanged(pool, disposable_settings):
    await ingest(pool, disposable_settings)
    await ingest(pool, disposable_settings, paper_id="2601.12346")
    provider = PureProvider("fixture")
    results = await asyncio.gather(
        IntelligenceService(pool, disposable_settings, [provider]).run(5),
        IntelligenceService(pool, disposable_settings, [provider]).run(5),
    )
    assert sum(result["enriched"] for result in results) == len(provider.calls) == 2
    async with pool.acquire() as connection:
        assert await connection.fetchval("SELECT count(*) FROM public.intelligence_records") == 2
        assert await connection.fetchval("SELECT count(*) FROM public.research_papers") == 2


async def test_rollback_and_expired_lease_recovery(pool, disposable_settings, monkeypatch):
    await ingest(pool, disposable_settings)

    async def simulated_failed_write(*args):
        raise RuntimeError("fixture rollback")

    with monkeypatch.context() as patch:
        patch.setattr(storage, "save", simulated_failed_write)
        with pytest.raises(ExceptionGroup, match="unhandled errors"):
            await IntelligenceService(pool, disposable_settings, [PureProvider("fixture")]).run(1)
    async with pool.acquire() as connection:
        assert await connection.fetchval("SELECT count(*) FROM public.intelligence_records") == 0
        task = await connection.fetchrow("SELECT * FROM public.ingestion_tasks WHERE stage = 'intelligence'")
        assert task["status"] == "CLAIMED"
        await connection.execute("""
            UPDATE public.ingestion_tasks SET lease_expires_at = statement_timestamp() - interval '1 second'
            WHERE id = $1
        """, task["id"])
    retry = await IntelligenceService(pool, disposable_settings, [PureProvider("fixture")]).run(1)
    assert retry["leases_recovered"] == retry["enriched"] == 1


async def test_quota_is_durable_and_failures_can_be_directly_requeued(pool, disposable_settings):
    settings = disposable_settings.model_copy(update={"task_max_attempts": 1})
    await ingest(pool, settings)
    with pytest.raises(IntelligenceError, match="No LLM provider"):
        await IntelligenceService(pool, settings, []).run(1)
    limited = PureProvider("fixture", ProviderError("PROVIDER_HTTP_429", status=429, transient=True, delay=60))
    failed = await IntelligenceService(pool, settings, [limited]).run(2)
    assert failed["failed"] == 1
    repaired = await IntelligenceService(pool, settings, [PureProvider("fixture")]).run(1, retry_failed=True)
    assert repaired["failed_requeued"] == repaired["enriched"] == 1


async def test_intelligence_foreign_keys(pool):
    import asyncpg

    async with pool.acquire() as connection:
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute("""
                INSERT INTO public.intelligence_records
                    (raw_document_id, task_id, record_type, canonical_record_id, extractor_version,
                     source_url, intelligence, chunk_count)
                VALUES ($1, $2, 'NEWS', $3, '1', 'https://example.org/', '{}'::jsonb, 1)
            """, uuid4(), uuid4(), uuid4())
