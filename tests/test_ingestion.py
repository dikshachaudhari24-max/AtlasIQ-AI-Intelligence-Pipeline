import os
from urllib.parse import parse_qs, urlsplit

import pytest

from atlasiq.crawlers.adapters import DEFAULTS
from atlasiq.crawlers.arxiv import ATOM, ArxivCrawler
from atlasiq.crawlers.http_client import FetchError, HttpClient, Response
from atlasiq.crawlers.runs import STAGE, CrawlState
from atlasiq.crawlers.service import IngestionService
from atlasiq.crawlers.validation import validate_stored_records
from atlasiq.db.connection import transaction
from atlasiq.sources.registry import SourceRegistry
from atlasiq.tasks.queue import TaskQueue
from test_crawlers import NOW, FakeHttp, app_listing, feed, github_body

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_paper_pipeline_commits_raw_evidence_and_task_and_replay_is_cheap(pool, disposable_settings):
    http = FakeHttp(feed(), github_body(0))
    first = await IngestionService(pool, disposable_settings, http).crawl("papers", 5)
    assert first["status"] == "COMPLETED" and first["inserted"] == first["validated"] == 1
    replay_http = FakeHttp(feed())
    second = await IngestionService(pool, disposable_settings, replay_http).crawl("papers", 1000)
    assert second["inserted"] == second["updated"] == 0 and second["duplicates"] == 1
    assert len(replay_http.calls) == 1

    new = await IngestionService(pool, disposable_settings, FakeHttp(
        feed(title="Revised Fixture Paper", include_repo=False),
    )).crawl("papers", 5)
    assert new["inserted"] == 0 and new["updated"] == 1
    async with pool.acquire() as connection:
        paper = await connection.fetchrow("SELECT * FROM public.research_papers")
        assert paper["title"] == "Revised Fixture Paper" and paper["abstract"] and paper["collected_at"]
        assert paper["published_date"].isoformat() == "2026-01-05T08:30:00+00:00"
        assert await connection.fetchval("SELECT count(*) FROM public.research_papers") == 1
        assert await connection.fetchval("SELECT count(*) FROM public.raw_documents WHERE status = 'ACCEPTED'") == 2
        assert await connection.fetchval("SELECT count(*) FROM public.ingestion_tasks WHERE status = 'COMPLETED'") == 2
        assert await connection.fetchval("SELECT github_stars FROM public.paper_github_metrics") == 0
        assert (await validate_stored_records(connection))["records"]["research_papers"]["valid"] == 1


async def test_discovery_resumes_from_committed_checkpoint(pool, disposable_settings):
    http = FakeHttp(feed(total=2, include_repo=False), FetchError("HTTP_503", transient=True))
    interrupted = await IngestionService(pool, disposable_settings, http).crawl("papers", 2)
    assert interrupted["status"] == "RUNNING" and interrupted["inserted"] == 1
    assert interrupted["discovery_error"] == "HTTP_503"
    async with pool.acquire() as connection:
        assert await connection.fetchval("SELECT cursor FROM public.crawl_runs WHERE status = 'RUNNING'") == 1
        await connection.execute("UPDATE public.crawl_runs SET discovery_retry_at = clock_timestamp()")

    resumed_http = FakeHttp(feed("2601.12346", start=1, total=2, include_repo=False))
    [resumed] = await IngestionService(pool, disposable_settings, resumed_http).resume()
    assert resumed["crawl_run_id"] == interrupted["crawl_run_id"]
    assert resumed["status"] == "COMPLETED" and resumed["inserted"] == 2
    assert "start=1" in resumed_http.calls[0]
    assert await IngestionService(pool, disposable_settings, FakeHttp()).resume() == []


async def test_repeated_ids_on_paginated_feed_count_once(pool, disposable_settings):
    http = FakeHttp(
        feed(total=3, include_repo=False), feed(start=1, total=3, include_repo=False),
        feed("2601.12346", start=2, total=3, include_repo=False),
    )
    report = await IngestionService(pool, disposable_settings, http).crawl("papers", 2)
    assert (report["discovered"], report["fetched"], report["inserted"], report["duplicates"]) == (2, 3, 2, 1)


async def test_thousand_item_limit_uses_bounded_pages(pool, disposable_settings):
    from xml.etree.ElementTree import fromstring, tostring

    class PaginatedFeed:
        page_sizes = []

        async def get(self, url, policy, media):
            query = parse_qs(urlsplit(url).query)
            start, size = (int(query[key][0]) for key in ("start", "max_results"))
            self.page_sizes.append(size)
            root = fromstring(feed(start=start, total=1001, include_repo=False))
            example = root.find(ATOM + "entry")
            root.remove(example)
            for number in range(start, min(start + size, 1001)):
                entry = tostring(example, encoding="unicode")
                root.append(fromstring(entry.replace("2601.12345", f"2601.{number + 10000:05d}")))
            return Response(tostring(root, encoding="unicode"), NOW)

    source = PaginatedFeed()
    report = await IngestionService(pool, disposable_settings, source).crawl("papers", 1000)
    assert report["status"] == "COMPLETED" and report["inserted"] == 1000
    assert source.page_sizes == [50] * 20


async def test_resume_recovers_expired_lease_without_refetching_completed_page(pool, disposable_settings):
    source = DEFAULTS["papers"]
    page = await ArxivCrawler(source, FakeHttp(feed(include_repo=False))).discover(None, 1)
    async with transaction(pool) as connection:
        await SourceRegistry(connection).register(source)
        runs = CrawlState(connection)
        run = await runs.start(source.id, 1)
        await runs.save_page(run, page, disposable_settings)
        queue = TaskQueue(connection, disposable_settings)
        [claimed] = await queue.claim_tasks("crashed-worker", stage=STAGE, crawl_run_id=run["id"])
        await connection.execute("""
            UPDATE public.ingestion_tasks SET lease_expires_at = clock_timestamp() - interval '1 second'
            WHERE id = $1
        """, claimed["id"])
    [resumed] = await IngestionService(pool, disposable_settings, FakeHttp()).resume()
    assert resumed["inserted"] == 1 and resumed["status"] == "COMPLETED"
    async with pool.acquire() as connection:
        finished = await TaskQueue(connection, disposable_settings).get_task_status(claimed["id"])
        assert finished["status"] == "COMPLETED" and finished["attempts"] == 2


async def test_invalid_product_is_quarantined_and_valid_product_is_persisted(pool, disposable_settings):
    valid = app_listing(key="valid.ai", payment="free")
    invalid = app_listing(key="unknown.ai", payment=None)
    http = FakeHttp({"_embedded": {"addons": [valid, invalid]}}, {"paymentModel": None})
    report = await IngestionService(pool, disposable_settings, http).crawl("products", 5)
    assert (report["status"], report["inserted"], report["rejected"]) == ("PARTIAL", 1, 1)
    async with pool.acquire() as connection:
        assert await connection.fetchval("SELECT pricing_model FROM public.products") == "FREE"
        assert await connection.fetchval("SELECT reason_code FROM public.quarantine_records") == "MARKETPLACE_MISSING_PRICING"
        assert await connection.fetchval("""
            SELECT processing_evidence IS NOT NULL FROM public.raw_documents WHERE status = 'QUARANTINED'
        """)


async def test_product_task_failure_is_durable_and_resumes(pool, disposable_settings):
    listing = {"_embedded": {"addons": [app_listing(payment=None)]}}
    report = await IngestionService(pool, disposable_settings, FakeHttp(
        listing, FetchError("HTTP_503", status=503, transient=True, delay=15),
    )).crawl("products", 1)
    assert report["status"] == "RUNNING" and report["unfinished_tasks"] == 1
    async with pool.acquire() as connection:
        row = await connection.fetchrow("SELECT * FROM public.ingestion_tasks")
        assert row["status"] == "RETRY" and row["last_error"] == "HTTP_503"
        assert (row["next_retry_at"] - row["updated_at"]).total_seconds() > 14
        await connection.execute("UPDATE public.ingestion_tasks SET next_retry_at = clock_timestamp()")
    [recovered] = await IngestionService(pool, disposable_settings, FakeHttp({"paymentModel": "atlassian"})).resume()
    assert recovered["inserted"] == 1 and recovered["status"] == "COMPLETED"


@pytest.mark.live_source
@pytest.mark.skipif(os.environ.get("ATLASIQ_LIVE_TESTS") != "1", reason="set ATLASIQ_LIVE_TESTS=1 for public API requests")
async def test_live_arxiv_github_postgres_end_to_end(pool, disposable_settings):
    source = DEFAULTS["papers"].model_copy(update={
        "adapter_config": {"id_list": "2103.00020"}, "id": "papers-clip",
    })
    async with pool.acquire() as connection:
        await SourceRegistry(connection).register(source)
    async with HttpClient(disposable_settings) as http:
        report = await IngestionService(pool, disposable_settings, http).crawl(source.id, 1)
    assert report["status"] == "COMPLETED", report
    assert report["inserted"] == report["fetched"] == report["validated"] == 1
    async with pool.acquire() as connection:
        row = await connection.fetchrow("""
            SELECT paper.arxiv_id, paper.paper_url, raw.raw_content, task.status,
                   metric.github_stars, metric.association_evidence
            FROM public.research_papers AS paper
            JOIN public.raw_documents AS raw ON raw.id = paper.raw_document_id
            JOIN public.ingestion_tasks AS task ON task.raw_document_id = raw.id
            JOIN public.paper_github_metrics AS metric ON metric.paper_id = paper.id
            WHERE metric.repository_identity = 'openai/clip'
        """)
    assert row is not None
    assert row["status"] == "COMPLETED" and row["arxiv_id"] == "2103.00020"
    assert "https://github.com/OpenAI/CLIP" in row["raw_content"]
    assert isinstance(row["github_stars"], int) and row["github_stars"] >= 0
    assert row["association_evidence"]["kind"] == "arxiv_abstract_link"
