import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from uuid import UUID, uuid4

import asyncpg
from pydantic import ValidationError

from atlasiq.config.settings import Settings
from atlasiq.crawlers.adapters import ADAPTERS, DEFAULTS
from atlasiq.crawlers.base import BaseCrawler, InvalidRecord
from atlasiq.crawlers.http_client import FetchError, HttpClient
from atlasiq.crawlers.persistence import persist_record
from atlasiq.crawlers.runs import STAGE, CrawlState
from atlasiq.db.connection import transaction
from atlasiq.db.repositories import IngestionEvidence, RawDocuments
from atlasiq.enrichment.github import GitHubClient, associations
from atlasiq.schemas.entities import ResearchPaper
from atlasiq.sources.registry import SourceAlreadyRegistered, SourceConfig, SourceRegistry
from atlasiq.tasks.queue import LeaseLost, TaskQueue

logger = logging.getLogger(__name__)
SOURCE_FIELDS = frozenset(SourceConfig.model_fields)


class CrawlError(Exception):
    pass


class IngestionService:
    def __init__(self, pool: asyncpg.Pool, settings: Settings, http: HttpClient):
        self.pool = pool
        self.settings = settings
        self.http = http
        self.github = GitHubClient(http, settings)
        self.worker_id = "worker-" + uuid4().hex

    async def _adapter(self, source_id: str, *, register_default: bool = False) -> BaseCrawler:
        async with self.pool.acquire() as connection:
            registry = SourceRegistry(connection)
            row = await registry.get(source_id)
            if row is None and register_default and source_id in DEFAULTS:
                try:
                    row = await registry.register(DEFAULTS[source_id])
                except SourceAlreadyRegistered:
                    row = await registry.get(source_id)
        if row is None:
            raise CrawlError("Source is unknown. Register its configuration or select papers, startups, or products.")
        if not row["enabled"]:
            raise CrawlError("Source is disabled. Enable it with the sources enable command.")
        adapter = ADAPTERS.get(row["adapter_name"])
        if adapter is None or row["category"] != adapter.category:
            raise CrawlError("Source has no supported adapter for its category.")
        try:
            return adapter(SourceConfig.model_validate({key: row[key] for key in SOURCE_FIELDS}), self.http)
        except ValidationError:
            raise CrawlError("Source settings are invalid for its adapter. Check the Phase 2 setup guide.") from None

    @asynccontextmanager
    async def _source_lock(self, source_id: str):
        async with self.pool.acquire() as connection:
            acquired = await connection.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 21))", source_id)
            try:
                yield acquired
            finally:
                if acquired:
                    await connection.execute("SELECT pg_advisory_unlock(hashtextextended($1, 21))", source_id)

    async def crawl(self, source_id: str, limit: int) -> dict:
        if not 1 <= limit <= 1_000_000:
            raise CrawlError("Crawl limit must be between 1 and 1,000,000.")
        adapter = await self._adapter(source_id, register_default=True)
        async with self._source_lock(source_id) as acquired:
            if not acquired:
                raise CrawlError("A worker is already running this source.")
            async with self.pool.acquire() as connection:
                state = CrawlState(connection)
                if await state.active(source_id):
                    raise CrawlError("An unfinished run exists for this source. Use resume before starting another crawl.")
                run = await state.start(source_id, limit)
            return await self._drive(run, adapter)

    async def resume(self) -> list[dict]:
        async with self.pool.acquire() as connection:
            active = await CrawlState(connection).active()
        summaries = []
        for run in active:
            source_id = run["source_id"]
            try:
                adapter = await self._adapter(source_id)
                async with self._source_lock(source_id) as acquired:
                    if not acquired:
                        summaries.append({"source_id": source_id, "crawl_run_id": str(run["id"]),
                                          "status": "BUSY", "error": "Another worker holds the source lock."})
                        continue
                    async with self.pool.acquire() as connection:
                        latest = await CrawlState(connection).active(source_id)
                    if latest and latest[0]["id"] == run["id"]:
                        summaries.append(await self._drive(latest[0], adapter))
            except CrawlError as error:
                summaries.append({"source_id": source_id, "crawl_run_id": str(run["id"]),
                                  "status": "BLOCKED", "error": str(error)})
        return summaries

    async def _drive(self, run: asyncpg.Record, adapter: BaseCrawler) -> dict:
        self._log("crawl.start", "Crawl processing started", run)
        while True:
            await self._drain(run["id"], adapter)
            if run["discovery_done"] or (
                run["discovery_retry_at"] is not None and run["discovery_retry_at"] > datetime.now(UTC)
            ):
                break
            try:
                page = await adapter.discover(run["cursor"], run["requested_limit"] - run["discovered_count"])
                if page.next_cursor is not None and page.next_cursor == run["cursor"]:
                    raise FetchError("CURSOR_DID_NOT_ADVANCE")
            except FetchError as error:
                self._log("crawl.discovery_failure", error.code, run, level=logging.WARNING)
                async with self.pool.acquire() as connection:
                    run = await CrawlState(connection).discovery_failure(
                        run["id"], error.code, terminal=not error.transient,
                        delay=max(error.delay, self.settings.task_retry_base_seconds),
                    )
                break
            async with transaction(self.pool) as connection:
                run = await CrawlState(connection).save_page(run, page, self.settings)
            self._log("crawl.page", "Discovery page committed", run, discovered=run["discovered_count"])
        async with self.pool.acquire() as connection:
            report = await CrawlState(connection).report(run, adapter.source.name)
        counts = {key: value for key, value in report.items() if type(value) is int}
        self._log("crawl.report", report["status"], run, **counts)
        return report

    async def _drain(self, run_id: UUID, adapter: BaseCrawler) -> None:
        workers = self.settings.crawler_worker_concurrency
        while True:
            async with self.pool.acquire() as connection:
                queue = TaskQueue(connection, self.settings)
                await queue.recover_expired_leases(stage=STAGE, crawl_run_id=run_id)
                tasks = await queue.claim_tasks(self.worker_id, stage=STAGE, limit=workers, crawl_run_id=run_id)
            if not tasks:
                return
            async with asyncio.TaskGroup() as group:
                for task in tasks:
                    group.create_task(self._process(task, adapter))

    async def _heartbeat(self, task: asyncpg.Record, lease_lost: asyncio.Event) -> None:
        while True:
            await asyncio.sleep(self.settings.task_lease_seconds / 3)
            try:
                async with self.pool.acquire() as connection:
                    await TaskQueue(connection, self.settings).renew_lease(task["id"], task["claim_token"])
            except LeaseLost:
                lease_lost.set()
                return

    async def _process(self, task: asyncpg.Record, adapter: BaseCrawler) -> None:
        lease_lost = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat(task, lease_lost))
        details = None
        try:
            async with self.pool.acquire() as connection:
                document = await RawDocuments(connection).get(task["raw_document_id"])
            try:
                raw = document["raw_content"]
                if raw is None:
                    raise InvalidRecord("MISSING_INLINE_EVIDENCE")
                details = await adapter.fetch(raw)
                parsed = adapter.parse(details, document["source_url"], document["fetched_at"])
                metrics = []
                if isinstance(parsed.record, ResearchPaper):
                    references = associations(parsed.metadata["links"], parsed.metadata["abstract"], parsed.record.source.url)
                    metrics = await self.github.collect(references)
                if lease_lost.is_set():
                    raise LeaseLost
                async with transaction(self.pool) as connection:
                    result = await persist_record(connection, parsed, document, metrics)
                    await self._settle(connection, task, result, details if details != raw else None)
            except (InvalidRecord, ValidationError) as error:
                reason = error.code if isinstance(error, InvalidRecord) else "CANONICAL_SCHEMA_INVALID"
                async with transaction(self.pool) as connection:
                    evidence = IngestionEvidence(connection)
                    await evidence.quarantine(
                        raw_document_id=document["id"], record_type=adapter.category, reason_code=reason,
                        payload={"validation_types": [issue["type"] for issue in error.errors(
                            include_input=False, include_context=False, include_url=False,
                        )]} if isinstance(error, ValidationError) else {},
                    )
                    await self._settle(connection, task, "REJECTED",
                                       details if details != document["raw_content"] else None)
            except FetchError as error:
                async with self.pool.acquire() as connection:
                    await TaskQueue(connection, self.settings).fail_task(
                        task["id"], task["claim_token"], error_code=error.code,
                        permanent=not error.transient, minimum_delay=error.delay,
                    )
        except LeaseLost:
            logger.warning("Task lease lost", extra={
                "operation": "task.lease_lost", "safe_message": "LEASE_LOST", "task_id": task["id"],
                "worker_id": self.worker_id, "source_id": adapter.source.id,
            })
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat

    async def _settle(
        self, connection: asyncpg.Connection, task: asyncpg.Record, result: str, additional_evidence: str | None,
    ) -> None:
        await TaskQueue(connection, self.settings).complete_task(task["id"], task["claim_token"])
        await connection.execute("""
            UPDATE public.ingestion_tasks SET processing_result = $2 WHERE id = $1
        """, task["id"], result)
        await connection.execute("""
            UPDATE public.raw_documents SET status = $2, processing_evidence = coalesce($3, processing_evidence)
            WHERE id = $1
        """, task["raw_document_id"], "QUARANTINED" if result == "REJECTED" else "ACCEPTED",
            {"adapter_payload": additional_evidence} if additional_evidence is not None else None)

    @staticmethod
    def _log(operation: str, message: str, run: asyncpg.Record, *, level: int = logging.INFO, **counts) -> None:
        logger.log(level, message, extra={
            "operation": operation, "safe_message": message, "source_id": run["source_id"],
            "crawl_run_id": run["id"], **counts,
        })
