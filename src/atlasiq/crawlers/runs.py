from datetime import datetime
from uuid import UUID

import asyncpg

from atlasiq.config.settings import Settings
from atlasiq.crawlers.base import Page
from atlasiq.db.repositories import RawDocuments
from atlasiq.tasks.queue import TaskQueue
from atlasiq.utils.urls import canonicalize_url

STAGE = "deterministic-ingestion"
INPUT_VERSION = "1"


class CrawlState:
    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    async def start(self, source_id: str, limit: int) -> asyncpg.Record:
        return await self.connection.fetchrow("""
            INSERT INTO public.crawl_runs (source_id, status, started_at, requested_limit)
            VALUES ($1, 'RUNNING', clock_timestamp(), $2) RETURNING *
        """, source_id, limit)

    async def active(self, source_id: str | None = None) -> list[asyncpg.Record]:
        return await self.connection.fetch("""
            SELECT * FROM public.crawl_runs
            WHERE status = 'RUNNING' AND requested_limit IS NOT NULL
              AND ($1::text IS NULL OR source_id = $1)
            ORDER BY started_at, id
        """, source_id)

    async def save_page(self, run: asyncpg.Record, page: Page, settings: Settings) -> asyncpg.Record:
        remaining = run["requested_limit"] - run["discovered_count"]
        added = duplicates = 0
        documents = RawDocuments(self.connection)
        queue = TaskQueue(self.connection, settings)
        for item in page.items:
            if added == remaining:
                break
            key = f"id:{item.upstream_id}" if item.upstream_id is not None else f"url:{canonicalize_url(item.url)}"
            if await self.connection.fetchval("""
                SELECT EXISTS (SELECT 1 FROM public.crawl_run_items WHERE crawl_run_id = $1 AND discovery_key = $2)
            """, run["id"], key):
                duplicates += 1
                continue
            document = await documents.save(
                source_id=run["source_id"], crawl_run_id=run["id"], upstream_id=item.upstream_id,
                url=item.url, raw_content=item.raw, fetched_at=page.fetched_at,
                original_timestamp_evidence={"request_url": page.request_url},
            )
            task = await queue.enqueue_task(document["id"], STAGE, INPUT_VERSION)
            await self.connection.execute("""
                INSERT INTO public.crawl_run_items (crawl_run_id, discovery_key, task_id, reused)
                VALUES ($1, $2, $3, $4)
            """, run["id"], key, task["id"], task["created_at"] < run["started_at"])
            added += 1
        done = page.next_cursor is None or added == remaining
        return await self.connection.fetchrow("""
            UPDATE public.crawl_runs
            SET cursor = $2, discovery_done = $3,
                discovered_count = discovered_count + $4, duplicate_discoveries = duplicate_discoveries + $5,
                discovery_retry_at = NULL, failure_summary = NULL
            WHERE id = $1 RETURNING *
        """, run["id"], page.next_cursor, done, added, duplicates)

    async def discovery_failure(
        self, run_id: UUID, code: str, *, terminal: bool, delay: float,
    ) -> asyncpg.Record:
        return await self.connection.fetchrow("""
            UPDATE public.crawl_runs
            SET failure_summary = $2, discovery_done = $3,
                discovery_retry_at = CASE WHEN NOT $3 THEN clock_timestamp() + make_interval(secs => $4) END
            WHERE id = $1 RETURNING *
        """, run_id, code, terminal, delay)

    async def stats(self, run_id: UUID) -> dict:
        result = await self.connection.fetchrow("""
            SELECT count(*) AS fetched,
                   count(*) FILTER (WHERE task.processing_result IN ('INSERTED', 'UPDATED')) AS validated,
                   count(*) FILTER (WHERE NOT item.reused AND task.processing_result = 'INSERTED') AS inserted,
                   count(*) FILTER (WHERE NOT item.reused AND task.processing_result = 'UPDATED') AS updated,
                   count(*) FILTER (WHERE item.reused) AS reused,
                   count(*) FILTER (WHERE task.processing_result = 'REJECTED') AS rejected,
                   count(*) FILTER (WHERE task.status = 'FAILED') AS failed_tasks,
                   count(*) FILTER (WHERE task.status IN ('PENDING', 'CLAIMED', 'RETRY')) AS unfinished_tasks,
                   min(coalesce(task.next_retry_at, task.lease_expires_at))
                        FILTER (WHERE task.status IN ('PENDING', 'CLAIMED', 'RETRY')) AS next_eligible_at
            FROM public.crawl_run_items AS item
            JOIN public.ingestion_tasks AS task ON task.id = item.task_id
            WHERE item.crawl_run_id = $1
        """, run_id)
        return dict(result)

    async def report(self, run: asyncpg.Record, name: str) -> dict:
        stats = await self.stats(run["id"])
        errors = stats["rejected"] or stats["failed_tasks"] or run["failure_summary"]
        finished = run["discovery_done"] and not stats["unfinished_tasks"]
        status = "RUNNING"
        if finished:
            status = "COMPLETED" if not errors else ("PARTIAL" if stats["validated"] else "FAILED")
        await self.connection.execute("""
            UPDATE public.crawl_runs
            SET status = $2, finished_at = CASE WHEN $2 = 'RUNNING' THEN NULL ELSE clock_timestamp() END,
                accepted_count = $3, rejected_count = $4
            WHERE id = $1
        """, run["id"], status, stats["validated"], stats["rejected"])
        next_eligible = [at for at in (run["discovery_retry_at"], stats["next_eligible_at"]) if at is not None]
        return {
            "source_id": run["source_id"], "source_name": name, "crawl_run_id": str(run["id"]),
            "started_at": run["started_at"].isoformat(), "requested_limit": run["requested_limit"],
            "status": status, "discovered": run["discovered_count"],
            "fetched": stats["fetched"] + run["duplicate_discoveries"],
            "validated": stats["validated"], "inserted": stats["inserted"], "updated": stats["updated"],
            "duplicates": run["duplicate_discoveries"] + stats["reused"],
            "rejected": stats["rejected"], "failed_tasks": stats["failed_tasks"],
            "unfinished_tasks": stats["unfinished_tasks"], "discovery_error": run["failure_summary"],
            "next_eligible_at": min(next_eligible).isoformat() if next_eligible else None,
        }
