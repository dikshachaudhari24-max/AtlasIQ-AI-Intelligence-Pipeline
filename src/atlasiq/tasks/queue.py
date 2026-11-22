import re
from uuid import UUID

import asyncpg

from atlasiq.config.settings import Settings
from atlasiq.db.models import TaskStatus

ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}")


class LeaseLost(Exception):
    """The task does not have an active lease for that claim."""


def _positive_db_integer(value: int, name: str) -> int:
    if type(value) is not int or not 1 <= value <= 2_147_483_647:
        raise ValueError(f"{name} must be a positive PostgreSQL integer")
    return value


class TaskQueue:
    def __init__(self, connection: asyncpg.Connection, settings: Settings):
        self.connection = connection
        self.settings = settings

    async def enqueue_task(
        self, raw_document_id: UUID, stage: str, input_version: str, *, max_attempts: int | None = None
    ) -> asyncpg.Record:
        attempts = self.settings.task_max_attempts if max_attempts is None else max_attempts
        _positive_db_integer(attempts, "max_attempts")
        async with self.connection.transaction():
            row = await self.connection.fetchrow("""
                INSERT INTO public.ingestion_tasks (raw_document_id, stage, input_version, max_attempts)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (raw_document_id, stage, input_version) DO NOTHING
                RETURNING *
            """, raw_document_id, stage, input_version, attempts)
            if row is not None:
                return row
            # A separate statement sees the winning INSERT under READ COMMITTED.
            return await self.connection.fetchrow("""
                SELECT * FROM public.ingestion_tasks
                WHERE raw_document_id = $1 AND stage = $2 AND input_version = $3
            """, raw_document_id, stage, input_version)

    async def claim_tasks(
        self, worker_id: str, *, limit: int = 1, stage: str | None = None, crawl_run_id: UUID | None = None,
    ) -> list[asyncpg.Record]:
        if not worker_id.strip():
            raise ValueError("worker_id must be nonempty")
        _positive_db_integer(limit, "limit")
        return await self.connection.fetch("""
            WITH ready AS MATERIALIZED (
                SELECT id, next_retry_at FROM public.ingestion_tasks
                WHERE status IN ('PENDING', 'RETRY')
                  AND next_retry_at <= statement_timestamp() AND attempts < max_attempts
                  AND ($2::text IS NULL OR stage = $2)
                  AND ($5::uuid IS NULL OR EXISTS (
                      SELECT 1 FROM public.crawl_run_items AS item
                      WHERE item.crawl_run_id = $5 AND item.task_id = ingestion_tasks.id
                  ))
                ORDER BY next_retry_at, id
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            ), claimed AS (
                UPDATE public.ingestion_tasks AS task
                SET status = 'CLAIMED', attempts = attempts + 1, lease_owner = $1,
                    lease_expires_at = clock_timestamp() + make_interval(secs => $4::integer),
                    claim_token = gen_random_uuid(), next_retry_at = NULL, updated_at = clock_timestamp()
                FROM ready WHERE task.id = ready.id
                RETURNING task.*
            )
            SELECT claimed.* FROM claimed JOIN ready USING (id) ORDER BY ready.next_retry_at, id
        """, worker_id, stage, limit, self.settings.task_lease_seconds, crawl_run_id)

    async def _lock_task(self, task_id: UUID) -> asyncpg.Record:
        row = await self.connection.fetchrow("""
            SELECT * FROM public.ingestion_tasks WHERE id = $1 FOR UPDATE
        """, task_id)
        if row is None:
            raise LeaseLost
        return row

    async def renew_lease(self, task_id: UUID, claim_token: UUID) -> asyncpg.Record:
        async with self.connection.transaction():
            await self._lock_task(task_id)
            row = await self.connection.fetchrow("""
                UPDATE public.ingestion_tasks
                SET lease_expires_at = greatest(
                        lease_expires_at, clock_timestamp() + make_interval(secs => $3::integer)),
                    updated_at = clock_timestamp()
                WHERE id = $1 AND claim_token = $2 AND status = 'CLAIMED'
                  AND lease_expires_at > clock_timestamp()
                RETURNING *
            """, task_id, claim_token, self.settings.task_lease_seconds)
            if row is None:
                raise LeaseLost
            return row

    async def complete_task(self, task_id: UUID, claim_token: UUID) -> asyncpg.Record:
        async with self.connection.transaction():
            current = await self._lock_task(task_id)
            if current["status"] == TaskStatus.COMPLETED and current["claim_token"] == claim_token:
                return current
            row = await self.connection.fetchrow("""
                UPDATE public.ingestion_tasks
                SET status = 'COMPLETED', lease_owner = NULL, lease_expires_at = NULL,
                    updated_at = clock_timestamp()
                WHERE id = $1 AND claim_token = $2 AND status = 'CLAIMED'
                  AND lease_expires_at > clock_timestamp()
                RETURNING *
            """, task_id, claim_token)
            if row is None:
                raise LeaseLost
            return row

    async def fail_task(
        self, task_id: UUID, claim_token: UUID, *, error_code: str, permanent: bool = False,
        minimum_delay: float = 0,
    ) -> asyncpg.Record:
        if not ERROR_CODE.fullmatch(error_code):
            raise ValueError("error_code must be a short uppercase category code")
        async with self.connection.transaction():
            await self._lock_task(task_id)
            row = await self.connection.fetchrow("""
                UPDATE public.ingestion_tasks
                SET status = CASE WHEN $6 OR attempts >= max_attempts THEN 'FAILED' ELSE 'RETRY' END,
                    next_retry_at = CASE WHEN NOT $6 AND attempts < max_attempts THEN clock_timestamp() + make_interval(
                        secs => greatest($7::double precision, least($5::double precision, $4::double precision
                                         * power(2::double precision, least(attempts - 1, 31))))
                    ) END,
                    lease_owner = NULL, lease_expires_at = NULL, claim_token = NULL,
                    last_error = $3, updated_at = clock_timestamp()
                WHERE id = $1 AND claim_token = $2 AND status = 'CLAIMED'
                  AND lease_expires_at > clock_timestamp()
                RETURNING *
            """, task_id, claim_token, error_code, self.settings.task_retry_base_seconds,
                self.settings.task_retry_max_seconds, permanent, minimum_delay)
            if row is None:
                raise LeaseLost
            return row

    async def recover_expired_leases(
        self, *, limit: int = 100, stage: str | None = None, crawl_run_id: UUID | None = None,
    ) -> list[asyncpg.Record]:
        _positive_db_integer(limit, "limit")
        return await self.connection.fetch("""
            WITH expired AS MATERIALIZED (
                SELECT id FROM public.ingestion_tasks
                WHERE status = 'CLAIMED' AND lease_expires_at <= statement_timestamp()
                  AND ($2::text IS NULL OR stage = $2)
                  AND ($3::uuid IS NULL OR EXISTS (
                      SELECT 1 FROM public.crawl_run_items AS item
                      WHERE item.crawl_run_id = $3 AND item.task_id = ingestion_tasks.id
                  ))
                ORDER BY lease_expires_at, id LIMIT $1 FOR UPDATE SKIP LOCKED
            )
            UPDATE public.ingestion_tasks AS task
            SET status = CASE WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'RETRY' END,
                next_retry_at = CASE WHEN attempts < max_attempts THEN clock_timestamp() END,
                lease_owner = NULL, lease_expires_at = NULL, claim_token = NULL,
                last_error = 'LEASE_EXPIRED', updated_at = clock_timestamp()
            FROM expired WHERE task.id = expired.id
            RETURNING task.*
        """, limit, stage, crawl_run_id)

    async def get_task_status(self, task_id: UUID) -> asyncpg.Record | None:
        return await self.connection.fetchrow("SELECT * FROM public.ingestion_tasks WHERE id = $1", task_id)

    async def status_counts(self) -> dict[str, int]:
        counts = {status.value: 0 for status in TaskStatus}
        counts.update(await self.connection.fetch("""
            SELECT status, count(*) FROM public.ingestion_tasks GROUP BY status
        """))
        return counts
