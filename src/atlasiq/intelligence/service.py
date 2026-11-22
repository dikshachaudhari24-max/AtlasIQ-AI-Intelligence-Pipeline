import asyncio
import logging
import random
from collections import deque
from collections.abc import Sequence
from contextlib import suppress
from time import perf_counter
from uuid import uuid4

import asyncpg

from atlasiq.config.settings import Settings
from atlasiq.core.progress import Observer, emit
from atlasiq.db.connection import transaction
from atlasiq.db.repositories import IngestionEvidence
from atlasiq.intelligence import storage
from atlasiq.intelligence.contracts import InvalidOutput, checked_facts, merge_facts
from atlasiq.intelligence.evidence import semantic_chunks, smaller_chunks, source_text
from atlasiq.intelligence.providers import Provider, ProviderError
from atlasiq.tasks.queue import LeaseLost, TaskQueue
from atlasiq.utils.urls import validate_source_url

logger = logging.getLogger(__name__)


class IntelligenceError(Exception):
    pass


class IntelligenceService:
    def __init__(
        self, pool: asyncpg.Pool, settings: Settings, providers: Sequence[Provider], observer: Observer | None = None,
    ):
        self.pool = pool
        self.settings = settings
        self.providers = providers
        self.observer = observer
        self.worker_id = "intel-" + uuid4().hex
        self.disabled: set[str] = set()

    async def run(self, limit: int, *, retry_failed: bool = False) -> dict:
        if not self.providers:
            raise IntelligenceError("No LLM provider credentials configured. Set GEMINI_API_KEY, GROQ_API_KEY or DEEPSEEK_API_KEY.")
        if not 1 <= limit <= 1000:
            raise IntelligenceError("Intelligence limit must be between 1 and 1,000.")
        started = perf_counter()
        total = dict.fromkeys(("processed", "enriched", "rejected", "retrying", "failed", "skipped"), 0)
        async with self.pool.acquire() as connection:
            queue = TaskQueue(connection, self.settings)
            recovered = await queue.recover_expired_leases(stage=storage.STAGE, limit=limit)
            retried = await storage.retry_failed(connection, limit) if retry_failed else 0
        remaining = limit
        scheduled = 0
        backfilled = False
        while remaining:
            async with self.pool.acquire() as connection:
                queue = TaskQueue(connection, self.settings)
                tasks = await queue.claim_tasks(
                    self.worker_id, limit=min(remaining, self.settings.llm_workers), stage=storage.STAGE,
                )
                if not tasks and not backfilled:
                    scheduled = await storage.schedule(connection, self.settings, remaining)
                    backfilled = True
                    tasks = await queue.claim_tasks(
                        self.worker_id, limit=min(remaining, self.settings.llm_workers), stage=storage.STAGE,
                    )
            if not tasks:
                break
            emit(self.observer, "extract", "RUNNING", f"{len(tasks)} leased tasks", **total)
            async with asyncio.TaskGroup() as group:
                jobs = [group.create_task(self._process(task)) for task in tasks]
            for job in jobs:
                total[job.result()] += 1
            total["processed"] += len(tasks)
            remaining -= len(tasks)
            emit(self.observer, "intelligence", "RUNNING", "Task outcomes committed", **total)
        status = "COMPLETED" if not (total["failed"] or total["rejected"] or total["retrying"]) else (
            "PARTIAL" if total["enriched"] else "RETRYING" if total["retrying"] else "FAILED"
        )
        if not total["processed"]:
            status = "SKIPPED"
        emit(self.observer, "extract", status, "Extraction task outcomes are persisted", **total)
        if total["enriched"]:
            emit(self.observer, "intelligence", "COMPLETED", "Source-checked results committed", **total)
        return {
            **total, "status": status, "requested_limit": limit, "scheduled": scheduled,
            "leases_recovered": len(recovered), "failed_requeued": retried,
            "providers": [{"provider": provider.name, "model": provider.model} for provider in self.providers],
            "duration_seconds": round(perf_counter() - started, 3),
        }

    async def _heartbeat(self, task: asyncpg.Record) -> None:
        while True:
            await asyncio.sleep(self.settings.task_lease_seconds / 3)
            try:
                async with self.pool.acquire() as connection:
                    await TaskQueue(connection, self.settings).renew_lease(task["id"], task["claim_token"])
            except LeaseLost:
                return

    async def _process(self, task: asyncpg.Record) -> str:
        heartbeat = asyncio.create_task(self._heartbeat(task))
        try:
            async with self.pool.acquire() as connection:
                candidate = await storage.candidate_for(connection, task)
            if candidate is None:
                await self._settle(task)
                return "skipped"
            try:
                validate_source_url(candidate["source_url"])
            except ValueError:
                await self._settle(task, candidate=candidate, reason="INVALID_SOURCE_URL")
                return "rejected"
            try:
                facts, count = await self._extract(task, candidate)
                emit(self.observer, "normalize", "RUNNING", "Merging source-checked facts", candidate["source_id"])
                final = merge_facts(facts)
                emit(self.observer, "normalize", "COMPLETED", "Source-checked facts merged", candidate["source_id"])
                await self._settle(task, candidate=candidate, facts=final, count=count)
            except InvalidOutput as error:
                await self._settle(task, candidate=candidate, reason=str(error))
                return "rejected"
            except ProviderError as error:
                async with self.pool.acquire() as connection:
                    row = await TaskQueue(connection, self.settings).fail_task(
                        task["id"], task["claim_token"], error_code=error.code,
                        minimum_delay=error.delay, permanent=False,
                    )
                emit(self.observer, "extract", "RETRYING" if row["status"] == "RETRY" else "FAILED", error.code,
                     candidate["source_id"])
                return "retrying" if row["status"] == "RETRY" else "failed"
            return "enriched"
        except LeaseLost:
            return "skipped"
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat

    async def _extract(self, task: asyncpg.Record, candidate: dict) -> tuple[list[list[dict]], int]:
        evidence = source_text(candidate)
        try:
            chunks = semantic_chunks(evidence, self.settings.llm_chunk_bytes, self.settings.llm_max_chunks)
        except ValueError:
            raise InvalidOutput("CHUNK_BUDGET_EXCEEDED") from None
        if not chunks:
            raise InvalidOutput("NO_SOURCE_EVIDENCE")
        pending = deque((chunk, 0) for chunk in chunks)
        result = []
        attempts = 0
        while pending:
            chunk, depth = pending.popleft()
            remaining = self.settings.llm_attempt_budget - attempts
            if remaining <= 0:
                raise InvalidOutput("EXTRACTION_ATTEMPT_BUDGET")
            can_split = depth < 4 and len(result) + len(pending) + 4 <= self.settings.llm_max_chunks
            payload, used = await self._chunk(task, candidate, chunk, len(result), can_split, remaining)
            attempts += used
            if isinstance(payload, str):
                pieces = smaller_chunks(chunk)
                pending.extendleft((piece, depth + 1) for piece in reversed(pieces))
            else:
                result.append(payload)
        return result, len(result)

    async def _chunk(
        self, task: asyncpg.Record, candidate: dict, chunk: str, index: int, can_split: bool, budget: int,
    ) -> tuple[list[dict] | str, int]:
        errors = []
        invalid = None
        count = 0
        for provider in self.providers:
            if provider.name in self.disabled:
                continue
            for attempt in range(self.settings.llm_provider_attempts):
                if count >= budget:
                    break
                start = perf_counter()
                reply = None
                failure = None
                facts = []
                count += 1
                emit(self.observer, "extract", "RUNNING", f"{provider.name} / {provider.model}", candidate["source_id"])
                try:
                    reply = await provider.generate(candidate["kind"], chunk)
                    facts = checked_facts(reply.text, chunk, candidate["kind"])
                except ProviderError as error:
                    failure = error
                    errors.append(error)
                    if error.code == "INVALID_PROVIDER_RESPONSE":
                        invalid = InvalidOutput(error.code)
                except InvalidOutput as error:
                    failure = ProviderError(str(error), status=200)
                    invalid = error
                async with self.pool.acquire() as connection:
                    await storage.record_attempt(
                        connection, task, provider=provider.name, model=provider.model, chunk=index,
                        attempt=attempt, status=failure.status if failure else 200, code=failure.code if failure else None,
                        elapsed_ms=int((perf_counter() - start) * 1000),
                        input_tokens=reply.input_tokens if reply else None,
                        output_tokens=reply.output_tokens if reply else None,
                    )
                logger.info("Extraction attempt", extra={
                    "operation": "intelligence.provider", "safe_message": failure.code if failure else "ACCEPTED",
                    "source_id": candidate["source_id"], "task_id": task["id"], "worker_id": self.worker_id,
                    "http_status": failure.status if failure else 200,
                })
                if failure is None:
                    return [
                        {**fact, "chunk_index": index, "provider": provider.name, "model": provider.model}
                        for fact in facts
                    ], count
                if failure.status == 413 and can_split and smaller_chunks(chunk):
                    return "split", count
                if failure.status in {401, 403, 404}:
                    self.disabled.add(provider.name)
                retry = failure.transient and attempt + 1 < self.settings.llm_provider_attempts and count < budget
                if not retry or failure.delay > self.settings.llm_max_retry_wait_seconds:
                    break
                wait = min(self.settings.llm_max_retry_wait_seconds, 2 ** attempt + random.random())
                emit(self.observer, "extract", "RETRYING", failure.code, candidate["source_id"])
                await asyncio.sleep(max(wait, failure.delay))
        transient = [error for error in errors if error.transient]
        if transient:
            raise max(transient, key=lambda error: error.delay)
        if invalid:
            raise invalid
        if errors and all(error.status == 413 for error in errors):
            raise InvalidOutput("PROVIDER_PAYLOAD_LIMIT")
        raise ProviderError(errors[-1].code if errors else "PROVIDERS_UNAVAILABLE")

    async def _settle(
        self, task: asyncpg.Record, *, candidate: dict | None = None, facts: list[dict] | None = None,
        reason: str | None = None, count: int = 0,
    ) -> None:
        async with transaction(self.pool) as connection:
            await TaskQueue(connection, self.settings).complete_task(task["id"], task["claim_token"])
            if reason:
                await IngestionEvidence(connection).quarantine(
                    raw_document_id=task["raw_document_id"], record_type=candidate["kind"], reason_code=reason,
                    payload={"stage": storage.STAGE, "task_id": str(task["id"])},
                )
            elif facts is not None:
                await storage.save(connection, task, candidate, facts, count)
            await connection.execute("""
                UPDATE public.ingestion_tasks SET processing_result = $2, last_error = coalesce($3, last_error)
                WHERE id = $1
            """, task["id"], "REJECTED" if reason else "INSERTED" if facts is not None else None, reason)
