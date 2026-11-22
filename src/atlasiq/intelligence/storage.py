from uuid import UUID

import asyncpg

from atlasiq.config.settings import Settings
from atlasiq.tasks.queue import TaskQueue

STAGE = "intelligence"
VERSION = "1"


async def enqueue(
    connection: asyncpg.Connection, settings: Settings, raw_id: UUID, kind: str, record_id: UUID,
) -> None:
    await TaskQueue(connection, settings).enqueue_task(raw_id, STAGE, f"{VERSION}:{kind}:{record_id}")


async def schedule(connection: asyncpg.Connection, settings: Settings, limit: int) -> int:
    return await connection.fetchval("""
        WITH targets AS (
            SELECT raw_document_id, $1::text || ':' || kind || ':' || record_id::text AS version
            FROM public.intelligence_candidates AS candidate
            WHERE NOT EXISTS (
                SELECT 1 FROM public.ingestion_tasks AS task
                WHERE task.stage = $2 AND task.raw_document_id = candidate.raw_document_id
                  AND task.input_version = $1 || ':' || candidate.kind || ':' || candidate.record_id::text
            )
            ORDER BY kind, record_id
            LIMIT $3
        ), created AS (
            INSERT INTO public.ingestion_tasks (raw_document_id, stage, input_version, max_attempts)
            SELECT raw_document_id, $2, version, $4 FROM targets
            ON CONFLICT (raw_document_id, stage, input_version) DO NOTHING
            RETURNING id
        )
        SELECT count(*) FROM created
    """, VERSION, STAGE, limit, settings.task_max_attempts)


async def retry_failed(connection: asyncpg.Connection, limit: int) -> int:
    return await connection.fetchval("""
        WITH failed AS (
            SELECT id FROM public.ingestion_tasks WHERE stage = $1 AND status = 'FAILED'
            ORDER BY updated_at, id LIMIT $2 FOR UPDATE SKIP LOCKED
        ), recovered AS (
            UPDATE public.ingestion_tasks AS task
            SET status = 'RETRY', attempts = 0, next_retry_at = clock_timestamp(), updated_at = clock_timestamp()
            FROM failed WHERE task.id = failed.id RETURNING task.id
        )
        SELECT count(*) FROM recovered
    """, STAGE, limit)


async def candidate_for(connection: asyncpg.Connection, task: asyncpg.Record) -> dict | None:
    version, kind, key = task["input_version"].split(":")
    if version != VERSION:
        return None
    row = await connection.fetchrow("""
        SELECT candidate.*, raw.raw_content
        FROM public.intelligence_candidates AS candidate
        JOIN public.raw_documents AS raw ON raw.id = candidate.raw_document_id
        WHERE candidate.kind = $1 AND candidate.record_id = $2 AND candidate.raw_document_id = $3
    """, kind, UUID(key), task["raw_document_id"])
    return dict(row) if row else None


async def record_attempt(
    connection: asyncpg.Connection, task: asyncpg.Record, *, provider: str, model: str, chunk: int,
    attempt: int, status: int | None, code: str | None, elapsed_ms: int,
    input_tokens: int | None, output_tokens: int | None,
) -> None:
    await connection.execute("""
        INSERT INTO public.extraction_attempts (
            raw_document_id, task_id, provider, model_name, chunk_index, latency_ms, response_status,
            error_code, retry_number, outcome, input_tokens, output_tokens
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
    """, task["raw_document_id"], task["id"], provider, model, chunk, elapsed_ms,
        status, code, attempt, "ACCEPTED" if code is None else "FAILED", input_tokens, output_tokens)


async def save(
    connection: asyncpg.Connection, task: asyncpg.Record, candidate: dict, facts: list[dict], chunks: int,
) -> None:
    await connection.execute("""
        INSERT INTO public.intelligence_records (
            raw_document_id, task_id, record_type, canonical_record_id, extractor_version,
            source_url, intelligence, chunk_count
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (record_type, canonical_record_id, raw_document_id, extractor_version) DO NOTHING
    """, task["raw_document_id"], task["id"], candidate["kind"], candidate["record_id"],
        VERSION, candidate["source_url"],
        {"schema_version": "1.0", "title": candidate["title"], "facts": facts}, chunks)


def _display(url: str) -> str:
    # Evidence URLs may have provider-supplied tracking values. The dashboard needs the public path.
    return url.split("?", 1)[0].split("#", 1)[0]


async def inspect(connection: asyncpg.Connection, record_id: UUID) -> dict | None:
    record = await connection.fetchrow("""
        SELECT candidate.kind, candidate.record_id, candidate.title, candidate.source_id,
               candidate.raw_document_id, candidate.source_url,
               result.id AS intelligence_id, result.intelligence, result.chunk_count, result.created_at,
               task.id AS task_id, task.status, task.processing_result, task.last_error, task.updated_at
        FROM public.intelligence_candidates AS candidate
        LEFT JOIN public.intelligence_current AS result
            ON result.record_type = candidate.kind AND result.canonical_record_id = candidate.record_id
           AND result.raw_document_id = candidate.raw_document_id AND result.extractor_version = $2
        LEFT JOIN public.ingestion_tasks AS task ON task.raw_document_id = candidate.raw_document_id
          AND task.stage = $3 AND task.input_version = $2 || ':' || candidate.kind || ':' || candidate.record_id::text
        WHERE candidate.record_id = $1 OR result.id = $1
    """, record_id, VERSION, STAGE)
    historical = False
    if record is None:
        record = await connection.fetchrow("""
            SELECT r.record_type AS kind, r.canonical_record_id AS record_id, r.intelligence->>'title' AS title, raw.source_id,
                   r.raw_document_id, r.source_url, r.id AS intelligence_id, r.intelligence, r.chunk_count,
                   r.created_at, task.id AS task_id, task.status, task.processing_result, task.last_error, task.updated_at
            FROM public.intelligence_records AS r
            JOIN public.raw_documents AS raw ON raw.id = r.raw_document_id
            JOIN public.ingestion_tasks AS task ON task.id = r.task_id
            WHERE r.id = $1
        """, record_id)
        historical = record is not None
    if record is None:
        return None
    data = dict(record)
    data["source_url"] = _display(data["source_url"])
    data["historical"] = historical
    data["attempts"] = [dict(row) for row in await connection.fetch("""
        SELECT provider, model_name, chunk_index, outcome, error_code, response_status, latency_ms,
               input_tokens, output_tokens, created_at
        FROM public.extraction_attempts WHERE task_id = $1 ORDER BY created_at DESC, id DESC LIMIT 12
    """, data["task_id"])]
    data["usage"] = dict(await connection.fetchrow("""
        SELECT count(*) AS attempts, sum(input_tokens) AS input_tokens, sum(output_tokens) AS output_tokens
        FROM public.extraction_attempts WHERE task_id = $1
    """, data["task_id"]))
    return data
