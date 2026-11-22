from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any
from uuid import UUID

import asyncpg

from atlasiq.db.models import CrawlStatus, SourceCategory
from atlasiq.schemas.entities import Job, News, Product, ResearchPaper, Startup, aware_utc
from atlasiq.utils.urls import canonicalize_url, validate_source_url


class CrawlRunStateError(Exception):
    """The run is missing or no longer allows the requested transition."""


class CrawlRuns:
    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    async def create(self, source_id: str) -> asyncpg.Record:
        return await self.connection.fetchrow("""
            INSERT INTO public.crawl_runs (source_id) VALUES ($1) RETURNING *
        """, source_id)

    async def get(self, run_id: UUID) -> asyncpg.Record | None:
        return await self.connection.fetchrow("SELECT * FROM public.crawl_runs WHERE id = $1", run_id)

    async def start(self, run_id: UUID) -> asyncpg.Record:
        row = await self.connection.fetchrow("""
            UPDATE public.crawl_runs SET status = 'RUNNING', started_at = clock_timestamp()
            WHERE id = $1 AND status = 'PENDING' RETURNING *
        """, run_id)
        if row is None:
            raise CrawlRunStateError("Only a pending run may start.")
        return row

    async def checkpoint(
        self, run_id: UUID, *, cursor: Any, discovered_count: int, accepted_count: int, rejected_count: int
    ) -> asyncpg.Record:
        row = await self.connection.fetchrow("""
            UPDATE public.crawl_runs
            SET cursor = $2, discovered_count = $3, accepted_count = $4, rejected_count = $5
            WHERE id = $1 AND status = 'RUNNING'
              AND discovered_count <= $3 AND accepted_count <= $4 AND rejected_count <= $5
            RETURNING *
        """, run_id, cursor, discovered_count, accepted_count, rejected_count)
        if row is None:
            raise CrawlRunStateError("Checkpoint requires a running crawl and nondecreasing counts.")
        return row

    async def finish(
        self, run_id: UUID, status: CrawlStatus, *, failure_summary: str | None = None
    ) -> asyncpg.Record:
        if status not in {CrawlStatus.COMPLETED, CrawlStatus.FAILED, CrawlStatus.PARTIAL}:
            raise ValueError("finish requires a terminal crawl status")
        row = await self.connection.fetchrow("""
            UPDATE public.crawl_runs SET status = $2, failure_summary = $3, finished_at = clock_timestamp()
            WHERE id = $1 AND status = 'RUNNING' RETURNING *
        """, run_id, status, failure_summary)
        if row is None:
            raise CrawlRunStateError("Only a running crawl may finish.")
        return row


class RawDocuments:
    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    async def save(
        self, *, source_id: str, crawl_run_id: UUID, url: str, fetched_at: datetime,
        upstream_id: str | None = None, raw_content: str | None = None,
        storage_reference: str | None = None, content_hash: str | None = None,
        original_timestamp_evidence: dict | None = None,
    ) -> asyncpg.Record:
        if (raw_content is None) == (storage_reference is None):
            raise ValueError("provide raw_content or storage_reference")
        if raw_content is not None:
            computed_hash = sha256(raw_content.encode("utf-8")).hexdigest()
            if content_hash is not None and content_hash != computed_hash:
                raise ValueError("content_hash does not match raw_content")
            content_hash = computed_hash
        if content_hash is None:
            raise ValueError("a storage reference requires a SHA-256 content_hash")
        canonical_url = canonicalize_url(url)
        async with self.connection.transaction():
            row = await self.connection.fetchrow("""
                INSERT INTO public.raw_documents (
                    source_id, crawl_run_id, upstream_id, source_url, canonical_url,
                    raw_content, storage_reference, content_hash, fetched_at, original_timestamp_evidence
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (source_id, discovery_key, content_hash) DO NOTHING
                RETURNING *
            """, source_id, crawl_run_id, upstream_id, url, canonical_url, raw_content, storage_reference,
                content_hash, aware_utc(fetched_at), original_timestamp_evidence)
            if row is not None:
                return row
            identity = f"url:{canonical_url}" if upstream_id is None else f"id:{upstream_id}"
            return await self.connection.fetchrow("""
                SELECT * FROM public.raw_documents
                WHERE source_id = $1 AND discovery_key = $2 AND content_hash = $3
            """, source_id, identity, content_hash)

    async def get(self, document_id: UUID) -> asyncpg.Record | None:
        return await self.connection.fetchrow("SELECT * FROM public.raw_documents WHERE id = $1", document_id)

    async def set_status(self, document_id: UUID, status: str) -> asyncpg.Record | None:
        return await self.connection.fetchrow("""
            UPDATE public.raw_documents SET status = $2 WHERE id = $1 RETURNING *
        """, document_id, status)


class CanonicalRecords:
    """Persist validated records with stable, caller-resolved canonical keys."""

    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    async def upsert_startup(
        self, record: Startup, *, canonical_key: str, source_id: str, raw_document_id: UUID
    ) -> asyncpg.Record:
        return await self.connection.fetchrow("""
            INSERT INTO public.startups (
                canonical_key, entity_name, employee_count, source_id, source_url,
                raw_document_id, schema_version, collected_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (canonical_key) DO UPDATE SET
                entity_name = EXCLUDED.entity_name, employee_count = EXCLUDED.employee_count,
                source_id = EXCLUDED.source_id, source_url = EXCLUDED.source_url,
                raw_document_id = EXCLUDED.raw_document_id, schema_version = EXCLUDED.schema_version,
                collected_at = EXCLUDED.collected_at, updated_at = clock_timestamp()
            RETURNING *
        """, canonical_key, record.content.entityName, record.content.data.employeeCount, source_id,
            record.source.url, raw_document_id, record.schemaVersion, record.collectedAt)

    async def upsert_product(
        self, record: Product, *, canonical_key: str, product_name: str, source_id: str,
        raw_document_id: UUID, startup_id: UUID | None = None,
    ) -> asyncpg.Record:
        return await self.connection.fetchrow("""
            INSERT INTO public.products (
                canonical_key, product_name, startup_id, startup_name, pricing_model,
                source_id, source_url, raw_document_id, schema_version, collected_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (canonical_key) DO UPDATE SET
                product_name = EXCLUDED.product_name, startup_id = EXCLUDED.startup_id,
                startup_name = EXCLUDED.startup_name, pricing_model = EXCLUDED.pricing_model,
                source_id = EXCLUDED.source_id, source_url = EXCLUDED.source_url,
                raw_document_id = EXCLUDED.raw_document_id, schema_version = EXCLUDED.schema_version,
                collected_at = EXCLUDED.collected_at
            RETURNING *
        """, canonical_key, product_name, startup_id, record.content.startupName, record.content.pricingModel,
            source_id, record.source.url, raw_document_id, record.schemaVersion, record.collectedAt)

    async def upsert_research_paper(
        self, record: ResearchPaper, *, canonical_key: str, source_id: str,
        raw_document_id: UUID, arxiv_id: str | None = None, doi: str | None = None,
        abstract: str | None = None, collected_at: datetime | None = None,
    ) -> asyncpg.Record:
        content = record.content
        return await self.connection.fetchrow("""
            INSERT INTO public.research_papers AS existing (
                canonical_key, arxiv_id, doi, title, authors, paper_url, published_date,
                source_id, source_url, raw_document_id, schema_version, abstract, collected_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (canonical_key) DO UPDATE SET
                arxiv_id = coalesce(existing.arxiv_id, EXCLUDED.arxiv_id),
                doi = coalesce(existing.doi, EXCLUDED.doi), title = EXCLUDED.title,
                authors = EXCLUDED.authors, paper_url = EXCLUDED.paper_url, published_date = EXCLUDED.published_date,
                source_id = EXCLUDED.source_id, source_url = EXCLUDED.source_url,
                raw_document_id = EXCLUDED.raw_document_id, schema_version = EXCLUDED.schema_version,
                abstract = coalesce(EXCLUDED.abstract, existing.abstract),
                collected_at = coalesce(EXCLUDED.collected_at, existing.collected_at)
            RETURNING *
        """, canonical_key, arxiv_id, doi, content.title, content.authors, content.paper_url,
            content.published_date, source_id, record.source.url, raw_document_id, record.schemaVersion,
            abstract, aware_utc(collected_at) if collected_at is not None else None)

    async def record_paper_metric(
        self, *, paper_id: UUID, repository_url: str, repository_identity: str,
        association_evidence: dict, github_stars: int | None, observed_at: datetime,
    ) -> asyncpg.Record:
        return await self.connection.fetchrow("""
            INSERT INTO public.paper_github_metrics AS existing (
                paper_id, github_repository_url, repository_identity, association_evidence, github_stars, observed_at
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (paper_id, repository_identity, observed_at) DO UPDATE SET
                github_repository_url = EXCLUDED.github_repository_url,
                association_evidence = EXCLUDED.association_evidence,
                github_stars = coalesce(EXCLUDED.github_stars, existing.github_stars)
            RETURNING *
        """, paper_id, validate_source_url(repository_url), repository_identity, association_evidence,
            github_stars, aware_utc(observed_at))

    async def upsert_job(
        self, record: Job, *, canonical_key: str, source_id: str, raw_document_id: UUID,
        job_title: str, full_text: str, collected_at: datetime, publication_evidence: dict,
        upstream_id: str | None = None, location: str | None = None,
    ) -> asyncpg.Record:
        content = record.content
        return await self.connection.fetchrow("""
            INSERT INTO public.jobs (
                canonical_key, source_url, canonical_url, upstream_id, source_id, raw_document_id, company,
                job_title, role_family, location, is_remote, published_at, publication_evidence,
                full_text, schema_version, collected_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (canonical_key) DO UPDATE SET
                source_url = EXCLUDED.source_url, canonical_url = EXCLUDED.canonical_url,
                upstream_id = EXCLUDED.upstream_id, source_id = EXCLUDED.source_id,
                raw_document_id = EXCLUDED.raw_document_id, company = EXCLUDED.company,
                job_title = EXCLUDED.job_title, role_family = EXCLUDED.role_family, location = EXCLUDED.location,
                is_remote = EXCLUDED.is_remote, published_at = EXCLUDED.published_at,
                publication_evidence = EXCLUDED.publication_evidence, full_text = EXCLUDED.full_text,
                schema_version = EXCLUDED.schema_version, collected_at = EXCLUDED.collected_at
            RETURNING *
        """, canonical_key, record.source.url, canonicalize_url(record.source.url), upstream_id, source_id,
            raw_document_id, content.company, job_title, content.role_family, location, content.is_remote,
            content.date, publication_evidence, full_text, record.schemaVersion, aware_utc(collected_at))

    async def upsert_news(
        self, record: News, *, canonical_key: str, source_id: str, raw_document_id: UUID,
        publication_evidence: dict, upstream_id: str | None = None,
    ) -> asyncpg.Record:
        content = record.content
        return await self.connection.fetchrow("""
            INSERT INTO public.news (
                canonical_key, source_url, canonical_url, upstream_id, source_id, raw_document_id, title, full_text,
                published_at, publication_evidence, schema_version, collected_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (canonical_key) DO UPDATE SET
                source_url = EXCLUDED.source_url, canonical_url = EXCLUDED.canonical_url,
                upstream_id = EXCLUDED.upstream_id, source_id = EXCLUDED.source_id,
                raw_document_id = EXCLUDED.raw_document_id, title = EXCLUDED.title, full_text = EXCLUDED.full_text,
                published_at = EXCLUDED.published_at, publication_evidence = EXCLUDED.publication_evidence,
                schema_version = EXCLUDED.schema_version, collected_at = EXCLUDED.collected_at
            RETURNING *
        """, canonical_key, record.source.url, canonicalize_url(record.source.url), upstream_id, source_id,
            raw_document_id, content.title, content.fullText, content.publishedAt, publication_evidence,
            record.schemaVersion, record.collectedAt)


class IngestionEvidence:
    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    async def record_extraction_attempt(
        self, *, raw_document_id: UUID, provider: str, model_name: str, chunk_index: int, outcome: str,
        latency_ms: int | None = None, response_status: int | None = None,
        error_code: str | None = None, retry_number: int = 0,
    ) -> asyncpg.Record:
        return await self.connection.fetchrow("""
            INSERT INTO public.extraction_attempts (
                raw_document_id, provider, model_name, chunk_index, latency_ms,
                response_status, error_code, retry_number, outcome
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *
        """, raw_document_id, provider, model_name, chunk_index, latency_ms,
            response_status, error_code, retry_number, outcome)

    async def record_mapping(
        self, *, raw_name: str, source_url: str, match_method: str, review_status: str,
        canonical_name: str | None = None, canonical_key: str | None = None,
        confidence: Decimal | None = None, resolved_at: datetime | None = None,
    ) -> asyncpg.Record:
        return await self.connection.fetchrow("""
            INSERT INTO public.entity_mappings (
                raw_name, canonical_name, canonical_key, source_url, match_method, confidence, review_status, resolved_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *
        """, raw_name, canonical_name, canonical_key, validate_source_url(source_url), match_method,
            confidence, review_status, aware_utc(resolved_at) if resolved_at is not None else None)

    async def quarantine(
        self, *, raw_document_id: UUID, record_type: SourceCategory, reason_code: str,
        payload: Any, reason_detail: str | None = None,
    ) -> asyncpg.Record:
        return await self.connection.fetchrow("""
            INSERT INTO public.quarantine_records (
                raw_document_id, record_type, reason_code, reason_detail, payload
            ) VALUES ($1, $2, $3, $4, coalesce($5::jsonb, 'null'::jsonb)) RETURNING *
        """, raw_document_id, record_type, reason_code, reason_detail, payload)
