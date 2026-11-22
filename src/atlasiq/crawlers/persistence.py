from uuid import UUID

import asyncpg

from atlasiq.crawlers.base import InvalidRecord, Parsed
from atlasiq.db.repositories import CanonicalRecords
from atlasiq.enrichment.github import Metric
from atlasiq.schemas.entities import Product, ResearchPaper, Startup


async def persist_record(
    connection: asyncpg.Connection, parsed: Parsed, document: asyncpg.Record, metrics: list[Metric],
) -> tuple[str, UUID]:
    records = CanonicalRecords(connection)
    record = parsed.record
    key = parsed.canonical_key
    metadata = parsed.metadata
    args = {"source_id": document["source_id"], "raw_document_id": document["id"]}

    # Serialize aliases and keys before deciding whether this transaction inserts or updates.
    locks = [key]
    if isinstance(record, ResearchPaper) and metadata["doi"]:
        locks.append(f"doi:{metadata['doi']}")
    for lock in sorted(locks):
        await connection.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 20))", lock)

    if isinstance(record, ResearchPaper):
        aliases = await connection.fetch("""
            SELECT canonical_key FROM public.research_papers
            WHERE canonical_key = $1 OR lower(arxiv_id) = $2 OR lower(doi) = $3
        """, key, metadata["arxiv_id"], metadata["doi"])
        if len(aliases) > 1:
            raise InvalidRecord("PAPER_IDENTITY_CONFLICT")
        existing = bool(aliases)
        if aliases:
            key = aliases[0]["canonical_key"]
        row = await records.upsert_research_paper(
            record, canonical_key=key, arxiv_id=metadata["arxiv_id"], doi=metadata["doi"],
            abstract=metadata["abstract"], collected_at=metadata["collected_at"], **args,
        )
        for metric in metrics:
            await records.record_paper_metric(
                paper_id=row["id"], repository_url=metric.repository.url,
                repository_identity=metric.repository.identity, association_evidence=metric.evidence,
                github_stars=metric.stars, observed_at=metric.observed_at,
            )
    elif isinstance(record, Startup):
        existing = await connection.fetchval("""
            SELECT EXISTS (SELECT 1 FROM public.startups WHERE canonical_key = $1)
        """, key)
        row = await records.upsert_startup(record, canonical_key=key, **args)
    elif isinstance(record, Product):
        previous = await connection.fetchrow("SELECT startup_id FROM public.products WHERE canonical_key = $1", key)
        existing = previous is not None
        startup_id = previous["startup_id"] if previous is not None else None
        if metadata.get("startup_key"):
            startup_id = await connection.fetchval(
                "SELECT id FROM public.startups WHERE canonical_key = $1", metadata["startup_key"],
            ) or startup_id
        row = await records.upsert_product(
            record, canonical_key=key, product_name=metadata["product_name"], startup_id=startup_id, **args,
        )
    else:
        raise TypeError("unsupported crawler record type")
    return ("UPDATED" if existing else "INSERTED"), row["id"]
