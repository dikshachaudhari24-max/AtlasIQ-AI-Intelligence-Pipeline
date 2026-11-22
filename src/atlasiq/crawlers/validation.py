import asyncpg
from pydantic import ValidationError

from atlasiq.schemas.entities import Job, News, Product, ResearchPaper, Startup

SOURCE = "jsonb_build_object('name', source.name, 'url', record.source_url) AS provenance"
CHECKS = (
    ("startups", Startup, """
        jsonb_build_object('entityName', record.entity_name,
            'data', jsonb_build_object('employeeCount', record.employee_count))
        AS content, record.collected_at
    """),
    ("products", Product, """
        jsonb_build_object('startupName', record.startup_name, 'pricingModel', record.pricing_model)
        AS content, record.collected_at
    """),
    ("research_papers", ResearchPaper, """
        jsonb_build_object('title', record.title, 'authors', record.authors, 'paper_url', record.paper_url,
            'github_url', metric.github_repository_url, 'github_stars', metric.github_stars,
            'published_date', record.published_date)
        AS content, NULL AS collected_at
    """),
    ("jobs", Job, """
        jsonb_build_object('company', record.company, 'role_family', record.role_family,
            'date', record.published_at, 'is_remote', record.is_remote)
        AS content, NULL AS collected_at
    """),
    ("news", News, """
        jsonb_build_object('title', record.title, 'fullText', record.full_text, 'publishedAt', record.published_at)
        AS content, record.collected_at
    """),
)
METRIC = """
LEFT JOIN LATERAL (
    SELECT github_repository_url, github_stars FROM public.paper_github_metrics
    WHERE paper_id = record.id
    ORDER BY (github_stars IS NOT NULL) DESC, observed_at DESC, id DESC LIMIT 1
) AS metric ON true
"""
RECORD_TYPES = {
    "startups": "STARTUP", "products": "PRODUCT", "research_papers": "RESEARCH_PAPER", "jobs": "JOB", "news": "NEWS",
}


async def validate_stored_records(connection: asyncpg.Connection) -> dict:
    tables = {}
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        for table, model, fields in CHECKS:
            join = METRIC if table == "research_papers" else ""
            query = f"""
                SELECT record.schema_version, {SOURCE}, {fields}
                FROM public.{table} AS record
                JOIN public.sources AS source ON source.id = record.source_id
                {join}
            """
            valid = invalid = 0
            async for row in connection.cursor(query, prefetch=100):
                candidate = {
                    "schemaVersion": row["schema_version"], "recordType": RECORD_TYPES[table],
                    "source": row["provenance"], "content": row["content"],
                }
                if table in {"startups", "products", "news"}:
                    candidate["collectedAt"] = row["collected_at"]
                try:
                    model.model_validate(candidate)
                except ValidationError:
                    invalid += 1
                else:
                    valid += 1
            tables[table] = {"checked": valid + invalid, "valid": valid, "invalid": invalid}
        quarantined = await connection.fetchval("SELECT count(*) FROM public.quarantine_records")
    return {
        "records": tables, "quarantined": quarantined,
        "checked": sum(table["checked"] for table in tables.values()),
        "invalid": sum(table["invalid"] for table in tables.values()),
    }
