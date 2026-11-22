import asyncpg

STAGES = (
    ("source", "Sources"), ("discover", "Discovery"), ("fetch", "Fetch & raw"),
    ("validate", "Validation"), ("canonical", "Canonical storage"), ("extract", "Extraction"),
    ("normalize", "Normalization"), ("intelligence", "Intelligence"), ("export", "Export"),
)
TASK_STATES = ("PENDING", "CLAIMED", "RETRY", "COMPLETED", "FAILED")
INTELLIGENCE_STAGE = "intelligence"


async def _metrics(connection: asyncpg.Connection) -> dict:
    return dict(await connection.fetchrow("""
        SELECT (SELECT coalesce(sum(discovered_count), 0)::bigint FROM public.crawl_runs) AS discovered,
               (SELECT coalesce(sum(accepted_count), 0)::bigint FROM public.crawl_runs) AS validated,
               (SELECT count(*) FROM public.raw_documents) AS fetched,
               (SELECT count(*) FROM public.startups) + (SELECT count(*) FROM public.products)
                 + (SELECT count(*) FROM public.research_papers) + (SELECT count(*) FROM public.jobs)
                 + (SELECT count(*) FROM public.news) AS persisted,
               (SELECT count(*) FROM public.intelligence_current) AS enriched,
               (SELECT count(*) FROM public.quarantine_records) AS rejected
    """))


async def read_dashboard(pool: asyncpg.Pool, env: str, cached: dict | None = None) -> dict:
    async with pool.acquire() as connection, connection.transaction(isolation="repeatable_read", readonly=True):
        metrics = dict(cached) if cached is not None else await _metrics(connection)
        states = dict.fromkeys(TASK_STATES, 0)
        extraction = dict.fromkeys(TASK_STATES, 0)
        rejected = verified = 0
        for row in await connection.fetch("""
            SELECT stage, status, processing_result, count(*) AS total FROM public.ingestion_tasks
            GROUP BY stage, status, processing_result
        """):
            states[row["status"]] += row["total"]
            if row["stage"] == INTELLIGENCE_STAGE:
                extraction[row["status"]] += row["total"]
                if row["processing_result"] == "REJECTED":
                    rejected += row["total"]
                if row["processing_result"] == "INSERTED":
                    verified += row["total"]

        sources = [dict(row) for row in await connection.fetch("""
            SELECT s.id, s.name, s.category, s.adapter_name, s.enabled,
                   run.status AS last_status, run.started_at AS last_run, run.failure_summary,
                   coalesce(run.discovered_count, 0) AS discovered,
                   coalesce(run.accepted_count, 0) AS validated,
                   coalesce(outcomes.persisted, 0) AS persisted,
                   coalesce(run.rejected_count, 0) + coalesce(outcomes.failed, 0)
                      + (run.failure_summary IS NOT NULL)::integer AS errors,
                   last_ok.finished_at AS last_success
            FROM public.sources AS s
            LEFT JOIN LATERAL (
                SELECT * FROM public.crawl_runs WHERE source_id = s.id
                ORDER BY started_at DESC NULLS LAST, id DESC LIMIT 1
            ) AS run ON true
            LEFT JOIN LATERAL (
                SELECT count(*) FILTER (WHERE NOT item.reused AND t.processing_result IN ('INSERTED','UPDATED')) AS persisted,
                       count(*) FILTER (WHERE t.status = 'FAILED') AS failed
                FROM public.crawl_run_items AS item JOIN public.ingestion_tasks AS t ON t.id = item.task_id
                WHERE item.crawl_run_id = run.id
            ) AS outcomes ON true
            LEFT JOIN LATERAL (
                SELECT finished_at FROM public.crawl_runs
                WHERE source_id = s.id AND accepted_count > 0 AND status IN ('COMPLETED','PARTIAL')
                ORDER BY finished_at DESC LIMIT 1
            ) AS last_ok ON true
            ORDER BY s.id
        """)]
        tasks = [dict(row) for row in await connection.fetch("""
            SELECT id, stage, status, attempts, max_attempts, next_retry_at, lease_expires_at,
                   lease_owner, last_error, processing_result, updated_at,
                   lease_expires_at <= statement_timestamp() AS lease_expired
            FROM public.ingestion_tasks ORDER BY updated_at DESC, id DESC LIMIT 12
        """)]
        attempts = [dict(row) for row in await connection.fetch("""
            SELECT provider, model_name, outcome, error_code, latency_ms, input_tokens, output_tokens, created_at
            FROM public.extraction_attempts ORDER BY created_at DESC, id DESC LIMIT 6
        """)]
    metrics.update(queued=states["PENDING"] + states["RETRY"], failed=states["FAILED"])
    return {
        "environment": env, "database": "CONNECTED", "sources": sources, "metrics": metrics,
        "queue": states, "tasks": tasks, "attempts": attempts,
        "extraction": {**extraction, "rejected": rejected, "enriched": verified},
        "stages": stage_states(sources, metrics, extraction, rejected, verified),
    }


def stage_states(sources: list[dict], metrics: dict, extraction: dict, rejected: int, verified: int) -> dict:
    enabled = sum(source["enabled"] for source in sources)
    active = [source for source in sources if source["last_status"] == "RUNNING"]
    failed = any(source["last_status"] in {"FAILED", "PARTIAL"} for source in sources)
    discovery_retry = bool(active) and all(source["failure_summary"] for source in active)
    ingest_state = "RETRYING" if discovery_retry else "RUNNING" if active else "FAILED" if failed else "PENDING"
    if not active and not failed and any(source["last_status"] == "COMPLETED" for source in sources):
        ingest_state = "COMPLETED"
    ai_state = (
        "RUNNING" if extraction["CLAIMED"] else "RETRYING" if extraction["RETRY"]
        else "PENDING" if extraction["PENDING"] else "FAILED" if extraction["FAILED"] or rejected
        else "COMPLETED" if verified else "SKIPPED" if extraction["COMPLETED"] else "PENDING"
    )
    return {
        "source": {"state": "COMPLETED" if enabled else "BLOCKED" if sources else "PENDING",
                   "detail": f"{enabled} enabled / {len(sources)} registered"},
        "discover": {"state": ingest_state, "detail": f"{metrics['discovered']:,} discovered"},
        "fetch": {"state": ingest_state if active else "COMPLETED" if metrics["fetched"] else ingest_state,
                  "detail": f"{metrics['fetched']:,} raw revisions"},
        "validate": {"state": "RUNNING" if active else "COMPLETED" if metrics["validated"] else ingest_state,
                     "detail": f"{metrics['validated']:,} accepted by crawl runs"},
        "canonical": {"state": "COMPLETED" if metrics["persisted"] else "PENDING",
                      "detail": f"{metrics['persisted']:,} canonical records"},
        "extract": {"state": ai_state, "detail": f"{verified:,} accepted / {rejected:,} rejected"},
        "normalize": {"state": "COMPLETED" if metrics["enriched"] else "PENDING",
                      "detail": f"{metrics['enriched']:,} current normalized results"},
        "intelligence": {"state": "COMPLETED" if metrics["enriched"] else "PENDING",
                         "detail": f"{metrics['enriched']:,} current results committed"},
        "export": {"state": "BLOCKED", "detail": "Sheets export is pending"},
    }
