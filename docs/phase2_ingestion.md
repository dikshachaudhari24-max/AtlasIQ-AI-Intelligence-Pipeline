# Phase 2 ingestion

Phase 2 collects research papers, AI startups, and AI products from public APIs. PostgreSQL stores verified records, the original source evidence, and restartable work state.

## Setup and commands

Reuse the Phase 1 virtual environment and PostgreSQL setup. Point `DATABASE_URL` at the intended AtlasIQ database. Run the commands from the project root.

```powershell
$AtlasPython = '.\.venv\Scripts\python.exe'
& $AtlasPython -m pip install -e '.[test]'
& $AtlasPython main.py db migrate
& $AtlasPython main.py health
& $AtlasPython main.py crawl --source papers --limit 5
& $AtlasPython main.py crawl --source startups --limit 5
& $AtlasPython main.py crawl --source products --limit 5
& $AtlasPython main.py sources list
& $AtlasPython main.py tasks status
& $AtlasPython main.py validate
& $AtlasPython main.py resume
```

A built-in source ID registers its defaults on first use. Existing registrations retain their configuration and enabled state. If you registered the disabled `papers` example in Phase 1, run `main.py sources enable papers` before the first crawl. For reference, register a custom source with `main.py sources add --config FILE` and a unique ID and name. Choose the matching category and adapter.

| Default ID | Registry category and adapter | Evidence and options |
| --- | --- | --- |
| `papers` | `RESEARCH_PAPER`, `arxiv` | Official Atom query API. Options are `query` (default `cat:cs.AI`), `id_list` for targeted paper IDs, `page_size` (1–100), and optional `sort_by` (`relevance`, `submittedDate`, or `lastUpdatedDate`). |
| `startups` | `STARTUP`, `wikidata-startups` | Wikidata Query Service requires explicit startup-company and artificial-intelligence industry claims. `page_size` accepts 1–100. A missing or ambiguous employee count stays null. |
| `products` | `PRODUCT`, `atlassian-marketplace` | Atlassian Marketplace cloud app API supplies app and vendor evidence. Options are `query` (default `AI`), `page_size` (1–50), and optional `vendor_startup_keys` for operator-verified Marketplace vendor ID to existing canonical startup key mappings. |

The sources above use public access. `GITHUB_TOKEN` is optional for GitHub REST quota. Provide a descriptive `CRAWLER_USER_AGENT` with a contact or project URL for ongoing public-source use. `CRAWLER_MAX_CONCURRENCY` caps concurrent HTTP requests and `CRAWLER_WORKER_CONCURRENCY` controls how many eligible tasks are processed at once.

For example, save this object to a JSON file and register it to crawl a different arXiv query.

```json
{
  "id": "machine-learning-papers",
  "name": "arXiv Machine Learning",
  "category": "RESEARCH_PAPER",
  "base_url": "https://arxiv.org/",
  "adapter_name": "arxiv",
  "adapter_config": {"query": "cat:cs.LG", "page_size": 25, "sort_by": "submittedDate"},
  "rate_limit_config": {"requests_per_minute": 20, "max_concurrency": 1},
  "enabled": true
}
```

## Data path and recovery

1. An adapter requests a bounded source API page through the shared `aiohttp` client. Each item retains an Atom entry or structured JSON, its source URL, its upstream ID when available, the request URL, and the UTC fetch time.
2. The service commits raw documents, their versioned tasks, run membership, and the next cursor in one transaction. The same source identity and content hash reuse a raw revision and its task. Changed source content creates a new revision. A distinct item counts toward `--limit` even if validation later rejects it. Set `--limit 100` or `--limit 1000` to process a larger sample.
3. Workers claim bounded batches, renew their leases, and extract typed records. Marketplace workers request the cloud version for payment evidence when the listing does not contain it. A canonical upsert and completion commit together. Invalid records retain their evidence and a quarantine reason. Transient task errors retain a retry schedule, and permanent fetch errors retain a failure code.
4. Run output reports persisted discovery, validation, insertion, update, duplicate, rejection, and task counts. An unchanged previously verified revision is a duplicate and contributes to the validated count without a new write. `RUNNING` reports pending retry or lease state, with `next_eligible_at` when known. `PARTIAL` reports a mix of accepted and rejected or failed items. `FAILED` reports a terminal run with errors and no accepted items. Source exhaustion can produce a completed run below the requested limit.
5. Run `main.py resume` after interruption or when the next retry becomes eligible. It recovers expired leases, processes eligible items, and resumes discovery from the last committed cursor. Run results already committed are reused. Task attempts are bounded. A PostgreSQL advisory lock serializes runs for each source ID, and the queue enforces claim ownership independently.

HTTP admission limits are enforced per host within each client, with built-in maximums per adapter. arXiv uses one request at a time with at least three seconds between request starts. HTTPS requests are restricted to each adapter's approved hosts and public DNS addresses. The client validates redirect targets, response types, and byte limits. Timeouts and transient statuses use bounded exponential backoff with jitter and respect `Retry-After`. A wait beyond the configured in-process threshold is handed to the crawl checkpoint or queue as a retry time. GitHub rate-limit headers are handled as part of its API policy.

## Field evidence and inspection

- arXiv IDs are normalized without version suffixes. DOI is an additional alias where supplied. Publication time comes from the paper metadata, and collection time is stored separately. The raw entry retains the original version and abstract.
- GitHub associations require an explicit repository-root URL in arXiv metadata or abstract text. They are evidence of a paper reference. The GitHub REST response provides the canonical owner, repository name, star count, and time of observation. A zero star count is stored as zero. An unavailable observation stays null. A missing or private repository does not yield a verified observation. The original link and its evidence category are retained for accepted associations.
- Startup keys and product keys are scoped to stable source identifiers. Marketplace payment labels `free`, `atlassian`, and `vendor` map to `FREE`, `PAID`, and `PAID` using Atlassian's documented payment models. Missing or unsupported pricing gets a quarantine record. A product links to a startup only if a verified vendor mapping resolves to an existing canonical key.
- `validate` runs a read-only, batched audit of stored canonical records. It returns actual valid, invalid, and quarantine totals. Raw documents and task records are available in PostgreSQL for diagnosis.

```sql
SELECT arxiv_id, title, source_url, published_date, collected_at FROM research_papers;
SELECT p.arxiv_id, m.github_repository_url, m.github_stars, m.observed_at
FROM paper_github_metrics m JOIN research_papers p ON p.id = m.paper_id;
SELECT entity_name, employee_count, source_url FROM startups;
SELECT product_name, startup_name, pricing_model, source_url FROM products;
SELECT record_type, reason_code, created_at FROM quarantine_records;
```

Migration `0002_crawling.sql` adds crawl request and checkpoint fields, a run-to-task association table, processing outcomes and evidence, and paper abstract and collection time. The existing Phase 1 rows and their migration history are preserved.

## Tests and scaling

```powershell
& $AtlasPython -m pytest -m 'not integration'
# Set TEST_DATABASE_URL to a development PostgreSQL server using a role with CREATEDB.
& $AtlasPython -m pytest -m 'integration and not live_source'
$env:ATLASIQ_LIVE_TESTS = '1'
& $AtlasPython -m pytest -m live_source
```

The live test fetches an arXiv paper with an explicit repository reference and verifies the GitHub star count and completed task in a disposable database. The normal suites use deterministic fixtures.

Memory use is bounded by source page size, response bytes, and active worker batches. Large runs can be distributed across independent sources, with deployment-wide rate coordination needed if multiple processes share the same provider. Public directories can change between pages and may exhaust before reaching a target. Wikidata coverage depends on explicit claims. The Marketplace adapter covers cloud listings with source-supported AI and pricing evidence. An unchanged arXiv revision reuses the existing repository observation; repeated star sampling is a separate future operation. Bulk acquisition, cross-source entity resolution, and the final 1,000-record thresholds require further source coverage and verification.

References for [arXiv API use](https://info.arxiv.org/help/api/user-manual.html), [arXiv API terms](https://info.arxiv.org/help/api/tou.html), [Wikidata data access](https://www.wikidata.org/wiki/Help:Linked_Data_Interface), [Marketplace apps API](https://developer.atlassian.com/platform/marketplace/rest/v2/api-group-apps/), [Marketplace payment models](https://developer.atlassian.com/platform/marketplace/pricing-payment-and-billing/), and [GitHub REST rate limits](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api).
