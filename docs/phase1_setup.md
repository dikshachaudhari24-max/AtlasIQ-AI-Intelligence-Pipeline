# Local setup and troubleshooting

1. Install Python 3.11 or newer and choose an existing PostgreSQL 17 instance or the optional PostgreSQL Compose service. Run the commands in the repository root so the CLI can load its local `.env`.
2. Create a virtual environment with `py -3 -m venv .venv` and install with `.\.venv\Scripts\python.exe -m pip install -e '.[test]'`. On Linux use `python3` to create it and `.venv/bin/python` to invoke it.
3. Copy `.env.example` to `.env` and set `DATABASE_URL`. For Compose, also set `POSTGRES_PASSWORD`, then run `docker compose up -d postgres`. For an existing server, follow the role and database setup in the [README](../README.md).
4. Run `main.py db check`, `main.py db migrate`, and `main.py health` through the virtual environment. Migration SQL and its checksum are packaged with the application. Each version is applied once and recorded in `atlasiq_schema_migrations`. Add a new numbered migration when changing a deployed schema.
5. Run `main.py sources add --config docs/example_sources.json` to register the optional sample configuration. A registration file can contain one object or an array of objects. IDs are stable lowercase identifiers. Rate limits require a positive number of requests per minute and default to one concurrent request. `adapter_config` and `schedule_config` hold JSON configuration for future phases. Keep secrets in environment variables.
6. Run `main.py sources list --disabled` and `main.py tasks status` to inspect the database. An initialized empty queue reports zero counts.

## Repository and queue usage

- Obtain a pool with `database_pool(settings)`. Use `transaction(pool)` to share a connection across repositories when several writes form one operation. Repository statements use parameters and PostgreSQL constraints to preserve identity and provenance.
- Use `CrawlRuns.create`, `start`, `checkpoint`, and `finish` for source execution state. Keep a single logical writer per crawl run and checkpoint with absolute, nondecreasing counts. Save raw documents, enqueue their tasks, and advance the cursor together in one transaction.
- Raw revision identity uses the source ID, a stable upstream ID or normalized URL, and the SHA-256 of the content. Use an upstream ID consistently for a given item. An exact rediscovery returns the original revision. Changed content creates another revision. The first discovery retains its crawl run and original URL.
- Pass a stable stage name and input version to `TaskQueue.enqueue_task`. Repeating that combination for the same raw document returns the existing task and preserves its state.
- `claim_tasks` increments the attempt counter and returns a unique token and lease expiry per claim. Commit the claim before doing external work. Renew the lease by task UUID and claim token. Use a bounded sweep of `recover_expired_leases` from a future scheduler. An expired claim either becomes immediately available or moves to `FAILED` if its final attempt was consumed.
- Use a single transaction for canonical writes, related evidence, and `complete_task`. Let `LeaseLost` propagate out of that transaction to roll back stale work. Completing again with the successful claim token is idempotent. Treat completion as part of a replayable transaction with idempotent writes.
- `fail_task` persists a short error code and schedules a capped exponential retry or terminal failure. Use stable categories such as `UPSTREAM_TIMEOUT` for error codes.
- Canonical keys and verified arXiv/DOI aliases must come from a future resolver. The database rejects a second canonical key for an existing arXiv ID or DOI; callers should resolve or quarantine the conflict. Separate paper association evidence from the optional star observation using `record_paper_metric`. Canonical product pricing and job remote status are required by the PRD extraction contracts. Unknown job remote status can be represented as SQL NULL in storage where needed. Preserve raw evidence for unresolved records and use quarantine for incomplete canonical records.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| `ModuleNotFoundError: atlasiq` | Run the editable install through the same virtual environment used for the CLI. |
| Missing or invalid `DATABASE_URL` | Check the shell environment and root `.env`. Use a valid host and database name. Percent-encode credential characters such as `@` and `#`. |
| Connection refused or timed out | Check the Windows service, Compose health, hostname, port, and network settings. `pg_isready` tests whether the server answers. `db check` also tests credentials. |
| Authentication failed | Check the role, password, and PostgreSQL authentication rules. Changing Compose environment values does not rotate a password stored in an existing volume. |
| Database does not exist | Create the intended AtlasIQ database with the documented role as owner or correct the URL. |
| Pending migration | Run `db migrate` explicitly. Health checks do not change the schema. |
| Existing table, checksum mismatch, or permission error | Check that the URL points to the intended database, verify deployed migration files, and check schema privileges. |
| Integration tests skipped | Set `TEST_DATABASE_URL` in the test shell for a development cluster with a role granted `CREATEDB`. The URL should select a maintenance database such as `postgres`. The tests use only newly created databases for application writes. |
| Integration tests fail during setup | Check authentication and `CREATEDB` on that test role. A configured but unreachable server produces a test failure. |

JSON logs on stderr contain categorized failures, elapsed time, and sanitized stack locations in development mode. CLI results go to stdout.
