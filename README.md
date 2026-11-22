# AtlasIQ

AtlasIQ is a backend-first AI intelligence ingestion platform. Phase 2 collects research papers from arXiv, AI startups from Wikidata, and AI products from Atlassian Marketplace. It verifies explicit GitHub repository references and stores observations, source evidence, and restartable work in PostgreSQL.

```text
Typer CLI
  ├─ source registry and bounded aiohttp requests
  └─ PostgreSQL
       ├─ crawl checkpoints, raw evidence, and durable task leases
       └─ validated records, GitHub observations, and quarantine
```

The project uses Python 3.11+, asyncio, aiohttp, PostgreSQL 17, asyncpg, Pydantic v2, Typer, and pytest. Dependencies are defined in `pyproject.toml`. Docker Compose provides an optional PostgreSQL service.

## Windows PowerShell setup

Run these commands in the project root. Use `python` in place of `py -3` if your Python installation has no Windows launcher.

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e '.[test]'
Copy-Item .env.example .env
```

Set `DATABASE_URL` in the local `.env`. Use the form `postgresql://role:password@host:port/database` and percent-encode special characters in credentials. AtlasIQ also accepts the `postgresql+asyncpg://` prefix. Shell environment variables override `.env`. Provider, GitHub, and Google credentials are optional in Phase 1.

### Use an existing PostgreSQL server

Connect with a database administrator role using `psql -U postgres -d postgres` or use pgAdmin. Create a new role and database if you do not already have a dedicated AtlasIQ database. In `psql`:

```sql
CREATE ROLE atlasiq_user LOGIN;
\password atlasiq_user
CREATE DATABASE atlasiq OWNER atlasiq_user;
```

Point `DATABASE_URL` at the selected database and use a role that can create and update objects in its `public` schema.

### Start PostgreSQL with Docker Compose

Set `POSTGRES_PASSWORD` in `.env` and use the same password in `DATABASE_URL`. Set `POSTGRES_PORT` and adjust the URL if port 5432 is already occupied. Docker Compose binds to loopback and persists the database in a named volume.

```powershell
docker compose up -d postgres
docker compose ps
```

### Initialize and inspect

```powershell
$AtlasPython = '.\.venv\Scripts\python.exe'
& $AtlasPython main.py --help
& $AtlasPython main.py db check
& $AtlasPython main.py db migrate
& $AtlasPython main.py health
& $AtlasPython main.py sources list
& $AtlasPython main.py tasks status
```

On Linux, use `python3 -m venv .venv`, `cp .env.example .env`, and invoke `.venv/bin/python`.

## CLI

| Command | Behavior |
| --- | --- |
| `health` | Validates settings, checks PostgreSQL, and reports migration state. Pending migrations produce exit code 1. |
| `db check` | Confirms an authenticated connection and reports the server version. |
| `db migrate` | Applies pending SQL versions atomically under an advisory lock. |
| `sources list [--category RESEARCH_PAPER] [--enabled/--disabled]` | Lists registered sources with optional filters. |
| `sources add --config FILE` | Registers a JSON source object or array in one database transaction. |
| `sources enable SOURCE_ID` and `sources disable SOURCE_ID` | Sets whether a registered source may run. |
| `tasks status [TASK_UUID]` | Shows counts for every status or the current state of an individual task. |
| `crawl --source papers --limit 5` | Fetches and persists up to five distinct source items. Other built-in IDs are `startups` and `products`. Set `--limit 100` or `--limit 1000` to crawl more. |
| `resume` | Recovers expired leases and resumes eligible work and discovery from the stored cursor. |
| `validate` | Runs a read-only audit of stored canonical records. |
| `run` and `export --target sheets` | Reserved for later ingestion phases. |

The public source APIs require no credentials. Set the optional `GITHUB_TOKEN` to increase GitHub quota. The HTTP client uses per-host limits, bounded retries, and `Retry-After` when supplied. If a Phase 1 sample already registered `papers` as disabled, run `sources enable papers`. The [Phase 2 ingestion guide](docs/phase2_ingestion.md) covers source settings, pricing evidence, resume behavior, and SQL examples for inspecting verified records. The [Phase 1 setup guide](docs/phase1_setup.md) covers PostgreSQL and queue troubleshooting.

## Tests and acceptance

```powershell
& $AtlasPython -m pytest -m 'not integration'
# Supply a URL for a development-only PostgreSQL cluster and a role with CREATEDB.
# Set TEST_DATABASE_URL in this shell without committing it or saving it in command history.
& $AtlasPython -m pytest -m 'integration and not live_source'
```

The integration suite creates a unique database for each test and drops only databases it created. It skips when `TEST_DATABASE_URL` is absent. Use a development-only PostgreSQL cluster. Set `ATLASIQ_LIVE_TESTS=1` and run `pytest -m live_source` to explicitly verify arXiv and GitHub through PostgreSQL.

- Confirm the CLI imports and reports actionable configuration errors with JSON logs on stderr.
- Confirm PostgreSQL is reachable and the explicit migrations are current after a repeat run.
- Verify source configuration, persistent crawl checkpoints, raw document revisions, canonical upserts, and rollback behavior.
- Verify exclusive claims, lease renewal and recovery, attempt limits, and idempotent completion against PostgreSQL.
- Verify canonical JSON contracts, missing required facts, timestamp offsets, and the zero versus unavailable distinction for paper metrics.

Later phases add LLM extraction and provider fallback, cross-source entity resolution, news and jobs, and Sheets export. Source coverage for the assessment's three 1,000-record targets remains a separate milestone.
