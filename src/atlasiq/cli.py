import asyncio
import json
import logging
from collections.abc import Callable, Coroutine
from pathlib import Path
from time import perf_counter
from typing import Annotated, Any, TypeVar
from uuid import UUID

import asyncpg
import typer
from pydantic import TypeAdapter, ValidationError

from atlasiq.config.settings import ConfigurationError, Settings, load_settings
from atlasiq.core.health import check_database, check_health
from atlasiq.core.logging import configure_logging
from atlasiq.crawlers.http_client import HttpClient
from atlasiq.crawlers.service import CrawlError, IngestionService
from atlasiq.crawlers.validation import validate_stored_records
from atlasiq.db.connection import database_pool, transaction
from atlasiq.db.migrate import MigrationError, apply_migrations
from atlasiq.db.models import SourceCategory
from atlasiq.intelligence.service import IntelligenceError
from atlasiq.sources.registry import SourceAlreadyRegistered, SourceConfig, SourceNotFound, SourceRegistry
from atlasiq.tasks.queue import TaskQueue
from atlasiq.ui.commands import Json, NoColor, Plain, register_operations
from atlasiq.ui.presentation import Presentation

app = typer.Typer(help="AtlasIQ ingestion platform.", no_args_is_help=True, pretty_exceptions_enable=False)
db = typer.Typer(help="Check PostgreSQL and manage schema migrations.", no_args_is_help=True)
sources = typer.Typer(help="Manage the source registry.", no_args_is_help=True)
tasks = typer.Typer(help="Inspect the ingestion task queue.", no_args_is_help=True)
app.add_typer(db, name="db")
app.add_typer(sources, name="sources")
app.add_typer(tasks, name="tasks")

logger = logging.getLogger(__name__)
T = TypeVar("T")


class TaskNotFound(Exception):
    pass


def _failure_message(error: Exception) -> str:
    if isinstance(error, (ConfigurationError, MigrationError, CrawlError, IntelligenceError)):
        return str(error)
    if isinstance(error, asyncpg.ClientConfigurationError):
        return "PostgreSQL rejected a client setting. Check the connection options in DATABASE_URL."
    if isinstance(error, (asyncpg.InvalidPasswordError, asyncpg.InvalidAuthorizationSpecificationError)):
        return "PostgreSQL authentication failed. Check DATABASE_URL and the role credentials."
    if isinstance(error, asyncpg.InvalidCatalogNameError):
        return "The configured PostgreSQL database does not exist. Create it or update DATABASE_URL."
    if isinstance(error, asyncpg.InsufficientPrivilegeError):
        return "The PostgreSQL role lacks the permissions for this operation."
    if isinstance(error, (asyncpg.UndefinedTableError, asyncpg.UndefinedColumnError)):
        return "The database schema is unavailable or incomplete. Check migration state with health."
    if isinstance(error, asyncpg.DuplicateTableError):
        return "Migration found an existing table. Check that DATABASE_URL points to the intended AtlasIQ database."
    if isinstance(error, (TimeoutError, OSError, asyncpg.PostgresConnectionError)):
        return "PostgreSQL is unreachable or the database operation timed out. Check DATABASE_URL and server availability."
    if isinstance(error, SourceAlreadyRegistered):
        return "A source with that ID or category and name already exists."
    if isinstance(error, SourceNotFound):
        return "No source with that ID is registered."
    if isinstance(error, TaskNotFound):
        return "No task with that ID exists."
    return "The operation failed. Review the structured log for its error type."


def _execute(
    operation: str, action: Callable[[Settings, asyncpg.Pool], Coroutine[Any, Any, T]],
    *, source_id: str | None = None, task_id: UUID | None = None,
    error_view: Callable[[str], None] | None = None,
) -> T:
    async def run(settings: Settings) -> T:
        async with database_pool(settings) as pool:
            return await action(settings, pool)

    start = perf_counter()
    configure_logging("INFO", diagnostics=False)
    try:
        settings = load_settings()
        configure_logging(settings.log_level, diagnostics=settings.app_env == "development")
        result = asyncio.run(run(settings))
    except Exception as error:
        message = _failure_message(error)
        logger.exception(
            "Operation failed",
            extra={"operation": operation, "safe_message": message, "task_id": task_id,
                   "duration_ms": (perf_counter() - start) * 1000},
        )
        if error_view is None:
            typer.echo(message, err=True)
        else:
            error_view(message)
        raise typer.Exit(1) from None
    logger.info(
        "Operation finished",
        extra={"operation": operation, "safe_message": "Database operation finished",
               "source_id": source_id, "task_id": task_id,
               "duration_ms": (perf_counter() - start) * 1000},
    )
    return result


def _json(value: object) -> None:
    typer.echo(json.dumps(value, indent=2))


@app.command()
def health() -> None:
    """Validate settings, PostgreSQL connectivity and migration state."""
    async def action(settings: Settings, pool: asyncpg.Pool):
        async with pool.acquire() as connection:
            return await check_health(connection)

    result = _execute("health", action)
    _json({
        "configuration": "valid",
        "database": "reachable",
        "postgres_version": result.postgres_version,
        "migrations": "current" if result.migrations.current else "pending",
        "applied": result.migrations.applied,
        "pending": result.migrations.pending,
    })
    if not result.migrations.current:
        typer.echo("Run python main.py db migrate to apply pending migrations.", err=True)
        raise typer.Exit(1)


@db.command("check")
def db_check() -> None:
    """Connect to PostgreSQL and report its version."""
    async def action(settings: Settings, pool: asyncpg.Pool):
        async with pool.acquire() as connection:
            return await check_database(connection)

    _json({"database": "reachable", "postgres_version": _execute("db.check", action)})


@db.command("migrate")
def db_migrate() -> None:
    """Apply pending AtlasIQ SQL migrations."""
    async def action(settings: Settings, pool: asyncpg.Pool):
        return await apply_migrations(pool)

    applied = _execute("db.migrate", action)
    _json({"migrations": "current", "newly_applied": applied})


def _source_summary(row: asyncpg.Record) -> dict:
    return {key: row[key] for key in ("id", "name", "category", "adapter_name", "enabled")}


@sources.command("list")
def source_list(
    category: Annotated[SourceCategory | None, typer.Option(help="Filter by source category.")] = None,
    enabled: Annotated[bool | None, typer.Option("--enabled/--disabled", help="Filter by registry state.")] = None,
) -> None:
    """List registered sources."""
    async def action(settings: Settings, pool: asyncpg.Pool):
        async with pool.acquire() as connection:
            return await SourceRegistry(connection).list(category=category, enabled=enabled)

    _json([_source_summary(row) for row in _execute("sources.list", action)])


@sources.command("add")
def source_add(
    config: Annotated[Path, typer.Option("--config", help="JSON file containing a source object or array.")],
) -> None:
    """Validate and register source configurations."""
    try:
        loaded = TypeAdapter(SourceConfig | list[SourceConfig]).validate_json(config.read_bytes())
    except OSError:
        typer.echo("Could not read the source configuration file.", err=True)
        raise typer.Exit(2) from None
    except ValidationError:
        typer.echo("Invalid source JSON. Check its fields against docs/example_sources.json.", err=True)
        raise typer.Exit(2) from None
    configs = loaded if isinstance(loaded, list) else [loaded]
    if not configs:
        typer.echo("Provide at least one source configuration.", err=True)
        raise typer.Exit(2)

    async def action(settings: Settings, pool: asyncpg.Pool):
        async with transaction(pool) as connection:
            registry = SourceRegistry(connection)
            return [await registry.register(source) for source in configs]

    _json([_source_summary(row) for row in _execute("sources.add", action)])


def _change_source(source_id: str, *, enabled: bool) -> None:
    async def action(settings: Settings, pool: asyncpg.Pool):
        async with pool.acquire() as connection:
            return await SourceRegistry(connection).set_enabled(source_id, enabled)

    operation = "sources.enable" if enabled else "sources.disable"
    _json(_source_summary(_execute(operation, action, source_id=source_id)))


@sources.command("enable")
def source_enable(source_id: Annotated[str, typer.Argument(help="Stable source ID.")]) -> None:
    """Enable a registered source."""
    _change_source(source_id, enabled=True)


@sources.command("disable")
def source_disable(source_id: Annotated[str, typer.Argument(help="Stable source ID.")]) -> None:
    """Disable a registered source."""
    _change_source(source_id, enabled=False)


@tasks.command("status")
def task_status(
    task_id: Annotated[UUID | None, typer.Argument(help="Task UUID. Omit for queue counts.")] = None,
) -> None:
    """Report task status or counts by status."""
    async def action(settings: Settings, pool: asyncpg.Pool):
        async with pool.acquire() as connection:
            queue = TaskQueue(connection, settings)
            if task_id is None:
                return await queue.status_counts()
            row = await queue.get_task_status(task_id)
            if row is None:
                raise TaskNotFound
            return {
                "id": str(row["id"]),
                "status": row["status"],
                "attempts": row["attempts"],
                "max_attempts": row["max_attempts"],
                "last_error": row["last_error"],
                "processing_result": row["processing_result"],
                "next_retry_at": row["next_retry_at"].isoformat() if row["next_retry_at"] else None,
                "lease_expires_at": row["lease_expires_at"].isoformat() if row["lease_expires_at"] else None,
            }

    _json(_execute("tasks.status", action, task_id=task_id))


def _scheduled(feature: str) -> None:
    typer.echo(f"Not implemented yet: {feature} is scheduled after Phase 1.", err=True)
    raise typer.Exit(2)


@app.command()
def run() -> None:
    """Run the pipeline (future phase)."""
    _scheduled("pipeline execution")


@app.command()
def crawl(
    source: Annotated[str, typer.Option(help="Source ID.")],
    limit: Annotated[int, typer.Option(min=1, max=1_000_000, help="Maximum distinct source items.")] = 5,
    plain: Plain = False, json_output: Json = False, no_color: NoColor = False,
) -> None:
    """Discover, validate, and persist records from one configured source."""
    presentation = Presentation(plain, json_output, no_color, automation_json=True)
    start = perf_counter()
    with presentation.execution("Crawl execution") as observer:
        async def action(settings: Settings, pool: asyncpg.Pool):
            async with HttpClient(settings) as http:
                return await IngestionService(pool, settings, http, observer).crawl(source, limit)

        report = _execute("crawl", action, source_id=source, error_view=presentation.error)
    presentation.output(report, lambda renderer: renderer.summary(
        "Crawl execution", {**report, "duration_seconds": round(perf_counter() - start, 3)},
    ))
    if report["status"] != "COMPLETED":
        raise typer.Exit(1)


@app.command()
def validate() -> None:
    """Audit stored canonical records against their validation contracts."""
    async def action(settings: Settings, pool: asyncpg.Pool):
        async with pool.acquire() as connection:
            return await validate_stored_records(connection)

    report = _execute("validate", action)
    _json(report)
    if report["invalid"]:
        raise typer.Exit(1)


@app.command()
def resume() -> None:
    """Recover leases and resume eligible crawl runs and ingestion tasks."""
    async def action(settings: Settings, pool: asyncpg.Pool):
        async with HttpClient(settings) as http:
            return await IngestionService(pool, settings, http).resume()

    reports = _execute("resume", action)
    _json({"runs": reports})
    if any(report["status"] != "COMPLETED" for report in reports):
        raise typer.Exit(1)


@app.command()
def export(target: Annotated[str, typer.Option(help="Export target, for example sheets.")]) -> None:
    """Export verified records (future phase)."""
    _scheduled("verified-record export")


register_operations(app, _execute)
