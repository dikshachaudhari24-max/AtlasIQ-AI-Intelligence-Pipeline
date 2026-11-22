import asyncio
from time import monotonic
from typing import Annotated
from uuid import UUID

import typer
from rich.live import Live

from atlasiq.core.observability import read_dashboard
from atlasiq.intelligence.providers import ProviderSession, configured_providers
from atlasiq.intelligence.service import IntelligenceError, IntelligenceService
from atlasiq.intelligence.storage import inspect
from atlasiq.ui.presentation import Presentation

Plain = Annotated[bool, typer.Option("--plain", help="Render a static ASCII view.")]
Json = Annotated[bool, typer.Option("--json", help="Emit a machine-readable JSON document.")]
NoColor = Annotated[bool, typer.Option("--no-color", help="Disable color styling.")]
Watch = Annotated[bool, typer.Option("--watch", help="Refresh in an interactive terminal until Ctrl+C.")]
Refresh = Annotated[float, typer.Option("--refresh", min=0.5, max=60.0, help="Watch interval in seconds.")]


def register_operations(app: typer.Typer, execute) -> None:
    dashboard = typer.Typer(help="Inspect PostgreSQL-backed pipeline activity.", invoke_without_command=True)
    intelligence = typer.Typer(help="Process and inspect source-backed intelligence.", no_args_is_help=True)
    app.add_typer(dashboard, name="dashboard")
    app.add_typer(intelligence, name="intelligence")

    def show(view: str, plain: bool, json_output: bool, no_color: bool,
             watch: bool, refresh: float, diagnostics: bool = False) -> None:
        presentation = Presentation(plain, json_output, no_color)

        async def action(settings, pool):
            has_provider = bool(configured_providers(settings))

            async def snapshot(cached=None):
                current = await read_dashboard(pool, settings.app_env, cached)
                if not has_provider and current["extraction"]["PENDING"] and not current["extraction"]["CLAIMED"]:
                    current["stages"]["extract"] = {"state": "BLOCKED", "detail": "No provider credentials configured"}
                return current

            data = await snapshot()
            if not watch or not presentation.interactive:
                return data
            updated = monotonic()
            with Live(presentation.renderer().dashboard(data, view, diagnostics), console=presentation.console,
                      auto_refresh=False, redirect_stdout=False, redirect_stderr=False) as live:
                while True:
                    await asyncio.sleep(refresh)
                    recalculate = monotonic() - updated >= 15
                    data = await snapshot(None if recalculate else {
                        key: value for key, value in data["metrics"].items() if key not in {"queued", "failed"}
                    })
                    if recalculate:
                        updated = monotonic()
                    live.update(presentation.renderer().dashboard(data, view, diagnostics), refresh=True)

        try:
            result = execute(f"dashboard.{view}", action, error_view=presentation.error)
        except KeyboardInterrupt:
            raise typer.Exit(0) from None
        presentation.output(result, lambda renderer: renderer.dashboard(result, view, diagnostics))

    @dashboard.callback()
    def overview(
        context: typer.Context, plain: Plain = False, json_output: Json = False, no_color: NoColor = False,
        watch: Watch = False, refresh: Refresh = 2,
    ) -> None:
        """Show health, records, sources, and the connected pipeline."""
        if context.invoked_subcommand is None:
            show("overview", plain, json_output, no_color, watch, refresh)

    @dashboard.command()
    def pipeline(
        plain: Plain = False, json_output: Json = False, no_color: NoColor = False,
        watch: Watch = False, refresh: Refresh = 2,
    ) -> None:
        """Show persisted stage states and record counts."""
        show("pipeline", plain, json_output, no_color, watch, refresh)

    @dashboard.command()
    def sources(
        plain: Plain = False, json_output: Json = False, no_color: NoColor = False,
        watch: Watch = False, refresh: Refresh = 2,
    ) -> None:
        """Show source registry state, latest crawl, and successful ingestion."""
        show("sources", plain, json_output, no_color, watch, refresh)

    @dashboard.command()
    def tasks(
        plain: Plain = False, json_output: Json = False, no_color: NoColor = False,
        watch: Watch = False, refresh: Refresh = 2,
        diagnostics: Annotated[bool, typer.Option("--diagnostics", help="Show full task IDs and lease owners.")] = False,
    ) -> None:
        """Show queue counts, leases, retries, and recent provider activity."""
        show("tasks", plain, json_output, no_color, watch, refresh, diagnostics)

    @intelligence.command()
    def run(
        limit: Annotated[int, typer.Option(min=1, max=1000, help="Maximum eligible tasks to process.")] = 5,
        retry_failed: Annotated[bool, typer.Option(help="Requeue previously failed tasks within the limit.")] = False,
        plain: Plain = False, json_output: Json = False, no_color: NoColor = False,
    ) -> None:
        """Extract structured intelligence from persisted source evidence."""
        presentation = Presentation(plain, json_output, no_color, automation_json=True)
        with presentation.execution("Intelligence execution") as observer:
            async def action(settings, pool):
                configs = configured_providers(settings)
                if not configs:
                    return await IntelligenceService(pool, settings, [], observer).run(limit)
                try:
                    async with ProviderSession(settings, configs) as providers:
                        return await IntelligenceService(pool, settings, providers, observer).run(
                            limit, retry_failed=retry_failed,
                        )
                except ValueError as error:
                    # Provider model identifiers are the only user-supplied value that
                    # can raise this constructor error before a request is sent.
                    if str(error) != "invalid configured LLM model ID":
                        raise
                    raise IntelligenceError("A configured LLM model ID is invalid. Check LLM_*_MODEL settings.") from None

            report = execute("intelligence.run", action, error_view=presentation.error)
        presentation.output(report, lambda renderer: renderer.summary("Intelligence execution", report))
        if report["status"] not in {"COMPLETED", "SKIPPED"}:
            raise typer.Exit(1)

    @intelligence.command("inspect")
    def inspect_record(
        record_id: Annotated[UUID, typer.Option("--id", help="Canonical record UUID or intelligence result UUID.")],
        plain: Plain = False, json_output: Json = False, no_color: NoColor = False,
    ) -> None:
        """Inspect stored facts with their evidence, task state, and provider attempts."""
        presentation = Presentation(plain, json_output, no_color)

        async def action(settings, pool):
            async with pool.acquire() as connection:
                result = await inspect(connection, record_id)
            if result is None:
                raise IntelligenceError("No canonical or intelligence record has that ID.")
            return result

        result = execute("intelligence.inspect", action, error_view=presentation.error)
        presentation.output(result, lambda renderer: renderer.inspection(result))
