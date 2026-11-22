import io
import json
from datetime import UTC, datetime

import pytest
from rich.console import Console
from typer.testing import CliRunner

from atlasiq.cli import app
from atlasiq.ui.presentation import supports_symbols
from atlasiq.ui.render import THEME, Renderer

runner = CliRunner()


def captured(renderable, width, *, color=False, no_color=False):
    stream = io.StringIO()
    console = Console(file=stream, width=width, theme=THEME, force_terminal=color, no_color=no_color,
                      color_system="standard" if color else None)
    console.print(renderable)
    return stream.getvalue()


def test_narrow_pipeline_and_source_names_wrap_with_ascii_fallback():
    renderer = Renderer(True, 46)
    now = datetime.now(UTC)
    row = {"id": "paper-fixture", "name": "Extremely long public research publication name without assumptions",
           "category": "RESEARCH_PAPER", "adapter_name": "arxiv", "enabled": True,
           "last_status": "COMPLETED", "last_run": now, "last_success": now,
           "discovered": 9, "persisted": 6, "errors": 1}
    table = captured(renderer.sources([row, {**row, "id": "another-source"}]), 46)
    assert "paper-fixture" in table and "another-source" in table
    assert all(len(line) <= 46 for line in table.splitlines())
    path = captured(renderer.flow({"discover": {"state": "RETRYING", "detail": "provider delay"} }), 46)
    assert "~ Retrying" in path and "Export" in path
    assert not supports_symbols("cp1252") and supports_symbols("utf-8")


def test_status_colors_and_no_color():
    renderer = Renderer(False, 90)
    assert "\x1b[32m" in captured(renderer.status("COMPLETED"), 90, color=True)
    assert "\x1b[31m" in captured(renderer.status("FAILED"), 90, color=True)
    assert "\x1b[" not in captured(renderer.status("FAILED"), 90, color=True, no_color=True)


def test_unknown_commands_invalid_arguments_and_missing_configuration(clean_env):
    assert runner.invoke(app, ["dashboard", "unrecognized"]).exit_code == 2
    assert runner.invoke(app, ["intelligence", "inspect", "--id", "not-a-uuid"]).exit_code == 2
    mixed = runner.invoke(app, ["dashboard", "--plain", "--json"])
    assert mixed.exit_code == 2
    unavailable = runner.invoke(app, ["dashboard", "--plain"])
    assert unavailable.exit_code == 1
    assert "Operation blocked" in unavailable.stderr and "DATABASE_URL" in unavailable.stderr


def test_unreachable_database_is_visible(clean_env, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://test@127.0.0.1:1/testdb?sslmode=disable")
    result = runner.invoke(app, ["dashboard", "--plain"])
    assert result.exit_code == 1
    assert "PostgreSQL is unreachable" in result.stderr and "Connected" not in result.stdout


@pytest.mark.integration
def test_empty_database_plain_and_json_watch_degrades_to_snapshot(disposable_settings, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", disposable_settings.asyncpg_dsn())
    monkeypatch.setenv("CI", "1")
    assert runner.invoke(app, ["db", "migrate"]).exit_code == 0
    table = runner.invoke(app, ["dashboard", "--plain", "--watch"])
    assert table.exit_code == 0, table.stderr
    assert "No sources are registered." in table.stdout and "Connected" in table.stdout
    assert "\x1b[" not in table.stdout
    machine = runner.invoke(app, ["dashboard", "tasks", "--json", "--watch"])
    assert machine.exit_code == 0
    state = json.loads(machine.stdout)
    assert state["queue"]["PENDING"] == 0 and state["sources"] == []
    assert state["stages"]["export"]["state"] == "BLOCKED"
    assert runner.invoke(app, ["intelligence", "run", "--json"]).exit_code == 1
