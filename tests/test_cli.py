import json
from pathlib import Path
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from atlasiq.cli import app

runner = CliRunner()


def test_help_works_without_database(clean_env):
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert all(command in result.stdout for command in ("health", "db", "sources", "tasks", "export"))


@pytest.mark.parametrize("args", [
    ["run"], ["export", "--target", "sheets"],
])
def test_future_commands_are_explicit_stubs(clean_env, args):
    result = runner.invoke(app, args)
    assert result.exit_code == 2
    assert "Not implemented yet" in result.stderr


def test_health_provides_safe_configuration_error(clean_env, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "https://user:private-credential@example.org/atlasiq")
    result = runner.invoke(app, ["health"])
    assert result.exit_code == 1
    assert "DATABASE_URL" in result.stderr and "private-credential" not in result.stderr
    event = json.loads(result.stderr.splitlines()[0])
    assert event["operation"] == "health" and event["error_type"] == "ConfigurationError"
    assert not result.stdout


def test_invalid_source_file_is_actionable_without_database(clean_env, tmp_path):
    config = tmp_path / "invalid.json"
    config.write_text('{"adapter_config":{"secret":"private-value"}}', encoding="utf-8")
    result = runner.invoke(app, ["sources", "add", "--config", str(config)])
    assert result.exit_code == 2 and "Invalid source JSON" in result.stderr
    assert "private-value" not in result.stderr


@pytest.mark.integration
def test_database_source_and_task_cli(disposable_settings, monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", disposable_settings.asyncpg_dsn())
    monkeypatch.setenv("APP_ENV", "test")
    check = runner.invoke(app, ["db", "check"])
    assert check.exit_code == 0 and json.loads(check.stdout)["database"] == "reachable"
    pending = runner.invoke(app, ["health"])
    assert pending.exit_code == 1 and json.loads(pending.stdout)["migrations"] == "pending"

    migrated = runner.invoke(app, ["db", "migrate"])
    assert migrated.exit_code == 0 and json.loads(migrated.stdout)["newly_applied"] == [
        "0001_foundation.sql", "0002_crawling.sql",
    ]
    health = runner.invoke(app, ["health"])
    assert health.exit_code == 0 and json.loads(health.stdout)["migrations"] == "current"

    config = Path(__file__).resolve().parents[1] / "docs" / "example_sources.json"
    added = runner.invoke(app, ["sources", "add", "--config", str(config)])
    assert added.exit_code == 0 and len(json.loads(added.stdout)) == 2
    enabled = runner.invoke(app, ["sources", "enable", "papers"])
    assert enabled.exit_code == 0 and json.loads(enabled.stdout)["enabled"]
    assert json.loads(enabled.stderr)["source_id"] == "papers"
    listed = runner.invoke(app, ["sources", "list", "--category", "RESEARCH_PAPER", "--enabled"])
    assert listed.exit_code == 0 and [row["id"] for row in json.loads(listed.stdout)] == ["papers"]
    disabled = runner.invoke(app, ["sources", "disable", "papers"])
    assert disabled.exit_code == 0 and not json.loads(disabled.stdout)["enabled"]

    existing = json.loads(config.read_text(encoding="utf-8"))[0]
    conflicting_batch = tmp_path / "sources.json"
    conflicting_batch.write_text(
        json.dumps([existing | {"id": "extra-papers", "name": "An Extra Source"}, existing]), encoding="utf-8",
    )
    conflict = runner.invoke(app, ["sources", "add", "--config", str(conflicting_batch)])
    assert conflict.exit_code == 1 and "already exists" in conflict.stderr
    remaining = runner.invoke(app, ["sources", "list", "--disabled"])
    assert {row["id"] for row in json.loads(remaining.stdout)} == {"papers", "product-hunt"}

    counts = runner.invoke(app, ["tasks", "status"])
    assert counts.exit_code == 0 and sum(json.loads(counts.stdout).values()) == 0
    missing = runner.invoke(app, ["tasks", "status", str(uuid4())])
    assert missing.exit_code == 1 and "No task" in missing.stderr
    assert json.loads(runner.invoke(app, ["resume"]).stdout) == {"runs": []}
    assert json.loads(runner.invoke(app, ["validate"]).stdout)["checked"] == 0
