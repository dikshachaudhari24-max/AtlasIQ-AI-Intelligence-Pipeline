import json
import logging
import sys

import pytest
from pydantic import SecretStr

from atlasiq.config.settings import ConfigurationError, load_settings
from atlasiq.core.logging import JsonFormatter


def test_valid_dotenv_and_environment_override(clean_env, tmp_path, monkeypatch):
    (tmp_path / ".env").write_text(
        "DATABASE_URL=postgresql+asyncpg://user:encoded%40password@localhost:5432/atlasiq\n"
        "CRAWLER_MAX_CONCURRENCY=5\nGEMINI_API_KEY=\nEXTRA_SETTING=ignored\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CRAWLER_MAX_CONCURRENCY", "12")
    settings = load_settings()
    assert settings.crawler_max_concurrency == 12
    assert settings.gemini_api_key is None
    assert settings.asyncpg_dsn() == "postgresql://user:encoded%40password@localhost:5432/atlasiq"
    assert isinstance(settings.database_url, SecretStr)
    assert "encoded" not in str(settings)


def test_missing_database(clean_env):
    with pytest.raises(ConfigurationError, match="DATABASE_URL.*required"):
        load_settings()


def test_postgres_dsn_is_normalized_for_asyncpg(clean_env, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", " POSTGRESQL://user@localhost/atlasiq ")
    assert load_settings().asyncpg_dsn() == "postgresql://user@localhost/atlasiq"


@pytest.mark.parametrize("key,value", [
    ("CRAWLER_MAX_CONCURRENCY", "0"), ("CRAWLER_REQUEST_TIMEOUT_SECONDS", "invalid"),
    ("TASK_LEASE_SECONDS", "-1"), ("TASK_MAX_ATTEMPTS", "2147483648"),
    ("TASK_RETRY_BASE_SECONDS", "0"), ("TASK_RETRY_MAX_SECONDS", "2"),
])
def test_invalid_numeric_settings(clean_env, monkeypatch, key, value):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user@localhost/atlasiq")
    monkeypatch.setenv(key, value)
    with pytest.raises(ConfigurationError, match=key):
        load_settings()


def test_malformed_dsn_message_is_redacted(clean_env, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "mysql://user:private-password@localhost/mydb")
    with pytest.raises(ConfigurationError, match="PostgreSQL URL") as caught:
        load_settings()
    assert "private-password" not in str(caught.value)


def test_json_logs_omit_untrusted_messages_and_exception_text():
    try:
        raise RuntimeError("raw-password and private@example.org")
    except RuntimeError:
        record = logging.LogRecord("atlasiq.test", logging.ERROR, __file__, 1, "raw-password", (), sys.exc_info())
    record.operation = "db.check"
    record.safe_message = "Database connection failed"
    record.source_id = "papers"
    record.worker_id = "private@example.org"
    record.duration_ms = 12
    rendered = JsonFormatter(diagnostics=True).format(record)
    log = json.loads(rendered)
    assert log["timestamp"].endswith("Z")
    assert log["error_type"] == "RuntimeError"
    assert log["error_message"] == "Database connection failed"
    assert log["source_id"] == "papers"
    assert log["operation"] == "db.check"
    assert log["duration_ms"] == 12
    assert log["stack"][0]["file"] == "test_config.py"
    assert "raw-password" not in rendered and "private@example.org" not in rendered
