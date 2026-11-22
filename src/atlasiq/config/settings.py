from typing import Annotated, Literal

from pydantic import Field, PostgresDsn, SecretStr, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Positive = Annotated[int, Field(gt=0, le=2_147_483_647)]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
        env_ignore_empty=True,
    )

    app_name: str = Field(default="AtlasIQ", min_length=1)
    app_env: Literal["development", "test", "staging", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    database_url: SecretStr
    gemini_api_key: SecretStr | None = None
    groq_api_key: SecretStr | None = None
    deepseek_api_key: SecretStr | None = None
    github_token: SecretStr | None = None
    google_sheets_spreadsheet_id: SecretStr | None = None
    google_application_credentials: SecretStr | None = None
    llm_provider_order: str = "gemini,groq,deepseek"
    llm_gemini_model: str = Field(default="gemini-flash-latest", min_length=1, max_length=100)
    llm_groq_model: str = Field(default="openai/gpt-oss-120b", min_length=1, max_length=100)
    llm_deepseek_model: str = Field(default="deepseek-flash", min_length=1, max_length=100)
    llm_workers: int = Field(default=3, ge=1, le=16)
    llm_requests_per_minute: int = Field(default=15, ge=1, le=1000)
    llm_timeout_seconds: int = Field(default=35, ge=1, le=180)
    llm_provider_attempts: int = Field(default=2, ge=1, le=5)
    llm_max_retry_wait_seconds: int = Field(default=15, ge=1, le=120)
    llm_chunk_bytes: int = Field(default=5000, ge=128, le=64000)
    llm_max_chunks: int = Field(default=24, ge=1, le=128)
    llm_request_bytes: int = Field(default=30000, ge=4096, le=256000)
    llm_input_token_budget: int = Field(default=12000, ge=4096, le=256000)
    llm_output_tokens: int = Field(default=2048, ge=128, le=16384)
    llm_attempt_budget: int = Field(default=96, ge=1, le=512)

    crawler_max_concurrency: Positive = 10
    crawler_request_timeout_seconds: Positive = 30
    crawler_http_attempts: int = Field(default=3, ge=1, le=8)
    crawler_max_retry_wait_seconds: int = Field(default=30, ge=1, le=300)
    crawler_max_response_bytes: int = Field(default=4_194_304, ge=1, le=33_554_432)
    crawler_worker_concurrency: int = Field(default=4, ge=1, le=32)
    crawler_user_agent: str = Field(default="AtlasIQ/0.2 (public-source ingestion)", min_length=8, max_length=256)
    task_lease_seconds: Positive = 120
    task_max_attempts: Positive = 5
    task_retry_base_seconds: Positive = 5
    task_retry_max_seconds: Positive = 300

    @field_validator("llm_provider_order")
    @classmethod
    def validate_provider_order(cls, value: str) -> str:
        order = [entry.strip().lower() for entry in value.split(",")]
        if any(name not in {"gemini", "groq", "deepseek"} for name in order) or len(set(order)) != len(order):
            raise ValueError("provide a comma-separated list of distinct providers from gemini, groq, deepseek")
        return ",".join(order)

    @field_validator("crawler_user_agent")
    @classmethod
    def validate_user_agent(cls, value: str) -> str:
        if not value.isascii() or not value.isprintable():
            raise ValueError("use printable ASCII for the HTTP user agent")
        return value

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr) -> SecretStr:
        try:
            parsed = PostgresDsn(value.get_secret_value())
        except ValueError:
            raise ValueError("use a PostgreSQL URL with a host and database name") from None
        if (
            parsed.scheme not in {"postgres", "postgresql", "postgresql+asyncpg"}
            or not parsed.path or not parsed.path.strip("/")
        ):
            raise ValueError("use a PostgreSQL URL with a host and database name")
        return SecretStr(str(parsed))

    @model_validator(mode="after")
    def validate_retry_bounds(self) -> "Settings":
        if self.task_retry_max_seconds < self.task_retry_base_seconds:
            raise ValueError("TASK_RETRY_MAX_SECONDS must be at least TASK_RETRY_BASE_SECONDS")
        return self

    def asyncpg_dsn(self) -> str:
        dsn = self.database_url.get_secret_value()
        scheme, separator, address = dsn.partition("://")
        if scheme == "postgresql+asyncpg":
            return f"postgresql{separator}{address}"
        return dsn


class ConfigurationError(Exception):
    pass


def load_settings() -> Settings:
    try:
        return Settings()
    except ValidationError as error:
        details = []
        for issue in error.errors(include_input=False, include_context=False, include_url=False):
            field = ".".join(str(part).upper() for part in issue["loc"]) or "Settings"
            details.append(f"{field}: {issue['msg']}")
        raise ConfigurationError("; ".join(details)) from None
