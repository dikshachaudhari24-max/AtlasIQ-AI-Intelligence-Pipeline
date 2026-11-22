from typing import Annotated

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, JsonValue, StringConstraints, field_validator

from atlasiq.db.models import SourceCategory
from atlasiq.schemas.entities import Text
from atlasiq.utils.urls import canonicalize_url

SourceID = Annotated[str, StringConstraints(pattern=r"^[a-z0-9]([a-z0-9_-]{0,62}[a-z0-9])?$")]


class RateLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    requests_per_minute: int = Field(gt=0, le=2_147_483_647)
    max_concurrency: int = Field(default=1, gt=0, le=2_147_483_647)


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    id: SourceID
    name: Text
    category: SourceCategory = Field(strict=False)
    base_url: str
    adapter_name: SourceID
    adapter_config: dict[str, JsonValue] = Field(default_factory=dict, repr=False)
    schedule_config: dict[str, JsonValue] | None = Field(default=None, repr=False)
    rate_limit_config: RateLimits
    enabled: bool = False

    @field_validator("base_url")
    @classmethod
    def normalize_base_url(cls, value: str) -> str:
        return canonicalize_url(value)


class SourceAlreadyRegistered(Exception):
    pass


class SourceNotFound(Exception):
    pass


class SourceRegistry:
    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    async def register(self, source: SourceConfig) -> asyncpg.Record:
        row = await self.connection.fetchrow("""
            INSERT INTO public.sources (
                id, name, category, base_url, adapter_name, adapter_config,
                schedule_config, rate_limit_config, enabled
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT DO NOTHING
            RETURNING *
        """, source.id, source.name, source.category, source.base_url, source.adapter_name,
            source.adapter_config, source.schedule_config, source.rate_limit_config.model_dump(), source.enabled)
        if row is None:
            raise SourceAlreadyRegistered
        return row

    async def get(self, source_id: str) -> asyncpg.Record | None:
        return await self.connection.fetchrow("SELECT * FROM public.sources WHERE id = $1", source_id)

    async def list(
        self, *, category: SourceCategory | None = None, enabled: bool | None = None
    ) -> list[asyncpg.Record]:
        return await self.connection.fetch("""
            SELECT * FROM public.sources
            WHERE ($1::text IS NULL OR category = $1) AND ($2::boolean IS NULL OR enabled = $2)
            ORDER BY id
        """, category, enabled)

    async def set_enabled(self, source_id: str, enabled: bool) -> asyncpg.Record:
        row = await self.connection.fetchrow("""
            UPDATE public.sources
            SET enabled = $2,
                updated_at = CASE WHEN enabled <> $2 THEN clock_timestamp() ELSE updated_at END
            WHERE id = $1
            RETURNING *
        """, source_id, enabled)
        if row is None:
            raise SourceNotFound
        return row
