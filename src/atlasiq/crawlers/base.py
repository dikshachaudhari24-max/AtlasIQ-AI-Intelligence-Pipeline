import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

from atlasiq.crawlers.http_client import HttpClient, RequestPolicy
from atlasiq.db.models import SourceCategory
from atlasiq.schemas.entities import Contract, Product, ResearchPaper, Startup
from atlasiq.sources.registry import SourceConfig

JSON = frozenset({"application/json", "application/sparql-results+json"})


class InvalidRecord(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class Item:
    upstream_id: str | None
    url: str
    raw: str


@dataclass(frozen=True)
class Page:
    items: list[Item]
    next_cursor: str | int | None
    fetched_at: datetime
    request_url: str


@dataclass
class Parsed:
    record: Startup | Product | ResearchPaper
    canonical_key: str
    metadata: dict = field(default_factory=dict)


class AdapterOptions(Contract):
    pass


def compact_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def record_fields(source: SourceConfig, url: str) -> dict:
    return {"schemaVersion": "1.0", "recordType": source.category.value, "source": {"name": source.name, "url": url}}


class BaseCrawler(ABC):
    category: SourceCategory
    hosts: frozenset[str]
    max_rpm: int = 60
    max_concurrency: int = 4

    def __init__(self, source: SourceConfig, http: HttpClient):
        self.source = source
        self.http = http
        self.policy = RequestPolicy(
            self.hosts, min(self.max_rpm, source.rate_limit_config.requests_per_minute),
            min(self.max_concurrency, source.rate_limit_config.max_concurrency),
        )

    @abstractmethod
    async def discover(self, cursor: str | int | None, limit: int) -> Page:
        """Fetch one bounded page and preserve each source item verbatim or structurally."""

    async def fetch(self, raw: str) -> str:
        """Use the item evidence already fetched in discovery, or fetch additional evidence."""
        return raw

    @abstractmethod
    def parse(self, raw: str, source_url: str, fetched_at: datetime) -> Parsed:
        """Extract deterministic fields and validate the canonical record."""
