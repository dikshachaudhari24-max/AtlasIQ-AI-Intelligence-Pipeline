import json
import re
from datetime import datetime
from hashlib import sha256
from urllib.parse import urlencode

from pydantic import Field

from atlasiq.crawlers.base import (
    JSON, AdapterOptions, BaseCrawler, InvalidRecord, Item, Page, Parsed, compact_json, record_fields,
)
from atlasiq.crawlers.http_client import FetchError
from atlasiq.crawlers.identity import identity_key
from atlasiq.db.models import SourceCategory
from atlasiq.schemas.entities import Startup

API = "https://query.wikidata.org/sparql"
ENTITY = re.compile(r"https?://www\.wikidata\.org/entity/(Q[1-9][0-9]*)")
EMPLOYEES = re.compile(r"\+?([0-9]{1,19})(?:\.0+)?")


class WikidataOptions(AdapterOptions):
    page_size: int = Field(default=50, ge=1, le=100)


def _binding(row: object, name: str) -> str | None:
    value = row.get(name) if isinstance(row, dict) else None
    if isinstance(value, dict) and isinstance(value.get("value"), str):
        return value["value"]
    return None


class WikidataCrawler(BaseCrawler):
    category = SourceCategory.STARTUP
    hosts = frozenset({"query.wikidata.org"})
    max_rpm = 10
    max_concurrency = 1

    def __init__(self, source, http):
        super().__init__(source, http)
        self.options = WikidataOptions.model_validate(source.adapter_config)

    async def discover(self, cursor, limit: int) -> Page:
        offset = int(cursor or 0)
        size = min(limit, self.options.page_size)
        # Group by company to prevent multiple employee statements from multiplying records.
        query = f"""
SELECT ?item ?label (GROUP_CONCAT(DISTINCT STR(?headcount); SEPARATOR=",") AS ?employees)
WHERE {{
  ?item wdt:P31 wd:Q129238;
        wdt:P452 wd:Q11660;
        rdfs:label ?label.
  FILTER(LANG(?label) = 'en')
  OPTIONAL {{ ?item wdt:P1128 ?headcount. }}
}}
GROUP BY ?item ?label
ORDER BY STR(?item)
LIMIT {size} OFFSET {offset}
"""
        request = API + "?" + urlencode({"query": query, "format": "json"})
        response = await self.http.get(request, self.policy, JSON)
        body = response.json()
        results = body.get("results") if isinstance(body, dict) else None
        if not isinstance(results, dict) or not isinstance(results.get("bindings"), list):
            raise FetchError("WIKIDATA_INVALID_RESULTS")
        rows = results["bindings"]
        items = []
        for row in rows:
            raw = compact_json(row)
            entity = ENTITY.fullmatch(_binding(row, "item") or "")
            identity = entity[1] if entity else "unidentified:" + sha256(raw.encode("utf-8")).hexdigest()
            source_url = f"https://www.wikidata.org/wiki/{identity}" if entity else API
            items.append(Item(identity, source_url, raw))
        return Page(items, offset + len(rows) if len(rows) == size else None, response.fetched_at, request)

    def parse(self, raw: str, source_url: str, fetched_at: datetime) -> Parsed:
        row = json.loads(raw)
        entity = ENTITY.fullmatch(_binding(row, "item") or "")
        label = _binding(row, "label")
        if entity is None or not label:
            raise InvalidRecord("WIKIDATA_MISSING_ID_OR_NAME")
        observed = EMPLOYEES.fullmatch(_binding(row, "employees") or "")
        count = int(observed[1]) if observed and int(observed[1]) < 2**63 else None
        record = Startup.model_validate({
            **record_fields(self.source, source_url),
            "content": {"entityName": label, "data": {"employeeCount": count}},
            "collectedAt": fetched_at,
        })
        return Parsed(record, identity_key(self.category, self.source.id, entity[1], source_url))
