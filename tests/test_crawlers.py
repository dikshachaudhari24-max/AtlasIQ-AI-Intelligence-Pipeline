import json
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlsplit

import pytest

from atlasiq.config.settings import Settings
from atlasiq.crawlers.adapters import DEFAULTS
from atlasiq.crawlers.arxiv import ArxivCrawler
from atlasiq.crawlers.base import InvalidRecord
from atlasiq.crawlers.http_client import FetchError, Response
from atlasiq.crawlers.identity import arxiv_identity, identity_key
from atlasiq.crawlers.marketplace import MarketplaceCrawler
from atlasiq.crawlers.wikidata import WikidataCrawler
from atlasiq.enrichment.github import GitHubClient, associations, parse_repository_url

NOW = datetime(2026, 1, 10, 12, tzinfo=UTC)
PAPER_ID = "2601.12345"


class FakeHttp:
    def __init__(self, *payloads):
        self.payloads = list(payloads)
        self.calls = []

    async def get(self, url, policy, types, **kwargs):
        self.calls.append(url)
        result = self.payloads.pop(0)
        if isinstance(result, Exception):
            raise result
        return Response(result if isinstance(result, str) else json.dumps(result), NOW)


def feed(paper_id=PAPER_ID, *, start=0, total=1, title="Fixture Paper", include_repo=True):
    abstract = "Metadata from a test fixture."
    if include_repo:
        abstract += " Code: https://github.com/Example/Fixture."
    return f"""<feed xmlns="http://www.w3.org/2005/Atom" xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
    <opensearch:totalResults>{total}</opensearch:totalResults><opensearch:startIndex>{start}</opensearch:startIndex>
    <entry><id>http://arxiv.org/abs/{paper_id}v2</id><title>{title}</title><summary>{abstract}</summary>
    <published>2026-01-05T10:30:00+02:00</published>
    <author><name>  Example  Author </name></author><author><name>Example Author</name></author>
    </entry></feed>"""


def github_body(stars, *, owner="Example", name="Fixture"):
    return {"html_url": f"https://github.com/{owner}/{name}", "name": name, "full_name": f"{owner}/{name}",
            "owner": {"login": owner}, "private": False, "stargazers_count": stars}


def app_listing(*, key="demo.ai", name="AI Assistant for Jira", payment="atlassian"):
    return {"key": key, "name": name, "tagLine": "A generative AI assistant",
            "_links": {"alternate": {"href": "/apps/1234/fixture-app?tab=overview"}},
            "_embedded": {"vendor": {"name": "Fixture Vendor", "_links": {"self": {"href": "/rest/2/vendors/11"}}},
                          "version": {"paymentModel": payment}}}


@pytest.mark.asyncio
async def test_arxiv_pagination_parse_and_identity():
    http = FakeHttp(feed(total=2), feed(start=1, total=2))
    adapter = ArxivCrawler(DEFAULTS["papers"], http)
    page = await adapter.discover(None, 1)
    second = await adapter.discover(page.next_cursor, 1)
    assert page.next_cursor == 1 and second.next_cursor is None
    assert parse_qs(urlsplit(http.calls[1]).query)["start"] == ["1"]
    item = page.items[0]
    assert item.upstream_id == PAPER_ID and arxiv_identity("HEP-TH/9901001v10") == "hep-th/9901001"
    parsed = adapter.parse(item.raw, item.url, page.fetched_at)
    assert parsed.canonical_key == f"arxiv:{PAPER_ID}"
    assert parsed.record.content.authors == ["Example Author"]
    assert parsed.record.content.published_date.isoformat() == "2026-01-05T08:30:00+00:00"
    assert "Metadata" in parsed.metadata["abstract"] and parsed.metadata["collected_at"] == NOW


@pytest.mark.asyncio
async def test_arxiv_disallows_xml_entities():
    http = FakeHttp('<!DOCTYPE feed [<!ENTITY x "untrusted">]><feed>&x;</feed>')
    with pytest.raises(FetchError, match="ARXIV_INVALID_FEED"):
        await ArxivCrawler(DEFAULTS["papers"], http).discover(None, 5)


def test_startup_missing_and_supported_employee_values():
    source = DEFAULTS["startups"]
    adapter = WikidataCrawler(source, None)
    row = {"item": {"value": "http://www.wikidata.org/entity/Q1234"}, "label": {"value": "Test Startup"}}
    first_url = "https://www.wikidata.org/wiki/Q1234"
    parsed = adapter.parse(json.dumps(row), first_url, NOW)
    assert parsed.record.content.data.employeeCount is None
    row["employees"] = {"value": "25"}
    updated = adapter.parse(json.dumps(row), first_url, NOW)
    assert updated.record.content.data.employeeCount == 25 and updated.canonical_key == parsed.canonical_key
    assert identity_key("STARTUP", source.id, "Q2345", first_url) != updated.canonical_key


@pytest.mark.asyncio
async def test_wikidata_uses_bounded_pagination():
    row = {"item": {"value": "http://www.wikidata.org/entity/Q1234"}, "label": {"value": "Test Startup"}}
    http = FakeHttp({"results": {"bindings": [row]}}, {"results": {"bindings": []}})
    adapter = WikidataCrawler(DEFAULTS["startups"], http)
    page = await adapter.discover(None, 1)
    assert page.next_cursor == 1 and page.items[0].upstream_id == "Q1234"
    assert (await adapter.discover(page.next_cursor, 1)).next_cursor is None
    assert "LIMIT 1 OFFSET 1" in parse_qs(urlsplit(http.calls[1]).query)["query"][0]


@pytest.mark.asyncio
async def test_product_fetch_version_pagination_pricing_and_verified_vendor_mapping():
    config = DEFAULTS["products"].model_copy(update={"adapter_config": {"vendor_startup_keys": {"11": "known-key"}}})
    app = app_listing(payment=None)
    http = FakeHttp({
        "_embedded": {"addons": [app]},
        "_links": {"next": [{"href": "/rest/2/addons?hosting=cloud&offset=1&limit=1"}]},
    }, {"paymentModel": "free"}, {"count": 0})
    adapter = MarketplaceCrawler(config, http)
    page = await adapter.discover(None, 1)
    [item] = page.items
    details = await adapter.fetch(item.raw)
    parsed = adapter.parse(details, item.url, NOW)
    assert parsed.record.content.pricingModel == "FREE" and parsed.record.content.startupName == "Fixture Vendor"
    assert parsed.metadata == {"product_name": "AI Assistant for Jira", "startup_key": "known-key"}
    assert "/versions/latest?hosting=cloud" in http.calls[1]
    assert (await adapter.discover(page.next_cursor, 1)).items == []
    assert parse_qs(urlsplit(http.calls[2]).query)["offset"] == ["1"]
    paid = adapter.parse(json.dumps(app_listing(payment="vendor")), item.url, NOW)
    assert paid.canonical_key == parsed.canonical_key and paid.record.content.pricingModel == "PAID"


def test_product_missing_pricing_is_rejected():
    with pytest.raises(InvalidRecord, match="MARKETPLACE_MISSING_PRICING"):
        MarketplaceCrawler(DEFAULTS["products"], None).parse(
            json.dumps(app_listing(payment=None)), "https://marketplace.atlassian.com/apps/1234", NOW,
        )


@pytest.mark.parametrize("url", [
    "https://github.com/example", "https://example.org/a/b", "https://github.com/a/b/issues",
    "https://github.com@evil.example/a/b", "https://github.com/example/..",
])
def test_unverified_github_urls(url):
    assert parse_repository_url(url) is None


@pytest.mark.asyncio
async def test_github_uses_explicit_links_and_distinguishes_zero_missing_and_invalid():
    paper_url = "https://arxiv.org/abs/2601.12345"
    abstract = "Code https://github.com/example/fixture.; additional https://github.com/a/missing"
    links = ["https://github.com/Example/Fixture", "https://github.com/a/rate", "https://github.com/a/invalid"]
    refs = associations(links, abstract, paper_url)
    assert refs[0].evidence["kind"] == "arxiv_metadata_link"
    assert len(refs) == 4
    http = FakeHttp(github_body(0), FetchError("HTTP_RATE_LIMIT", transient=True, status=429),
                    {"stargazers_count": 10}, FetchError("HTTP_404", status=404))
    settings = Settings(_env_file=None, database_url="postgresql://user@localhost/test")
    metrics = await GitHubClient(http, settings).collect(refs)
    assert [(metric.repository.identity, metric.stars) for metric in metrics] == [
        ("example/fixture", 0), ("a/rate", None),
    ]
    assert metrics[1].evidence["metric_error"] == "HTTP_RATE_LIMIT"
