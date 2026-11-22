from atlasiq.crawlers.arxiv import ArxivCrawler
from atlasiq.crawlers.marketplace import MarketplaceCrawler
from atlasiq.crawlers.wikidata import WikidataCrawler
from atlasiq.sources.registry import SourceConfig

ADAPTERS = {"arxiv": ArxivCrawler, "wikidata-startups": WikidataCrawler, "atlassian-marketplace": MarketplaceCrawler}

DEFAULTS = {
    source.id: source
    for source in (
        SourceConfig(
            id="papers", name="arXiv", category="RESEARCH_PAPER", base_url="https://arxiv.org/",
            adapter_name="arxiv", enabled=True, rate_limit_config={"requests_per_minute": 20, "max_concurrency": 1},
        ),
        SourceConfig(
            id="startups", name="Wikidata AI startups", category="STARTUP", base_url="https://query.wikidata.org/",
            adapter_name="wikidata-startups", enabled=True,
            rate_limit_config={"requests_per_minute": 10, "max_concurrency": 1},
        ),
        SourceConfig(
            id="products", name="Atlassian Marketplace AI apps", category="PRODUCT",
            base_url="https://marketplace.atlassian.com/", adapter_name="atlassian-marketplace", enabled=True,
            rate_limit_config={"requests_per_minute": 60, "max_concurrency": 4},
        ),
    )
}
