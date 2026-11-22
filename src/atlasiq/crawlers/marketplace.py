import json
import re
from datetime import datetime
from hashlib import sha256
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from pydantic import Field, TypeAdapter

from atlasiq.crawlers.base import (
    JSON, AdapterOptions, BaseCrawler, InvalidRecord, Item, Page, Parsed, compact_json, record_fields,
)
from atlasiq.crawlers.http_client import FetchError
from atlasiq.crawlers.identity import identity_key
from atlasiq.db.models import SourceCategory
from atlasiq.schemas.entities import PricingModel, Product, Text

ORIGIN = "https://marketplace.atlassian.com"
PATH = "/rest/2/addons"
APP_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}")
VENDOR_LINK = re.compile(r"/rest/2/vendors/([0-9]+)")
AI = re.compile(
    r"\b(?:artificial intelligence|generative ai|ai[- ](?:powered|assistant|agent|chatbot|search|review|"
    r"summary|summaries|for|automation)|(?:with|using|by) ai|(?:smart|conversational) ai|"
    r"large language models?|machine learning|llms?|openai|chatgpt)\b", re.I,
)
PRICING = {"free": PricingModel.FREE, "atlassian": PricingModel.PAID, "vendor": PricingModel.PAID}
PRODUCT_NAME = TypeAdapter(Text)


class MarketplaceOptions(AdapterOptions):
    query: str = Field(default="AI", min_length=1, max_length=100)
    page_size: int = Field(default=25, ge=1, le=50)
    vendor_startup_keys: dict[str, str] = Field(default_factory=dict)


def _get(value: object, *keys: str) -> object:
    for key in keys:
        value = value.get(key) if isinstance(value, dict) else None
    return value


def _marketplace_url(href: object, prefix: str) -> str | None:
    if not isinstance(href, str):
        return None
    try:
        url = urljoin(ORIGIN, href)
        address = urlsplit(url)
    except ValueError:
        return None
    if address.scheme == "https" and address.netloc == "marketplace.atlassian.com" and address.path.startswith(prefix):
        return url
    return None


class MarketplaceCrawler(BaseCrawler):
    category = SourceCategory.PRODUCT
    hosts = frozenset({"marketplace.atlassian.com"})
    max_rpm = 60
    max_concurrency = 4

    def __init__(self, source, http):
        super().__init__(source, http)
        self.options = MarketplaceOptions.model_validate(source.adapter_config)

    async def discover(self, cursor, limit: int) -> Page:
        size = min(limit, self.options.page_size)
        if cursor is None:
            request = ORIGIN + PATH + "?" + urlencode({"text": self.options.query, "hosting": "cloud", "limit": size})
        else:
            address = urlsplit(str(cursor))
            query = dict(parse_qsl(address.query))
            query["limit"] = str(size)
            request = urlunsplit(address._replace(query=urlencode(query)))
        response = await self.http.get(request, self.policy, JSON)
        body = response.json()
        apps = _get(body, "_embedded", "addons")
        if not isinstance(apps, list):
            if isinstance(body, dict) and body.get("count") == 0:
                apps = []
            else:
                raise FetchError("MARKETPLACE_INVALID_LISTING")
        next_links = _get(body, "_links", "next") or []
        if isinstance(next_links, dict):
            next_links = [next_links]
        if not isinstance(next_links, list):
            raise FetchError("MARKETPLACE_INVALID_CURSOR")
        next_url = next((
            url for link in next_links
            if (url := _marketplace_url(_get(link, "href"), PATH))
            and urlsplit(url).path == PATH
        ), None)
        if next_links and (not apps or not next_url):
            raise FetchError("MARKETPLACE_INVALID_CURSOR")
        if next_url:
            try:
                current_offset = int(dict(parse_qsl(urlsplit(request).query)).get("offset", "0"))
                next_offset = int(dict(parse_qsl(urlsplit(next_url).query))["offset"])
                if next_offset <= current_offset:
                    raise ValueError
            except (KeyError, ValueError):
                raise FetchError("MARKETPLACE_INVALID_CURSOR") from None
        items = []
        for app in apps:
            raw = compact_json(app)
            key = _get(app, "key")
            identity = key if isinstance(key, str) and APP_KEY.fullmatch(key) else (
                "unidentified:" + sha256(raw.encode("utf-8")).hexdigest()
            )
            url = _marketplace_url(_get(app, "_links", "alternate", "href"), "/apps/") or ORIGIN + PATH
            items.append(Item(identity, url, raw))
        return Page(items, next_url, response.fetched_at, request)

    async def fetch(self, raw: str) -> str:
        app = json.loads(raw)
        key = _get(app, "key")
        if not isinstance(key, str) or not APP_KEY.fullmatch(key) or not self._describes_ai(app):
            return raw
        if isinstance(_get(app, "_embedded", "version", "paymentModel"), str):
            return raw
        version_url = f"{ORIGIN}{PATH}/{key}/versions/latest?hosting=cloud"
        try:
            response = await self.http.get(version_url, self.policy, JSON)
            version = response.json()
        except FetchError as error:
            if error.status != 404:
                raise
            version = None
        return compact_json({"listing": app, "cloud_version_url": version_url, "cloud_version": version})

    @staticmethod
    def _describes_ai(app: object) -> bool:
        text = " ".join(
            value for field in ("name", "tagLine", "summary")
            if isinstance(value := _get(app, field), str)
        )
        return AI.search(text) is not None

    def parse(self, raw: str, source_url: str, fetched_at: datetime) -> Parsed:
        evidence = json.loads(raw)
        app = evidence.get("listing", evidence) if isinstance(evidence, dict) else evidence
        key = _get(app, "key")
        if not isinstance(key, str) or not APP_KEY.fullmatch(key) or not urlsplit(source_url).path.startswith("/apps/"):
            raise InvalidRecord("MARKETPLACE_MISSING_ID_OR_URL")
        if not self._describes_ai(app):
            raise InvalidRecord("MARKETPLACE_NO_AI_EVIDENCE")
        payment = _get(evidence, "cloud_version", "paymentModel") if app is not evidence else (
            _get(app, "_embedded", "version", "paymentModel")
        )
        pricing = PRICING.get(payment) if isinstance(payment, str) else None
        if pricing is None:
            raise InvalidRecord("MARKETPLACE_MISSING_PRICING")
        product_name = _get(app, "name")
        vendor = _get(app, "_embedded", "vendor", "name")
        if not isinstance(product_name, str) or not product_name.strip() or not isinstance(vendor, str) or not vendor.strip():
            raise InvalidRecord("MARKETPLACE_MISSING_PRODUCT_OR_VENDOR")
        record = Product.model_validate({
            **record_fields(self.source, source_url), "collectedAt": fetched_at,
            "content": {"startupName": vendor, "pricingModel": pricing},
        })
        href = _get(app, "_embedded", "vendor", "_links", "self", "href")
        vendor_url = _marketplace_url(href, "/rest/2/vendors/")
        match = VENDOR_LINK.fullmatch(urlsplit(vendor_url).path) if vendor_url else None
        return Parsed(record, identity_key(self.category, self.source.id, key, source_url), {
            "product_name": PRODUCT_NAME.validate_python(product_name, strict=True),
            "startup_key": self.options.vendor_startup_keys.get(match[1]) if match else None,
        })
