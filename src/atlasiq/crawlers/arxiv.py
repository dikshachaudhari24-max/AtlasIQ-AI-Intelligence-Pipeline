import re
from datetime import datetime
from hashlib import sha256
from urllib.parse import urlencode, urlsplit
from xml.etree.ElementTree import Element, ParseError, tostring

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException
from pydantic import Field

from atlasiq.crawlers.base import AdapterOptions, BaseCrawler, InvalidRecord, Item, Page, Parsed, record_fields
from atlasiq.crawlers.http_client import FetchError
from atlasiq.crawlers.identity import arxiv_identity
from atlasiq.db.models import SourceCategory
from atlasiq.schemas.entities import ResearchPaper

ATOM = "{http://www.w3.org/2005/Atom}"
SEARCH = "{http://a9.com/-/spec/opensearch/1.1/}"
ARXIV = "{http://arxiv.org/schemas/atom}"
XML = frozenset({"application/atom+xml", "application/xml", "text/xml"})
API = "https://export.arxiv.org/api/query"
DOI = re.compile(r"10\.[0-9]{4,9}/[^\s]+", re.I)


class ArxivOptions(AdapterOptions):
    query: str = Field(default="cat:cs.AI", min_length=1, max_length=1500)
    id_list: str | None = Field(default=None, min_length=1, max_length=1500)
    page_size: int = Field(default=50, ge=1, le=100)
    sort_by: str | None = Field(default=None, pattern="^(relevance|lastUpdatedDate|submittedDate)$")


def paper_id(entry: Element) -> str:
    source = urlsplit(entry.findtext(ATOM + "id") or "")
    if source.hostname not in {"arxiv.org", "www.arxiv.org", "export.arxiv.org"} or not source.path.startswith("/abs/"):
        raise ValueError("invalid Atom paper URL")
    return arxiv_identity(source.path.removeprefix("/abs/"))


def _text(element: Element, name: str) -> str:
    return " ".join((element.findtext(ATOM + name) or "").split())


class ArxivCrawler(BaseCrawler):
    category = SourceCategory.RESEARCH_PAPER
    hosts = frozenset({"export.arxiv.org"})
    max_rpm = 20
    max_concurrency = 1

    def __init__(self, source, http):
        super().__init__(source, http)
        self.options = ArxivOptions.model_validate(source.adapter_config)

    async def discover(self, cursor, limit: int) -> Page:
        start = int(cursor or 0)
        params = (
            {"id_list": self.options.id_list} if self.options.id_list
            else {"search_query": self.options.query}
        )
        params.update(start=start, max_results=min(self.options.page_size, limit))
        if self.options.sort_by:
            params["sortBy"] = self.options.sort_by
            params["sortOrder"] = "descending"
        request = API + "?" + urlencode(params)
        response = await self.http.get(request, self.policy, XML)
        try:
            feed = ElementTree.fromstring(response.text)
            if feed.tag != ATOM + "feed":
                raise ValueError
            total = int(feed.findtext(SEARCH + "totalResults") or "")
            reported_start = feed.findtext(SEARCH + "startIndex")
            if total < 0 or (reported_start is not None and int(reported_start) != start):
                raise ValueError
        except (ParseError, DefusedXmlException, ValueError):
            raise FetchError("ARXIV_INVALID_FEED") from None

        entries = feed.findall(ATOM + "entry")
        if not entries and start < total:
            raise FetchError("ARXIV_EMPTY_PAGE", transient=True)
        items = []
        for entry in entries:
            raw = tostring(entry, encoding="unicode")
            entry_url = entry.findtext(ATOM + "id") or ""
            if "/api/errors" in entry_url:
                raise FetchError("ARXIV_REJECTED_QUERY")
            try:
                identity = paper_id(entry)
                url = f"https://arxiv.org/abs/{identity}"
            except ValueError:
                identity = "unidentified:" + sha256(raw.encode("utf-8")).hexdigest()
                url = request
            items.append(Item(identity, url, raw))
        next_start = start + len(entries)
        return Page(items, next_start if entries and next_start < total else None, response.fetched_at, request)

    def parse(self, raw: str, source_url: str, fetched_at: datetime) -> Parsed:
        try:
            entry = ElementTree.fromstring(raw)
            identity = paper_id(entry)
        except (ParseError, DefusedXmlException, ValueError):
            raise InvalidRecord("ARXIV_INVALID_ENTRY") from None
        authors = list(dict.fromkeys(
            name for author in entry.findall(ATOM + "author") if (name := _text(author, "name"))
        ))
        if not authors:
            raise InvalidRecord("ARXIV_MISSING_AUTHORS")
        abstract = _text(entry, "summary") or None
        links = [link.attrib["href"] for link in entry.findall(ATOM + "link") if "href" in link.attrib]
        doi = (entry.findtext(ARXIV + "doi") or "").strip()
        paper = ResearchPaper.model_validate({
            **record_fields(self.source, source_url),
            "content": {
                "title": _text(entry, "title"), "authors": authors, "paper_url": source_url,
                "published_date": entry.findtext(ATOM + "published"),
            },
        })
        return Parsed(paper, f"arxiv:{identity}", {
            "arxiv_id": identity, "doi": doi.lower() if DOI.fullmatch(doi) else None,
            "abstract": abstract, "links": links, "collected_at": fetched_at,
        })
