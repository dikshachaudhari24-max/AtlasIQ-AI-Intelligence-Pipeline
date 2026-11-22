import asyncio
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlsplit

from atlasiq.config.settings import Settings
from atlasiq.crawlers.base import JSON
from atlasiq.crawlers.http_client import FetchError, HttpClient, RequestPolicy

OWNER = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?")
NAME = re.compile(r"[A-Za-z0-9_.-]{1,100}")
ABSTRACT_URL = re.compile(r"https?://(?:www\.)?github\.com/[^\s<>\{\}\[\]\\]+", re.I)


@dataclass(frozen=True)
class Repository:
    owner: str
    name: str

    @property
    def identity(self) -> str:
        return f"{self.owner}/{self.name}".lower()

    @property
    def url(self) -> str:
        return f"https://github.com/{self.owner}/{self.name}"


@dataclass(frozen=True)
class Association:
    repository: Repository
    evidence: dict


@dataclass(frozen=True)
class Metric:
    repository: Repository
    evidence: dict
    stars: int | None
    observed_at: datetime


def parse_repository_url(value: str) -> Repository | None:
    try:
        url = urlsplit(value)
        if (
            url.scheme not in {"http", "https"} or url.netloc.lower() not in {"github.com", "www.github.com"}
            or url.query or url.fragment or not url.path.startswith("/")
        ):
            return None
        owner, name = url.path.removeprefix("/").removesuffix("/").split("/")
    except ValueError:
        return None
    name = name.removesuffix(".git")
    if not OWNER.fullmatch(owner) or not NAME.fullmatch(name) or name in {".", ".."}:
        return None
    return Repository(owner, name)


def associations(links: list[str], abstract: str | None, paper_url: str) -> list[Association]:
    repositories = {}
    candidates = [
        ("arxiv_metadata_link", links),
        ("arxiv_abstract_link", [match.rstrip(".,;:!?)") for match in ABSTRACT_URL.findall(abstract or "")]),
    ]
    for kind, urls in candidates:
        for url in urls:
            repository = parse_repository_url(url)
            if repository and repository.identity not in repositories:
                repositories[repository.identity] = Association(repository, {
                    "kind": kind, "paper_url": paper_url, "referenced_url": url,
                })
    return list(repositories.values())[:10]


class GitHubClient:
    def __init__(self, http: HttpClient, settings: Settings):
        self.http = http
        token = settings.github_token.get_secret_value() if settings.github_token else None
        self.policy = RequestPolicy(frozenset({"api.github.com"}), 60 if token else 30, 1)
        self.headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            self.headers["Authorization"] = f"Bearer {token}"

    async def _metric(self, association: Association) -> Metric | None:
        repository = association.repository
        try:
            response = await self.http.get(
                f"https://api.github.com/repos/{repository.owner}/{repository.name}",
                self.policy, JSON, headers=self.headers,
            )
            body = response.json()
        except FetchError as error:
            if error.status in {404, 410}:
                return None
            return Metric(repository, {**association.evidence, "metric_error": error.code}, None, datetime.now(UTC))
        if not isinstance(body, dict) or body.get("private") is not False:
            return None
        canonical = parse_repository_url(body.get("html_url", "")) if isinstance(body.get("html_url"), str) else None
        owner = body.get("owner")
        if (
            canonical is None or not isinstance(owner, dict)
            or not isinstance(owner.get("login"), str) or not isinstance(body.get("name"), str)
            or canonical.identity != f"{owner['login']}/{body['name']}".lower()
            or not isinstance(body.get("full_name"), str)
            or canonical.identity != body["full_name"].lower()
        ):
            return None
        value = body.get("stargazers_count")
        stars = value if type(value) is int and 0 <= value < 2**63 else None
        evidence = association.evidence if stars is not None else {**association.evidence, "metric_error": "INVALID_STARS"}
        return Metric(canonical, evidence, stars, response.fetched_at)

    async def collect(self, references: list[Association]) -> list[Metric]:
        # Requests are bounded by the per-domain policy even when multiple papers are active.
        results = await asyncio.gather(*(self._metric(reference) for reference in references))
        metrics = {}
        for metric in results:
            if metric is not None:
                existing = metrics.get(metric.repository.identity)
                if existing is None or existing.stars is None:
                    metrics[metric.repository.identity] = metric
        return list(metrics.values())
