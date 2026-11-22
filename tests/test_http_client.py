import asyncio
import socket
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import aiohttp
import pytest
from multidict import CIMultiDict

from atlasiq.config.settings import Settings
from atlasiq.crawlers.http_client import FetchError, HttpClient, PublicResolver, RequestPolicy, Response as Payload, _retry_after

POLICY = RequestPolicy(frozenset({"source.example"}), 1_000_000_000, 2)
JSON = frozenset({"application/json"})


@pytest.mark.parametrize("text", ["x", '{"value":NaN}', '{"value":1e10000}'])
def test_invalid_json_payload(text):
    with pytest.raises(FetchError, match="INVALID_JSON"):
        Payload(text, datetime.now(UTC)).json()

class Body:
    def __init__(self, chunks):
        self.chunks = chunks

    async def iter_chunked(self, size):
        for chunk in self.chunks:
            yield chunk


class Response:
    def __init__(self, session, status, *, headers=None, content_type="application/json", chunks=(b'{"ok":true}',)):
        self.session = session
        self.status = status
        self.headers = CIMultiDict(headers or {})
        self.content_type = content_type
        self.content_length = None
        self.content = Body(chunks)

    async def __aenter__(self):
        self.session.active += 1
        self.session.peak = max(self.session.peak, self.session.active)
        if self.session.release_at_peak is not None:
            if self.session.active >= self.session.release_at_peak:
                self.session.release.set()
            await self.session.release.wait()
        return self

    async def __aexit__(self, *exc):
        self.session.active -= 1


class Session:
    def __init__(self, replies):
        self.replies = list(replies)
        self.requested = []
        self.active = self.peak = 0
        self.release_at_peak = None
        self.release = asyncio.Event()

    def get(self, url, **kwargs):
        self.requested.append((str(url), kwargs))
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        status, options = reply
        return Response(self, status, **options)


def mock_client(replies, **values):
    settings = Settings(_env_file=None, database_url="postgresql://user@localhost/test",
                        crawler_max_retry_wait_seconds=1, **values)
    client = HttpClient(settings)
    client._session = Session(replies)
    return client, client._session


@pytest.mark.asyncio
async def test_success_and_http_500_retry():
    client, session = mock_client([(500, {}), (200, {})])
    response = await client.get("https://source.example/data", POLICY, JSON)
    assert response.json() == {"ok": True}
    assert len(session.requested) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("status, options, code", [
    (404, {}, "HTTP_404"),
    (403, {}, "HTTP_403"),
    (200, {"content_type": "text/html"}, "HTTP_UNEXPECTED_CONTENT"),
    (200, {"chunks": (b"too many bytes",)}, "HTTP_BODY_TOO_LARGE"),
    (302, {"headers": {"Location": "https://127.0.0.1/"}}, "UNSAFE_URL"),
])
async def test_permanent_errors_do_not_retry(status, options, code):
    client, session = mock_client([(status, options)], crawler_max_response_bytes=10)
    with pytest.raises(FetchError, match=code):
        await client.get("https://source.example/items", POLICY, JSON)
    assert len(session.requested) == 1


@pytest.mark.asyncio
async def test_timeout_retries_only_to_configured_limit():
    client, session = mock_client([TimeoutError(), TimeoutError()], crawler_http_attempts=2)
    with pytest.raises(FetchError, match="HTTP_TIMEOUT"):
        await client.get("https://source.example/items", POLICY, JSON)
    assert len(session.requested) == 2


@pytest.mark.asyncio
async def test_rate_limiting_honors_retry_after_and_shared_cooldown():
    client, session = mock_client([(429, {"headers": {"Retry-After": "30"}})])
    for _ in range(2):
        with pytest.raises(FetchError) as caught:
            await client.get("https://source.example/items", POLICY, JSON)
        assert caught.value.code == "HTTP_RATE_LIMIT" and caught.value.delay > 25
    assert len(session.requested) == 1
    assert _retry_after({"Retry-After": format_datetime(datetime.now(UTC) + timedelta(seconds=60))},
                        rate_limited=False) > 55


@pytest.mark.asyncio
async def test_rate_limiting_can_retry_short_retry_after():
    client, session = mock_client([(429, {"headers": {"Retry-After": "0"}}), (200, {})])
    assert (await client.get("https://source.example/items", POLICY, JSON)).json() == {"ok": True}
    assert len(session.requested) == 2


@pytest.mark.asyncio
async def test_github_rate_limited_403_is_transient():
    client, _ = mock_client([(403, {"headers": {"X-RateLimit-Remaining": "0"}})])
    policy = RequestPolicy(frozenset({"api.github.com"}), 30, 1)
    with pytest.raises(FetchError) as caught:
        await client.get("https://api.github.com/repos/example/project", policy, JSON)
    assert caught.value.transient and caught.value.delay >= 60


@pytest.mark.asyncio
async def test_domain_concurrency_and_cross_host_redirect_discards_authorization():
    client, session = mock_client([(200, {})] * 5)
    session.release_at_peak = 2
    await asyncio.wait_for(
        asyncio.gather(*(client.get(f"https://source.example/{index}", POLICY, JSON) for index in range(5))),
        timeout=5,
    )
    assert session.peak == 2
    client, session = mock_client([
        (302, {"headers": {"Location": "https://alternate.example/items"}}), (200, {}),
    ])
    policy = RequestPolicy(frozenset({"source.example", "alternate.example"}), 1_000_000_000, 2)
    await client.get("https://source.example/items", policy, JSON, headers={"Authorization": "example"})
    assert session.requested[1][1]["headers"] is None


@pytest.mark.asyncio
async def test_non_public_resolved_addresses_are_excluded(monkeypatch):
    async def resolve(self, host, port, family):
        return [{"host": address} for address in ("127.0.0.1", "169.254.169.254", "::ffff:10.0.0.1", "8.8.8.8")]

    monkeypatch.setattr(aiohttp.ThreadedResolver, "resolve", resolve)
    resolver = PublicResolver()
    try:
        assert await resolver.resolve("source.example", 443, socket.AF_UNSPEC) == [{"host": "8.8.8.8"}]
    finally:
        await resolver.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("url", [
    "http://source.example/", "https://user:password@source.example/", "https://source.example:8443/",
    "https://169.254.169.254/", "https://localhost/", "https://wrong.example/",
])
async def test_unsafe_url_rejected_before_request(url):
    client, session = mock_client([])
    with pytest.raises(FetchError, match="UNSAFE_URL"):
        await client.get(url, POLICY, JSON)
    assert not session.requested
