import asyncio
import ipaddress
import json
import logging
import math
import random
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from time import monotonic

import aiohttp
from yarl import URL

from atlasiq.config.settings import Settings

logger = logging.getLogger(__name__)
REDIRECTS = {301, 302, 303, 307, 308}
TRANSIENT = {408, 425, 429, 500, 502, 503, 504}


class FetchError(Exception):
    def __init__(self, code: str, *, transient: bool = False, status: int | None = None, delay: float = 0):
        super().__init__(code)
        self.code = code
        self.transient = transient
        self.status = status
        self.delay = delay


class PublicResolver(aiohttp.ThreadedResolver):
    async def resolve(self, host: str, port: int = 0, family: int = socket.AF_UNSPEC):
        addresses = await super().resolve(host, port, family)
        public = []
        for address in addresses:
            ip = ipaddress.ip_address(address["host"])
            ip = getattr(ip, "ipv4_mapped", None) or ip
            if ip.is_global and not ip.is_multicast and not ip.is_reserved:
                public.append(address)
        if not public:
            raise OSError("No permitted public address")
        return public


@dataclass(frozen=True)
class RequestPolicy:
    hosts: frozenset[str]
    requests_per_minute: int
    max_concurrency: int


@dataclass(frozen=True)
class Response:
    text: str
    fetched_at: datetime

    def json(self) -> object:
        def reject_constant(value: str):
            raise ValueError("invalid JSON constant")

        def finite_float(value: str) -> float:
            number = float(value)
            if not math.isfinite(number):
                raise ValueError("invalid JSON number")
            return number

        try:
            return json.loads(self.text, parse_constant=reject_constant, parse_float=finite_float)
        except (ValueError, RecursionError):
            raise FetchError("INVALID_JSON") from None


class _RateGate:
    def __init__(self, concurrency: int):
        self.slots = asyncio.Semaphore(concurrency)
        self.lock = asyncio.Lock()
        self.last_request = float("-inf")
        self.next_request = 0.0
        self.cooldown = 0.0

    async def wait(self, requests_per_minute: int, max_wait: int) -> None:
        # Serialize admission and recheck cooldown before admitting a waiting request.
        interval = 60 / requests_per_minute
        async with self.lock:
            while True:
                now = monotonic()
                cooldown = self.cooldown - now
                if cooldown > max_wait:
                    raise FetchError("HTTP_RATE_LIMIT", transient=True, delay=cooldown)
                delay = max(cooldown, self.next_request - now, self.last_request + interval - now)
                if delay <= 0:
                    self.last_request = now
                    self.next_request = now + interval
                    return
                await asyncio.sleep(delay)


def _safe_url(value: str | URL, policy: RequestPolicy) -> URL:
    try:
        url = URL(value)
        host = url.raw_host
        port = url.port
    except (ValueError, TypeError):
        raise FetchError("UNSAFE_URL") from None
    if (
        url.scheme != "https" or host not in policy.hosts or port != 443
        or url.raw_user is not None or url.raw_password is not None
        or len(str(url).encode("utf-8")) > 8192 or "\\" in str(value)
    ):
        raise FetchError("UNSAFE_URL")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return url.with_fragment(None)
    raise FetchError("UNSAFE_URL")


def _retry_after(headers: Mapping[str, str], *, rate_limited: bool) -> float:
    value = headers.get("Retry-After")
    now = datetime.now(UTC)
    if value:
        try:
            if value.isascii() and value.isdigit():
                return float(int(value[:12]))
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=UTC)
            return max(0, (date - now).total_seconds())
        except (ValueError, TypeError, OverflowError):
            pass
    if rate_limited:
        try:
            if headers.get("X-RateLimit-Remaining") == "0":
                return max(1, int(headers["X-RateLimit-Reset"]) - now.timestamp())
        except (ValueError, KeyError):
            pass
        return 60
    return 0


class HttpClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._slots = asyncio.Semaphore(settings.crawler_max_concurrency)
        self._gates: dict[str, _RateGate] = {}
        self._policy_slots: dict[RequestPolicy, asyncio.Semaphore] = {}

    async def __aenter__(self):
        self._resolver = PublicResolver()
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                resolver=self._resolver, limit=self.settings.crawler_max_concurrency, limit_per_host=0,
            ),
            timeout=aiohttp.ClientTimeout(total=self.settings.crawler_request_timeout_seconds),
            headers={"User-Agent": self.settings.crawler_user_agent},
            cookie_jar=aiohttp.DummyCookieJar(),
            trust_env=False,
        )
        return self

    async def __aexit__(self, *exc):
        await self._session.close()
        await self._resolver.close()

    async def get(
        self, url: str, policy: RequestPolicy, media_types: frozenset[str],
        *, headers: Mapping[str, str] | None = None,
    ) -> Response:
        initial = _safe_url(url, policy)
        for attempt in range(self.settings.crawler_http_attempts):
            try:
                return await self._fetch(initial, policy, media_types, headers)
            except FetchError as error:
                retry = (
                    error.transient and attempt + 1 < self.settings.crawler_http_attempts
                    and error.delay <= self.settings.crawler_max_retry_wait_seconds
                )
                logger.warning("HTTP request failed", extra={
                    "operation": "http.retry" if retry else "http.failure",
                    "safe_message": error.code,
                    "http_status": error.status,
                })
                if not retry:
                    raise
                backoff = min(self.settings.crawler_max_retry_wait_seconds, 2 ** attempt + random.random())
                await asyncio.sleep(max(error.delay, backoff))
        raise AssertionError("positive HTTP attempts required")

    async def _fetch(
        self, initial: URL, policy: RequestPolicy, media_types: frozenset[str],
        headers: Mapping[str, str] | None,
    ) -> Response:
        url = initial
        concurrency = min(policy.max_concurrency, self.settings.crawler_max_concurrency)
        source_slots = self._policy_slots.setdefault(policy, asyncio.Semaphore(concurrency))
        for _ in range(6):
            gate = self._gates.setdefault(url.raw_host, _RateGate(concurrency))
            async with source_slots, gate.slots, self._slots:
                await gate.wait(policy.requests_per_minute, self.settings.crawler_max_retry_wait_seconds)
                try:
                    async with self._session.get(
                        url, allow_redirects=False, proxy=None,
                        headers=headers if url.origin() == initial.origin() else None,
                    ) as response:
                        if response.status in REDIRECTS:
                            location = response.headers.get("Location")
                            if not location:
                                raise FetchError("INVALID_REDIRECT")
                            try:
                                destination = url.join(URL(location))
                            except (TypeError, ValueError):
                                raise FetchError("INVALID_REDIRECT") from None
                            url = _safe_url(destination, policy)
                            continue
                        if not 200 <= response.status < 300:
                            self._raise_status(response, gate, url.raw_host)
                        return await self._read(response, media_types)
                except TimeoutError:
                    raise FetchError("HTTP_TIMEOUT", transient=True) from None
                except aiohttp.ClientSSLError:
                    raise FetchError("HTTP_TLS_ERROR") from None
                except aiohttp.ClientConnectionError:
                    raise FetchError("HTTP_CONNECTION_ERROR", transient=True) from None
                except aiohttp.ClientPayloadError:
                    raise FetchError("HTTP_INVALID_BODY", transient=True) from None
                except aiohttp.ClientError:
                    raise FetchError("HTTP_PROTOCOL_ERROR") from None
        raise FetchError("TOO_MANY_REDIRECTS")

    def _raise_status(self, response: aiohttp.ClientResponse, gate: _RateGate, host: str) -> None:
        status = response.status
        limited = status == 429 or (
            status == 403 and host == "api.github.com" and (
                response.headers.get("X-RateLimit-Remaining") == "0" or "Retry-After" in response.headers
            )
        )
        delay = _retry_after(response.headers, rate_limited=limited)
        if delay and (limited or status in TRANSIENT):
            gate.cooldown = max(gate.cooldown, monotonic() + delay)
        raise FetchError(
            "HTTP_RATE_LIMIT" if limited else f"HTTP_{status}",
            transient=limited or status in TRANSIENT, status=status, delay=delay,
        )

    async def _read(self, response: aiohttp.ClientResponse, media_types: frozenset[str]) -> Response:
        if response.content_type not in media_types:
            raise FetchError("HTTP_UNEXPECTED_CONTENT")
        body = bytearray()
        maximum = self.settings.crawler_max_response_bytes
        if response.content_length and response.content_length > maximum:
            raise FetchError("HTTP_BODY_TOO_LARGE")
        async for chunk in response.content.iter_chunked(64 * 1024):
            body.extend(chunk)
            if len(body) > maximum:
                raise FetchError("HTTP_BODY_TOO_LARGE")
        try:
            return Response(body.decode("utf-8-sig"), datetime.now(UTC))
        except UnicodeError:
            raise FetchError("HTTP_INVALID_ENCODING") from None
