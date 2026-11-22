import re
from hashlib import sha256

from atlasiq.utils.urls import canonicalize_url

ARXIV_ID = re.compile(r"(?:[0-9]{4}\.[0-9]{4,5}|[a-z][a-z0-9.-]*/[0-9]{7})(?:v[1-9][0-9]*)?", re.I)
VERSION = re.compile(r"v[1-9][0-9]*$", re.I)


def arxiv_identity(value: str) -> str:
    candidate = value.strip()
    if not ARXIV_ID.fullmatch(candidate):
        raise ValueError("invalid arXiv identifier")
    return VERSION.sub("", candidate).lower()


def identity_key(category: str, source_id: str, upstream_id: str | None, source_url: str) -> str:
    identity = "id:" + upstream_id if upstream_id else "url:" + canonicalize_url(source_url)
    signature = sha256(identity.encode("utf-8")).hexdigest()
    return f"{category.lower()}:{source_id}:{signature}"
