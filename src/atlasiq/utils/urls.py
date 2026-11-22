from urllib.parse import urlsplit

from pydantic import HttpUrl, TypeAdapter, ValidationError

http_url = TypeAdapter(HttpUrl)


def validate_source_url(value: str) -> str:
    if (
        not isinstance(value, str)
        or "\\" in value
        or any(ord(character) <= 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError("use an absolute HTTP(S) URL without spaces or control characters")
    address = urlsplit(value)
    if address.scheme not in {"http", "https"} or not address.netloc:
        raise ValueError("use an absolute HTTP(S) URL")
    try:
        parsed = http_url.validate_python(value)
    except ValidationError:
        raise ValueError("use an absolute HTTP(S) URL") from None
    if parsed.username is not None or parsed.password is not None or "@" in address.netloc:
        raise ValueError("URL credentials are prohibited")
    return value


def canonicalize_url(value: str) -> str:
    """Normalize URL identity while retaining path and query semantics."""
    validate_source_url(value)
    normalized = str(http_url.validate_python(value))
    return normalized.split("#", maxsplit=1)[0]
