import re
import unicodedata
from typing import Annotated, Literal

from pydantic import Field, StringConstraints, ValidationError

from atlasiq.db.models import SourceCategory
from atlasiq.schemas.entities import Contract

FactField = Literal[
    "entity_name", "description", "category", "technology", "geography",
    "organization", "research_topic", "funding", "launch", "market_signal",
]
ShortText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=400)]
Quote = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=800)]

TECHNOLOGIES = {
    "ai": "artificial intelligence",
    "artificial intelligence": "artificial intelligence",
    "ml": "machine learning",
    "machine learning": "machine learning",
    "nlp": "natural language processing",
    "natural language processing": "natural language processing",
    "llm": "large language models",
    "llms": "large language models",
    "large language model": "large language models",
    "large language models": "large language models",
    "generative ai": "generative AI",
    "genai": "generative AI",
    "computer vision": "computer vision",
    "deep learning": "deep learning",
    "pytorch": "PyTorch",
    "tensorflow": "TensorFlow",
    "python": "Python",
    "rag": "retrieval-augmented generation",
    "retrieval-augmented generation": "retrieval-augmented generation",
}
CATEGORIES = {
    "artificial intelligence", "machine learning", "natural language processing", "computer vision",
    "robotics", "healthcare", "finance", "education", "developer tools", "data infrastructure",
    "cybersecurity", "automation", "research", "enterprise software",
}


class Fact(Contract):
    field: FactField
    value: ShortText
    quote: Quote
    confidence: Annotated[float, Field(ge=0, le=1, allow_inf_nan=False)] | None = None


class Extraction(Contract):
    schema_version: Literal["1.0"]
    record_type: SourceCategory = Field(strict=False)
    facts: Annotated[list[Fact], Field(max_length=24)]


class InvalidOutput(Exception):
    pass


def tidy(value: str) -> str:
    return " ".join(unicodedata.normalize("NFC", value).split())


def _occurs(needle: str, haystack: str, flags: int = 0) -> bool:
    left = r"(?<!\w)" if re.match(r"\w", needle[0]) else ""
    right = r"(?!\w)" if re.match(r"\w", needle[-1]) else ""
    return re.search(left + re.escape(needle) + right, haystack, flags) is not None


def checked_facts(output: str, evidence: str, kind: str) -> list[dict]:
    try:
        result = Extraction.model_validate_json(output)
    except ValidationError:
        raise InvalidOutput("INVALID_STRUCTURED_OUTPUT") from None
    if result.record_type != kind:
        raise InvalidOutput("RECORD_TYPE_MISMATCH")
    body = tidy(evidence)
    records = []
    for fact in result.facts:
        quote, value = tidy(fact.quote), tidy(fact.value)
        # Quotes must be literal after formatting whitespace is normalized. The claimed value
        # must occur verbatim, with word boundaries to prevent matches such as AI in paid.
        if not _occurs(quote, body) or not _occurs(value, quote, re.I):
            raise InvalidOutput("UNSUPPORTED_EVIDENCE")
        folded = value.casefold()
        normalized = value
        if fact.field == "technology":
            normalized = TECHNOLOGIES.get(folded, value)
        elif fact.field == "category":
            normalized = folded if folded in CATEGORIES else None
        records.append({
            "field": fact.field, "value": value, "normalized": normalized, "quote": quote,
            "model_confidence": fact.confidence, "evidence_status": "source_quote_matched",
        })
    return records


def merge_facts(chunks: list[list[dict]]) -> list[dict]:
    facts = {}
    for chunk in chunks:
        for fact in chunk:
            key = (fact["field"], (fact["normalized"] or fact["value"]).casefold())
            facts.setdefault(key, fact)
    return list(facts.values())
