import json
import re

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException
from xml.etree.ElementTree import ParseError

from atlasiq.db.models import SourceCategory

ATOM = "{http://www.w3.org/2005/Atom}"
SECTIONS = re.compile(r"(?<=[.!?])\s+|\n\s*\n+")


def _raw_text(raw: str | None, kind: str) -> list[str]:
    if not raw:
        return []
    if kind == SourceCategory.RESEARCH_PAPER:
        try:
            entry = ElementTree.fromstring(raw)
        except (DefusedXmlException, ParseError):
            return []
        return [value for field in ("title", "summary", "comment") if (value := entry.findtext(ATOM + field))]
    if kind in {SourceCategory.PRODUCT, SourceCategory.STARTUP}:
        try:
            item = json.loads(raw)
        except (ValueError, RecursionError):
            return []
        if not isinstance(item, dict):
            return []
        return [item[field] for field in ("description", "name", "summary", "tagLine")
                if isinstance(item.get(field), str)]
    return []


def source_text(candidate: dict) -> str:
    sections = [
        candidate["title"], candidate.get("evidence_text"),
        *_raw_text(candidate.get("raw_content"), candidate["kind"]),
    ]
    return "\n\n".join(dict.fromkeys(value.strip() for value in sections if value and value.strip()))


def _byte_pieces(text: str, budget: int):
    encoded = text.encode("utf-8")
    start = 0
    while len(encoded) - start > budget:
        # Starting the following slice at a code-point boundary preserves all Unicode text.
        end = start + budget
        while encoded[end] & 0xC0 == 0x80:
            end -= 1
        yield encoded[start:end].decode("utf-8")
        start = end
    if start < len(encoded):
        yield encoded[start:].decode("utf-8")


def semantic_chunks(text: str, budget: int, maximum: int) -> list[str]:
    if budget < 4:
        raise ValueError("UTF-8 budget must be at least four bytes")
    chunks = []
    current = ""
    for unit in SECTIONS.split(text):
        for piece in _byte_pieces(unit, budget):
            combined = f"{current}\n{piece}" if current else piece
            if len(combined.encode("utf-8")) > budget:
                chunks.append(current)
                if len(chunks) >= maximum:
                    raise ValueError("CHUNK_BUDGET_EXCEEDED")
                current = piece
            else:
                current = combined
    if current:
        chunks.append(current)
    return chunks


def smaller_chunks(chunk: str) -> list[str]:
    size = len(chunk.encode("utf-8"))
    if size <= 128:
        return []
    return semantic_chunks(chunk, max(64, size // 2), 4)
