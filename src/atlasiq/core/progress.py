from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True)
class ProgressEvent:
    stage: str
    state: str
    detail: str
    source: str | None = None
    counts: dict[str, int] = field(default_factory=dict)
    at: datetime = field(default_factory=lambda: datetime.now(UTC))


Observer = Callable[[ProgressEvent], None]


def emit(
    observer: Observer | None, stage: str, state: str, detail: str,
    source: str | None = None, **counts: int,
) -> None:
    if observer is not None:
        observer(ProgressEvent(stage, state, detail, source, counts))
