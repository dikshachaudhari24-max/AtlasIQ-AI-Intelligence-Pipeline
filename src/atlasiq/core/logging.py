import json
import logging
import re
import traceback
from datetime import UTC, datetime
from pathlib import Path


class JsonFormatter(logging.Formatter):
    def __init__(self, *, diagnostics: bool = False):
        super().__init__()
        self.diagnostics = diagnostics

    def format(self, record: logging.LogRecord) -> str:
        event = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "module": record.name,
            "operation": getattr(record, "operation", "application"),
            "message": getattr(record, "safe_message", "Application event"),
        }
        for name in ("source_id", "crawl_run_id", "task_id", "worker_id"):
            identifier = getattr(record, name, None)
            if identifier is not None and re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", str(identifier)):
                event[name] = str(identifier)
        duration = getattr(record, "duration_ms", None)
        if isinstance(duration, (int, float)):
            event["duration_ms"] = duration
        for name in ("http_status", "discovered", "validated", "inserted", "updated", "duplicates", "rejected", "failed_tasks"):
            value = getattr(record, name, None)
            if type(value) is int:
                event[name] = value
        if record.exc_info and record.exc_info[0] is not None:
            event["error_type"] = record.exc_info[0].__name__
            event["error_message"] = getattr(record, "safe_message", "Operation failed")
            if self.diagnostics:
                event["stack"] = [
                    {"file": Path(frame.filename).name, "function": frame.name, "line": frame.lineno}
                    for frame in traceback.extract_tb(record.exc_info[2])
                ]
        return json.dumps(event, ensure_ascii=True)


def configure_logging(level: str, *, diagnostics: bool) -> None:
    logger = logging.getLogger("atlasiq")
    logger.setLevel(level)
    logger.propagate = False
    for previous in logger.handlers[:]:
        logger.removeHandler(previous)
        previous.close()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(diagnostics=diagnostics))
    logger.addHandler(handler)
