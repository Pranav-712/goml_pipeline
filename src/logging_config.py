"""Structured logging: JSON (machine-readable) or text, every record tagged with a run_id.

    log.info("batch loaded", extra={"stage": "load_bronze", "rows": 940})

JSON output (``--log-format json``) puts every ``extra`` field at the top level of the
record, so logs can be shipped straight to ELK / CloudWatch / Datadog and filtered by
``run_id``, ``stage`` or ``duration_ms``.
"""
from __future__ import annotations

import contextvars
import json
import logging
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

_run_id: contextvars.ContextVar[str] = contextvars.ContextVar("run_id", default="-")
_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime"}
_handler: logging.Handler | None = None


def new_run_id() -> str:
    run_id = uuid.uuid4().hex[:12]
    _run_id.set(run_id)
    return run_id


def get_run_id() -> str:
    return _run_id.get()


class RunIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = _run_id.get()
        return True


def _extras(record: logging.LogRecord) -> dict:
    return {k: v for k, v in vars(record).items() if k not in _RESERVED}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            **_extras(record),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        extras = " ".join(f"{k}={v}" for k, v in _extras(record).items() if k != "run_id")
        line = (f"{self.formatTime(record, '%H:%M:%S')} {record.levelname:<7} "
                f"[{_run_id.get()}] {record.name}: {record.getMessage()}")
        if extras:
            line += f"  ({extras})"
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


def configure_logging(fmt: str = "text", level: str = "INFO") -> None:
    """Install one stderr handler on the root logger (idempotent)."""
    global _handler
    root = logging.getLogger()
    if _handler is not None:
        root.removeHandler(_handler)
    _handler = logging.StreamHandler(sys.stderr)
    _handler.addFilter(RunIdFilter())
    _handler.setFormatter(JsonFormatter() if fmt == "json" else TextFormatter())
    root.addHandler(_handler)
    root.setLevel(level.upper())


@contextmanager
def stage(logger: logging.Logger, name: str, **fields):
    """Log start / finish (with duration_ms) of a pipeline stage; log + re-raise on error."""
    start = time.perf_counter()
    logger.info(f"{name} started", extra={"stage": name, "event": "start", **fields})
    try:
        yield
    except Exception:
        logger.exception(f"{name} failed", extra={
            "stage": name, "event": "error",
            "duration_ms": round((time.perf_counter() - start) * 1000), **fields})
        raise
    logger.info(f"{name} finished", extra={
        "stage": name, "event": "finish",
        "duration_ms": round((time.perf_counter() - start) * 1000), **fields})
