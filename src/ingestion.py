"""Phase 1 - extraction and contract validation (with Dead Letter Queue).

Reads the raw files, validates every record against its Pydantic contract and
splits the stream into *valid* rows (destined for Bronze) and *invalid* rows
(written to the DLQ). A validation failure never stops the pipeline.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Type

from pydantic import BaseModel, ValidationError

from src.contracts import EntityContract, TransactionContract

log = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    valid: list[tuple[int, BaseModel]] = field(default_factory=list)  # (source_row, model)
    invalid: list[dict[str, Any]] = field(default_factory=list)       # DLQ entries


def format_errors(exc: ValidationError) -> str:
    """Flatten a ValidationError, e.g. ``entity_id: Field required; currency: ...``."""
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"]) or "<record>"
        parts.append(f"{loc}: {err['msg']}")
    return "; ".join(parts)


def validate_records(records: Iterable[Any], contract: Type[BaseModel]) -> ValidationResult:
    """Validate each record; collect failures instead of raising."""
    result = ValidationResult()
    for row_number, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            result.invalid.append({"record": record, "source_row": row_number,
                                   "error": f"expected an object, got {type(record).__name__}"})
            continue
        try:
            result.valid.append((row_number, contract.model_validate(record)))
        except ValidationError as exc:
            result.invalid.append({"record": record, "error": format_errors(exc),
                                   "source_row": row_number})
    return result


def read_entities_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def read_transactions_json(path: Path) -> list[Any]:
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON array of transaction objects")
    return data


def write_dlq(entries: list[dict[str, Any]], path: Path) -> None:
    """Write DLQ entries (original payload + error) as a JSON array."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(entries, fh, indent=2, default=str)


def to_naive_utc(dt: datetime) -> datetime:
    """Contracts accept tz-aware timestamps; normalise to naive UTC for Bronze."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


@dataclass(frozen=True)
class SourceSpec:
    name: str
    pattern: str
    reader: Callable[[Path], list]
    contract: Type[BaseModel]


SOURCES: dict[str, SourceSpec] = {
    "entities": SourceSpec("entities", "entities*.csv", read_entities_csv, EntityContract),
    "transactions": SourceSpec("transactions", "transactions*.json", read_transactions_json,
                               TransactionContract),
}


@dataclass(frozen=True)
class Batch:
    """One immutable landed file = one load batch (identified by its content hash)."""
    source: str
    path: Path
    sha256: str

    @property
    def batch_id(self) -> str:
        return self.sha256[:16]

    @property
    def dlq_filename(self) -> str:
        # entities.csv -> invalid_entities.json, transactions_batch2.json -> invalid_transactions_batch2.json
        return f"invalid_{self.path.stem}.json"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover_batches(raw_dir: Path) -> list[Batch]:
    """Find landed files (``entities*.csv``, ``transactions*.json``) in sorted order.

    Files with byte-identical content are treated as one batch (a re-landed copy is ignored).
    Sorted order = arrival order, so name batches with a sortable suffix (_batch2, _2026-10-28).
    """
    seen: set[str] = set()
    batches: list[Batch] = []
    for spec in SOURCES.values():
        for path in sorted(raw_dir.glob(spec.pattern)):
            digest = file_sha256(path)
            if digest in seen:
                log.warning("ignoring file with duplicate content", extra={"file": path.name})
                continue
            seen.add(digest)
            batches.append(Batch(spec.name, path, digest))
    return batches


def process_batch(batch: Batch, dlq_dir: Path) -> tuple[ValidationResult, int]:
    """Read + validate one file; write its DLQ file. Returns (result, rows_read)."""
    spec = SOURCES[batch.source]
    records = spec.reader(batch.path)
    result = validate_records(records, spec.contract)
    write_dlq(result.invalid, dlq_dir / batch.dlq_filename)
    log.info("batch validated", extra={
        "source": batch.source, "file": batch.path.name, "batch_id": batch.batch_id,
        "rows_read": len(records), "rows_valid": len(result.valid),
        "rows_invalid": len(result.invalid)})
    return result, len(records)


def ingest(entities_csv: Path, transactions_json: Path,
           invalid_entities_json: Path, invalid_transactions_json: Path
           ) -> tuple[ValidationResult, ValidationResult]:
    """Validate the two canonical files and write their DLQ files (single-batch helper)."""
    entities = validate_records(read_entities_csv(entities_csv), EntityContract)
    transactions = validate_records(read_transactions_json(transactions_json), TransactionContract)
    write_dlq(entities.invalid, invalid_entities_json)
    write_dlq(transactions.invalid, invalid_transactions_json)
    log.info("entities: %d valid, %d -> DLQ", len(entities.valid), len(entities.invalid))
    log.info("transactions: %d valid, %d -> DLQ", len(transactions.valid), len(transactions.invalid))
    return entities, transactions
