"""Central configuration: all paths and tunables live here.

Every path can be overridden with an environment variable or (for the
pipeline / generator) a CLI flag, so nothing is hard-coded in the logic.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"
QUERIES_DIR = SQL_DIR / "queries"

ALLOWED_CURRENCIES = frozenset({"USD", "EUR", "GBP"})

# Circuit breaker: if more than this fraction of a source's rows land in the DLQ the
# data-quality report raises an ERROR (usually an upstream schema change, not "a few bad rows").
DEFAULT_MAX_INVALID_RATIO = 0.20
# Warn if more than this fraction of valid transactions reference an unknown entity.
MAX_ORPHAN_RATIO = 0.10


@dataclass(frozen=True)
class Paths:
    raw_dir: Path
    dlq_dir: Path
    output_dir: Path
    db_path: Path

    @property
    def entities_csv(self) -> Path:
        return self.raw_dir / "entities.csv"

    @property
    def transactions_json(self) -> Path:
        return self.raw_dir / "transactions.json"

    @property
    def invalid_entities_json(self) -> Path:
        return self.dlq_dir / "invalid_entities.json"

    @property
    def invalid_transactions_json(self) -> Path:
        return self.dlq_dir / "invalid_transactions.json"


def default_paths() -> Paths:
    data = Path(os.getenv("PIPELINE_DATA_DIR", PROJECT_ROOT / "data"))
    return Paths(
        raw_dir=Path(os.getenv("PIPELINE_RAW_DIR", data / "raw")),
        dlq_dir=Path(os.getenv("PIPELINE_DLQ_DIR", data / "dlq")),
        output_dir=Path(os.getenv("PIPELINE_OUTPUT_DIR", data / "output")),
        db_path=Path(os.getenv("PIPELINE_DB_PATH", PROJECT_ROOT / "warehouse.duckdb")),
    )
