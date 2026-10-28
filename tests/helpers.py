"""Shared helpers for building tiny, hand-controlled raw files."""
import csv
import json
from pathlib import Path

import duckdb

from src.config import Paths

ENTITY_FIELDS = ["entity_id", "first_name", "last_name", "contact_email", "registration_date", "region"]


def make_paths(root: Path) -> Paths:
    return Paths(root / "raw", root / "dlq", root / "out", root / "w.duckdb")


def uid(n: int) -> str:
    return f"00000000-0000-4000-8000-{n:012d}"


def entity(n: int, region: str = "North", **overrides) -> dict:
    row = {"entity_id": uid(n), "first_name": f"First{n}", "last_name": f"Last{n}",
           "contact_email": f"user{n}@example.com", "registration_date": "2025-01-01T00:00:00",
           "region": region}
    row.update(overrides)
    return row


def tx(n: int, entity_n: int, amount: float, ts: str, category: str = "Retail",
       currency: str = "USD", **overrides) -> dict:
    row = {"transaction_id": uid(1000 + n), "entity_id": uid(entity_n), "category": category,
           "transaction_amount": amount, "currency": currency, "timestamp": ts}
    row.update(overrides)
    return row


def write_entities(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=ENTITY_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def write_transactions(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows))


def query(paths: Paths, sql: str) -> list[tuple]:
    con = duckdb.connect(str(paths.db_path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()
