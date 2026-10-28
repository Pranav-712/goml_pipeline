"""DuckDB helpers: connection, Bronze load, batch ledger, transforms, queries."""
from __future__ import annotations

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

import duckdb

from src.config import QUERIES_DIR, SQL_DIR
from src.ingestion import Batch, ValidationResult, to_naive_utc

log = logging.getLogger(__name__)

# Children before parents (foreign keys), Gold -> Silver -> Bronze -> ledger.
RESET_ORDER = ["dq_report", "fact_transactions", "dim_date", "dim_entity",
               "silver_orphan_transactions", "silver_transactions", "silver_entities",
               "bronze_transactions", "bronze_entities", "etl_batches"]
COUNTED_TABLES = ["bronze_entities", "bronze_transactions", "silver_entities", "silver_transactions",
                  "silver_orphan_transactions", "dim_entity", "dim_date", "fact_transactions"]

ENTITY_COLUMNS = ["entity_id", "first_name", "last_name", "contact_email", "registration_date",
                  "region", "_source_file", "_source_row", "_batch_id", "_ingested_at"]
TRANSACTION_COLUMNS = ["transaction_id", "entity_id", "category", "transaction_amount", "currency",
                       "timestamp", "_source_file", "_source_row", "_batch_id", "_ingested_at"]


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def run_sql_file(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    con.execute(path.read_text(encoding="utf-8"))


def insert_many(con: duckdb.DuckDBPyConnection, table: str, columns: Sequence[str],
                rows: Sequence[Sequence[Any]], chunk: int = 500) -> None:
    """Multi-row parameterised INSERT (far faster than one statement per row)."""
    col_list = ", ".join(f'"{c}"' for c in columns)
    placeholder = "(" + ", ".join("?" * len(columns)) + ")"
    for i in range(0, len(rows), chunk):
        part = rows[i:i + chunk]
        sql = f"INSERT INTO {table} ({col_list}) VALUES " + ", ".join([placeholder] * len(part))
        con.execute(sql, [v for row in part for v in row])


# ---------------------------------------------------------------- schema / state
def reset_warehouse(con: duckdb.DuckDBPyConnection) -> None:
    """Full refresh: drop every pipeline table (a full run = reset + the incremental logic)."""
    for table in RESET_ORDER:
        con.execute(f"DROP TABLE IF EXISTS {table}")


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, SQL_DIR / "00_bronze_ddl.sql")


def processed_hashes(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {r[0] for r in con.execute("SELECT file_sha256 FROM etl_batches").fetchall()}


# ---------------------------------------------------------------- bronze
def load_bronze_batch(con: duckdb.DuckDBPyConnection, batch: Batch, result: ValidationResult,
                      rows_read: int) -> None:
    """Insert one batch's contract-valid rows and record it in the ledger, atomically.

    Invalid records never reach Bronze. The ledger row is written in the same transaction,
    so a batch is either fully loaded and recorded, or not at all.
    """
    now = datetime.now().replace(microsecond=0)
    if batch.source == "entities":
        table, columns = "bronze_entities", ENTITY_COLUMNS
        rows = [(str(m.entity_id), m.first_name, m.last_name, m.contact_email,
                 to_naive_utc(m.registration_date), m.region,
                 batch.path.name, row, batch.batch_id, now) for row, m in result.valid]
    else:
        table, columns = "bronze_transactions", TRANSACTION_COLUMNS
        rows = [(str(m.transaction_id), str(m.entity_id), m.category, m.transaction_amount,
                 m.currency, to_naive_utc(m.timestamp),
                 batch.path.name, row, batch.batch_id, now) for row, m in result.valid]

    con.begin()
    try:
        insert_many(con, table, columns, rows)
        con.execute(
            "INSERT INTO etl_batches VALUES (?, ?, ?, ?, ?, ?, ?, 'LOADED', ?)",
            [batch.batch_id, batch.source, batch.path.name, batch.sha256,
             rows_read, len(result.valid), len(result.invalid), now])
        con.commit()
    except Exception:
        con.rollback()
        raise


# ---------------------------------------------------------------- silver / gold
def build_silver(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, SQL_DIR / "01_silver.sql")


def build_gold(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, SQL_DIR / "02_gold.sql")


def transform_pending(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Run Silver + Gold for every batch still in LOADED state, in ONE transaction.

    If anything fails, Silver and Gold roll back together and the batches stay LOADED, so the
    next run resumes them (no half-built warehouse, no manual clean-up).
    """
    pending = [r[0] for r in con.execute(
        "SELECT batch_id FROM etl_batches WHERE status = 'LOADED' ORDER BY loaded_at, batch_id").fetchall()]
    if not pending:
        return []
    con.begin()
    try:
        con.execute("CREATE OR REPLACE TEMP TABLE _new_batches (_batch_id VARCHAR)")
        insert_many(con, "_new_batches", ["_batch_id"], [(b,) for b in pending])
        build_silver(con)
        build_gold(con)
        con.execute("UPDATE etl_batches SET status = 'COMPLETE' WHERE status = 'LOADED'")
        con.commit()
    except Exception:
        con.rollback()
        raise
    return pending


# ---------------------------------------------------------------- reporting
def table_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for source in ("entities", "transactions"):
        valid, invalid = con.execute(
            "SELECT COALESCE(SUM(rows_valid), 0), COALESCE(SUM(rows_invalid), 0) "
            "FROM etl_batches WHERE source = ?", [source]).fetchone()
        counts[f"{source}_valid"], counts[f"{source}_dlq"] = int(valid), int(invalid)
    for table in COUNTED_TABLES:
        counts[table] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return counts


def run_query(con: duckdb.DuckDBPyConnection, sql_path: Path) -> tuple[list[str], list[tuple[Any, ...]]]:
    cur = con.execute(sql_path.read_text(encoding="utf-8"))
    return [c[0] for c in cur.description], cur.fetchall()


def run_all_queries(con: duckdb.DuckDBPyConnection, output_dir: Path
                    ) -> dict[str, tuple[list[str], list[tuple[Any, ...]]]]:
    """Run every sql/queries/*.sql file, return results and write each as CSV."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results = {}
    for sql_path in sorted(QUERIES_DIR.glob("*.sql")):
        cols, rows = run_query(con, sql_path)
        results[sql_path.stem] = (cols, rows)
        with (output_dir / f"{sql_path.stem}.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(cols)
            writer.writerows(rows)
    return results
