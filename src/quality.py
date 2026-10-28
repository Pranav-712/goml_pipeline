"""Post-load data-quality & reconciliation checks, persisted to the ``dq_report`` table.

Two severities:
  * error - an invariant of the pipeline is broken (counts don't reconcile, orphaned keys, ...)
  * warn  - the data looks suspicious but the pipeline is still correct (duplicate e-mails, ...)

Every check is plain SQL returning the number of offending rows, so checks are easy to read,
add and audit. ``python -m src.pipeline --strict`` exits non-zero if any *error* check fails.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

import duckdb

from src.config import ALLOWED_CURRENCIES, DEFAULT_MAX_INVALID_RATIO, MAX_ORPHAN_RATIO
from src.warehouse import insert_many

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Check:
    name: str
    layer: str
    severity: str          # "error" | "warn"
    description: str
    failed_sql: str        # -> single number: offending rows
    total_sql: str         # -> single number: rows examined


def build_checks(max_invalid_ratio: float = DEFAULT_MAX_INVALID_RATIO) -> list[Check]:
    currencies = ", ".join(f"'{c}'" for c in sorted(ALLOWED_CURRENCIES))

    def dlq_ratio(source: str) -> Check:
        return Check(
            f"dlq_ratio_{source}", "ingestion", "error",
            f"{source}: share of rows rejected to the DLQ is <= {max_invalid_ratio:.0%} (circuit breaker)",
            f"SELECT CASE WHEN COALESCE(SUM(rows_invalid), 0) > {max_invalid_ratio} * NULLIF(SUM(rows_read), 0) "
            f"THEN SUM(rows_invalid) ELSE 0 END FROM etl_batches WHERE source = '{source}'",
            f"SELECT COALESCE(SUM(rows_read), 0) FROM etl_batches WHERE source = '{source}'")

    return [
        # ---- reconciliation: every row is accounted for between layers ----------------
        Check("bronze_entities_reconcile", "bronze", "error",
              "Bronze entity rows == rows that passed validation",
              "SELECT ABS((SELECT COUNT(*) FROM bronze_entities) - "
              "COALESCE((SELECT SUM(rows_valid) FROM etl_batches WHERE source = 'entities'), 0))",
              "SELECT COUNT(*) FROM bronze_entities"),
        Check("bronze_transactions_reconcile", "bronze", "error",
              "Bronze transaction rows == rows that passed validation",
              "SELECT ABS((SELECT COUNT(*) FROM bronze_transactions) - "
              "COALESCE((SELECT SUM(rows_valid) FROM etl_batches WHERE source = 'transactions'), 0))",
              "SELECT COUNT(*) FROM bronze_transactions"),
        Check("silver_entities_reconcile", "silver", "error",
              "Silver entities == distinct entity_id in Bronze",
              "SELECT ABS((SELECT COUNT(DISTINCT entity_id) FROM bronze_entities) - "
              "(SELECT COUNT(*) FROM silver_entities))",
              "SELECT COUNT(DISTINCT entity_id) FROM bronze_entities"),
        Check("silver_transactions_reconcile", "silver", "error",
              "Silver transactions + orphans == distinct transaction_id in Bronze",
              "SELECT ABS((SELECT COUNT(DISTINCT transaction_id) FROM bronze_transactions) - "
              "(SELECT COUNT(*) FROM silver_transactions) - (SELECT COUNT(*) FROM silver_orphan_transactions))",
              "SELECT COUNT(DISTINCT transaction_id) FROM bronze_transactions"),
        Check("gold_dim_entity_reconcile", "gold", "error",
              "dim_entity rows == Silver entities",
              "SELECT ABS((SELECT COUNT(*) FROM dim_entity) - (SELECT COUNT(*) FROM silver_entities))",
              "SELECT COUNT(*) FROM silver_entities"),
        Check("gold_fact_reconcile", "gold", "error",
              "fact_transactions rows == Silver transactions",
              "SELECT ABS((SELECT COUNT(*) FROM fact_transactions) - (SELECT COUNT(*) FROM silver_transactions))",
              "SELECT COUNT(*) FROM silver_transactions"),
        # ---- integrity -----------------------------------------------------------------
        Check("silver_no_null_fields", "silver", "error",
              "No NULLs in Silver key / required columns",
              "SELECT (SELECT COUNT(*) FROM silver_entities WHERE first_name IS NULL OR last_name IS NULL "
              "OR contact_email IS NULL OR region IS NULL OR registration_date IS NULL) + "
              "(SELECT COUNT(*) FROM silver_transactions WHERE entity_id IS NULL OR category IS NULL "
              "OR transaction_amount IS NULL OR currency IS NULL OR \"timestamp\" IS NULL)",
              "SELECT (SELECT COUNT(*) FROM silver_entities) + (SELECT COUNT(*) FROM silver_transactions)"),
        Check("silver_region_standardised", "silver", "error",
              "Region is trimmed and upper-case in Silver",
              "SELECT COUNT(*) FROM silver_entities WHERE region <> UPPER(TRIM(region))",
              "SELECT COUNT(*) FROM silver_entities"),
        Check("fact_amount_positive", "gold", "error",
              "Every fact amount is > 0",
              "SELECT COUNT(*) FROM fact_transactions WHERE transaction_amount <= 0",
              "SELECT COUNT(*) FROM fact_transactions"),
        Check("fact_currency_allowed", "gold", "error",
              f"Every fact currency is one of {sorted(ALLOWED_CURRENCIES)}",
              f"SELECT COUNT(*) FROM fact_transactions WHERE currency NOT IN ({currencies})",
              "SELECT COUNT(*) FROM fact_transactions"),
        Check("fact_dimension_keys_resolve", "gold", "error",
              "Every fact row resolves to a dim_entity and a dim_date row",
              "SELECT COUNT(*) FROM fact_transactions f "
              "LEFT JOIN dim_entity e ON e.entity_key = f.entity_key "
              "LEFT JOIN dim_date d ON d.date_key = f.date_key "
              "WHERE e.entity_key IS NULL OR d.date_key IS NULL",
              "SELECT COUNT(*) FROM fact_transactions"),
        Check("fact_date_key_matches_timestamp", "gold", "error",
              "fact.date_key agrees with the date of fact.timestamp",
              "SELECT COUNT(*) FROM fact_transactions f JOIN dim_date d ON d.date_key = f.date_key "
              "WHERE d.full_date <> CAST(f.\"timestamp\" AS DATE)",
              "SELECT COUNT(*) FROM fact_transactions"),
        Check("dim_date_contiguous", "gold", "error",
              "dim_date has no gaps between its first and last day",
              "SELECT COALESCE((SELECT DATEDIFF('day', MIN(full_date), MAX(full_date)) + 1 FROM dim_date) "
              "- (SELECT COUNT(*) FROM dim_date), 0)",
              "SELECT COUNT(*) FROM dim_date"),
        dlq_ratio("entities"),
        dlq_ratio("transactions"),
        # ---- suspicious-but-not-broken (warnings) ---------------------------------------
        Check("duplicate_emails_across_entities", "silver", "warn",
              "No e-mail address is shared by two different entity_ids",
              "SELECT COUNT(*) FROM (SELECT contact_email FROM silver_entities "
              "GROUP BY contact_email HAVING COUNT(*) > 1)",
              "SELECT COUNT(DISTINCT contact_email) FROM silver_entities"),
        Check("future_dated_transactions", "silver", "warn",
              "No transaction is timestamped in the future",
              "SELECT COUNT(*) FROM silver_transactions WHERE \"timestamp\" > CAST(now() AS TIMESTAMP)",
              "SELECT COUNT(*) FROM silver_transactions"),
        Check("future_registration_dates", "silver", "warn",
              "No entity is registered in the future",
              "SELECT COUNT(*) FROM silver_entities WHERE registration_date > CAST(now() AS TIMESTAMP)",
              "SELECT COUNT(*) FROM silver_entities"),
        Check("orphan_transaction_rate", "silver", "warn",
              f"At most {MAX_ORPHAN_RATIO:.0%} of valid transactions reference an unknown entity",
              f"SELECT CASE WHEN (SELECT COUNT(*) FROM silver_orphan_transactions) > {MAX_ORPHAN_RATIO} * "
              "NULLIF((SELECT COUNT(*) FROM silver_transactions) + (SELECT COUNT(*) FROM silver_orphan_transactions), 0) "
              "THEN (SELECT COUNT(*) FROM silver_orphan_transactions) ELSE 0 END",
              "SELECT (SELECT COUNT(*) FROM silver_transactions) + (SELECT COUNT(*) FROM silver_orphan_transactions)"),
    ]


def run_checks(con: duckdb.DuckDBPyConnection, run_id: str,
               max_invalid_ratio: float = DEFAULT_MAX_INVALID_RATIO) -> list[dict]:
    """Run every check, persist the results to ``dq_report`` and return them."""
    checked_at = datetime.now().replace(microsecond=0)
    results = []
    for check in build_checks(max_invalid_ratio):
        failed = int(con.execute(check.failed_sql).fetchone()[0] or 0)
        total = int(con.execute(check.total_sql).fetchone()[0] or 0)
        status = "PASS" if failed == 0 else "FAIL"
        results.append({"run_id": run_id, "checked_at": checked_at, "check_name": check.name,
                        "layer": check.layer, "severity": check.severity,
                        "description": check.description, "failed_rows": failed,
                        "total_rows": total, "status": status})
        level = logging.INFO if status == "PASS" else (logging.ERROR if check.severity == "error" else logging.WARNING)
        log.log(level, "dq check", extra={"check": check.name, "severity": check.severity,
                                          "status": status, "failed_rows": failed, "total_rows": total})

    con.execute("""CREATE OR REPLACE TABLE dq_report (
        run_id VARCHAR, checked_at TIMESTAMP, check_name VARCHAR, layer VARCHAR, severity VARCHAR,
        description VARCHAR, failed_rows BIGINT, total_rows BIGINT, status VARCHAR)""")
    columns = ["run_id", "checked_at", "check_name", "layer", "severity", "description",
               "failed_rows", "total_rows", "status"]
    insert_many(con, "dq_report", columns, [[r[c] for c in columns] for r in results])
    return results


def has_errors(results: list[dict]) -> bool:
    return any(r["severity"] == "error" and r["status"] == "FAIL" for r in results)


def summarize(results: list[dict]) -> dict[str, int]:
    return {
        "checks": len(results),
        "passed": sum(r["status"] == "PASS" for r in results),
        "failed_errors": sum(r["severity"] == "error" and r["status"] == "FAIL" for r in results),
        "failed_warnings": sum(r["severity"] == "warn" and r["status"] == "FAIL" for r in results),
    }
