"""Integration test: generate -> validate -> Bronze -> Silver -> Gold -> queries."""
import json
from datetime import datetime

import duckdb
import pytest

from src.config import Paths
from src.generate_data import generate
from src.pipeline import run_pipeline


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    root = tmp_path_factory.mktemp("pipe")
    paths = Paths(root / "raw", root / "dlq", root / "out", root / "w.duckdb")
    generate(paths, n_entities=300, n_transactions=1500, seed=11, now=datetime.now().replace(microsecond=0))
    result = run_pipeline(paths)
    con = duckdb.connect(str(paths.db_path), read_only=True)
    yield paths, result, con
    con.close()


def scalar(con, sql):
    return con.execute(sql).fetchone()[0]


def test_invalid_records_go_to_dlq_not_bronze(built):
    paths, result, con = built
    c = result["counts"]
    assert c["entities_dlq"] > 0 and c["transactions_dlq"] > 0
    assert c["bronze_entities"] == c["entities_valid"]
    assert c["bronze_transactions"] == c["transactions_valid"]
    assert len(json.loads(paths.invalid_transactions_json.read_text())) == c["transactions_dlq"]
    assert scalar(con, "SELECT COUNT(*) FROM bronze_transactions WHERE transaction_amount <= 0") == 0
    assert scalar(con, "SELECT COUNT(*) FROM bronze_transactions WHERE entity_id IS NULL") == 0
    assert scalar(con, "SELECT COUNT(*) FROM bronze_entities WHERE contact_email NOT LIKE '%_@_%._%'") == 0


def test_bronze_keeps_duplicates_silver_removes_them(built):
    _, _, con = built
    assert scalar(con, "SELECT COUNT(*) - COUNT(DISTINCT entity_id) FROM bronze_entities") > 0
    assert scalar(con, "SELECT COUNT(*) - COUNT(DISTINCT entity_id) FROM silver_entities") == 0


def test_silver_standardises_region(built):
    _, _, con = built
    assert scalar(con, "SELECT COUNT(*) FROM silver_entities WHERE region <> UPPER(TRIM(region))") == 0
    assert scalar(con, "SELECT COUNT(DISTINCT region) FROM silver_entities") == 8


def test_silver_transactions_only_reference_known_entities(built):
    _, result, con = built
    assert scalar(con, """SELECT COUNT(*) FROM silver_transactions t
                          LEFT JOIN silver_entities e USING (entity_id) WHERE e.entity_id IS NULL""") == 0
    c = result["counts"]
    assert c["silver_transactions"] + c["silver_orphan_transactions"] == c["bronze_transactions"]


def test_star_schema_referential_integrity(built):
    _, result, con = built
    assert scalar(con, """SELECT COUNT(*) FROM fact_transactions f
                          LEFT JOIN dim_entity e USING (entity_key) WHERE e.entity_key IS NULL""") == 0
    assert scalar(con, """SELECT COUNT(*) FROM fact_transactions f
                          LEFT JOIN dim_date d USING (date_key) WHERE d.date_key IS NULL""") == 0
    assert result["counts"]["fact_transactions"] == result["counts"]["silver_transactions"]
    # the fact's date_key must agree with its own timestamp
    assert scalar(con, """SELECT COUNT(*) FROM fact_transactions f JOIN dim_date d USING (date_key)
                          WHERE d.full_date <> CAST(f."timestamp" AS DATE)""") == 0


def test_query1_covers_only_current_month(built):
    _, result, con = built
    cols, rows = result["queries"]["q1_monthly_revenue_by_category"]
    assert cols[:2] == ["category", "total_transaction_amount"]
    expected = scalar(con, """SELECT SUM(f.transaction_amount) FROM fact_transactions f
                              JOIN dim_date d USING (date_key)
                              WHERE d.year = YEAR(CURRENT_DATE) AND d.month = MONTH(CURRENT_DATE)""")
    assert rows and sum(r[1] for r in rows) == expected


def test_query2_returns_five_regions_sorted_desc(built):
    _, result, _ = built
    cols, rows = result["queries"]["q2_top_regions_avg_per_entity"]
    assert cols[:2] == ["region", "average_transaction_amount_per_entity"]
    assert len(rows) == 5
    values = [r[1] for r in rows]
    assert values == sorted(values, reverse=True)


def test_pipeline_is_idempotent(tmp_path):
    """Re-running on the same inputs rebuilds every layer with identical row counts."""
    paths = Paths(tmp_path / "raw", tmp_path / "dlq", tmp_path / "out", tmp_path / "w.duckdb")
    generate(paths, n_entities=100, n_transactions=400, seed=5, now=datetime.now().replace(microsecond=0))
    first = run_pipeline(paths)
    second = run_pipeline(paths)
    assert first["counts"] == second["counts"]
