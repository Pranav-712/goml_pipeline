"""Data-quality checks must pass on good data and actually catch problems."""
import duckdb
import pytest

from src import quality
from src.generate_data import generate
from src.pipeline import run_pipeline
from tests.helpers import entity, make_paths, query, tx, write_entities, write_transactions
from datetime import datetime

NOW = datetime.now().replace(microsecond=0)


@pytest.fixture
def built(tmp_path):
    paths = make_paths(tmp_path)
    generate(paths, 200, 800, seed=5, now=NOW)
    result = run_pipeline(paths)
    return paths, result


def by_name(results):
    return {r["check_name"]: r for r in results}


def test_clean_run_passes_every_check(built):
    _, result = built
    assert result["data_quality"]["failed_errors"] == 0
    assert result["data_quality"]["failed_warnings"] == 0
    assert result["data_quality"]["checks"] >= 15


def test_report_is_persisted_in_the_warehouse(built):
    paths, result = built
    n = query(paths, "SELECT COUNT(*) FROM dq_report")[0][0]
    assert n == result["data_quality"]["checks"]
    assert query(paths, "SELECT DISTINCT run_id FROM dq_report") == [(result["run_id"],)]


def test_reconciliation_detects_a_lost_fact_row(built):
    paths, _ = built
    con = duckdb.connect(str(paths.db_path))
    con.execute("DELETE FROM fact_transactions WHERE transaction_id = "
                "(SELECT transaction_id FROM fact_transactions LIMIT 1)")
    results = quality.run_checks(con, "t")
    con.close()
    assert by_name(results)["gold_fact_reconcile"]["status"] == "FAIL"
    assert quality.has_errors(results)


def test_warnings_do_not_count_as_errors(built):
    paths, _ = built
    con = duckdb.connect(str(paths.db_path))
    con.execute("UPDATE silver_entities SET contact_email = 'shared@example.com' "
                "WHERE entity_id IN (SELECT entity_id FROM silver_entities LIMIT 2)")
    results = quality.run_checks(con, "t")
    con.close()
    checks = by_name(results)
    assert checks["duplicate_emails_across_entities"]["status"] == "FAIL"
    assert checks["duplicate_emails_across_entities"]["severity"] == "warn"
    assert not quality.has_errors(results)


def test_future_dated_transaction_is_flagged(tmp_path):
    paths = make_paths(tmp_path)
    write_entities(paths.raw_dir / "entities.csv", [entity(1)])
    write_transactions(paths.raw_dir / "transactions.json", [tx(1, 1, 5.0, "2999-01-01T00:00:00")])
    result = run_pipeline(paths)
    assert by_name(result["dq"])["future_dated_transactions"]["failed_rows"] == 1
    assert result["data_quality"]["failed_errors"] == 0


def test_dlq_circuit_breaker_trips_on_mostly_bad_input(tmp_path):
    paths = make_paths(tmp_path)
    write_entities(paths.raw_dir / "entities.csv", [entity(i, contact_email="bad@") for i in range(1, 9)] + [entity(9)])
    write_transactions(paths.raw_dir / "transactions.json", [tx(1, 9, 5.0, NOW.isoformat())])
    result = run_pipeline(paths)                          # default threshold 20%
    check = by_name(result["dq"])["dlq_ratio_entities"]
    assert check["status"] == "FAIL" and check["failed_rows"] == 8 and check["total_rows"] == 9
    assert quality.has_errors(result["dq"])
    assert by_name(result["dq"])["dlq_ratio_transactions"]["status"] == "PASS"


def test_checks_survive_an_empty_warehouse(tmp_path):
    paths = make_paths(tmp_path)
    write_entities(paths.raw_dir / "entities.csv", [entity(1, contact_email="nope")])   # everything rejected
    result = run_pipeline(paths, max_invalid_ratio=1.0)
    assert result["counts"]["fact_transactions"] == 0
    assert result["data_quality"]["failed_errors"] == 0
