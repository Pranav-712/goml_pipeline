"""Incremental processing: idempotency, new batches, late-arriving data, crash recovery."""
import shutil
from datetime import datetime

import pytest

from src import warehouse
from src.generate_data import generate
from src.ingestion import discover_batches
from src.pipeline import run_pipeline
from tests.helpers import entity, make_paths, query, tx, write_entities, write_transactions

NOW = datetime.now().replace(microsecond=0)
TS = NOW.replace(hour=1, minute=0, second=0).isoformat()

FINGERPRINTS = {
    "silver_entities": "SELECT * FROM silver_entities ORDER BY entity_id",
    "silver_transactions": "SELECT * FROM silver_transactions ORDER BY transaction_id",
    "orphans": "SELECT * FROM silver_orphan_transactions ORDER BY transaction_id",
    "dim_date": "SELECT * FROM dim_date ORDER BY date_key",
    # surrogate keys may differ between a full and an incremental build, so compare via the natural key
    "fact": """SELECT f.transaction_id, e.entity_id, f.date_key, f.category, f.transaction_amount,
                      f.currency, f."timestamp"
               FROM fact_transactions f JOIN dim_entity e USING (entity_key) ORDER BY f.transaction_id""",
    "dim_entity": """SELECT entity_id, first_name, last_name, contact_email, registration_date, region
                     FROM dim_entity ORDER BY entity_id""",
}


def fingerprint(paths):
    return {name: query(paths, sql) for name, sql in FINGERPRINTS.items()}


def test_rerun_with_same_files_is_a_noop(tmp_path):
    paths = make_paths(tmp_path)
    generate(paths, 100, 400, seed=1, now=NOW)
    first = run_pipeline(paths, mode="incremental")
    before = fingerprint(paths)
    second = run_pipeline(paths, mode="incremental")
    assert len(first["batches_loaded"]) == 2 and second["batches_loaded"] == []
    assert len(second["batches_skipped"]) == 2 and second["batches_transformed"] == 0
    assert second["counts"] == first["counts"] and fingerprint(paths) == before


def test_new_batch_adds_only_new_rows(tmp_path):
    paths = make_paths(tmp_path)
    generate(paths, 100, 400, seed=1, now=NOW)
    first = run_pipeline(paths, mode="incremental")
    generate(paths, 100, 400, seed=2, now=NOW, suffix="_batch2", include_existing_entities=True)
    second = run_pipeline(paths, mode="incremental")

    assert sorted(second["batches_loaded"]) == ["entities_batch2.csv", "transactions_batch2.json"]
    assert sorted(second["batches_skipped"]) == ["entities.csv", "transactions.json"]
    assert second["counts"]["bronze_entities"] > first["counts"]["bronze_entities"]
    assert second["counts"]["fact_transactions"] > first["counts"]["fact_transactions"]
    assert query(paths, "SELECT COUNT(*) FROM etl_batches WHERE status = 'COMPLETE'") == [(4,)]
    assert (paths.dlq_dir / "invalid_transactions_batch2.json").exists()


def test_incremental_converges_to_the_same_warehouse_as_a_full_rebuild(tmp_path):
    inc = make_paths(tmp_path / "inc")
    generate(inc, 150, 600, seed=1, now=NOW)
    run_pipeline(inc, mode="incremental")
    generate(inc, 80, 500, seed=2, now=NOW, suffix="_batch2", include_existing_entities=True)
    run_pipeline(inc, mode="incremental")

    full = make_paths(tmp_path / "full")
    shutil.copytree(inc.raw_dir, full.raw_dir)
    run_pipeline(full, mode="full")

    assert fingerprint(inc) == fingerprint(full)


def test_surrogate_keys_stay_stable_across_incremental_runs(tmp_path):
    paths = make_paths(tmp_path)
    generate(paths, 100, 300, seed=1, now=NOW)
    run_pipeline(paths, mode="incremental")
    keys_before = dict(query(paths, "SELECT entity_id, entity_key FROM dim_entity"))
    generate(paths, 100, 300, seed=2, now=NOW, suffix="_batch2")
    run_pipeline(paths, mode="incremental")
    keys_after = dict(query(paths, "SELECT entity_id, entity_key FROM dim_entity"))
    assert all(keys_after[k] == v for k, v in keys_before.items())
    assert len(set(keys_after.values())) == len(keys_after) > len(keys_before)


def test_late_arriving_entity_resolves_earlier_orphan(tmp_path):
    paths = make_paths(tmp_path)
    write_transactions(paths.raw_dir / "transactions_a.json", [tx(1, 1, 25.0, TS)])
    day1 = run_pipeline(paths, mode="incremental")        # no entity file has arrived yet
    assert (day1["counts"]["silver_orphan_transactions"], day1["counts"]["fact_transactions"]) == (1, 0)

    write_entities(paths.raw_dir / "entities_b.csv", [entity(1, "north")])
    day2 = run_pipeline(paths, mode="incremental")        # the dimension row shows up late
    assert (day2["counts"]["silver_orphan_transactions"], day2["counts"]["fact_transactions"]) == (0, 1)
    assert query(paths, "SELECT region FROM dim_entity") == [("NORTH",)]
    assert day2["data_quality"]["failed_errors"] == 0


def test_first_received_record_wins_across_batches(tmp_path):
    paths = make_paths(tmp_path)
    write_entities(paths.raw_dir / "entities_1.csv", [entity(1, first_name="Original")])
    run_pipeline(paths, mode="incremental")
    write_entities(paths.raw_dir / "entities_2.csv", [entity(1, first_name="Changed")])
    write_transactions(paths.raw_dir / "transactions.json", [tx(1, 1, 5.0, TS)])
    run_pipeline(paths, mode="incremental")
    assert query(paths, "SELECT first_name FROM silver_entities") == [("Original",)]
    run_pipeline(paths, mode="full")                       # a full rebuild applies the same rule
    assert query(paths, "SELECT first_name FROM silver_entities") == [("Original",)]


def test_duplicate_transaction_across_batches_is_loaded_once(tmp_path):
    paths = make_paths(tmp_path)
    write_entities(paths.raw_dir / "entities.csv", [entity(1)])
    write_transactions(paths.raw_dir / "transactions_1.json", [tx(1, 1, 10.0, TS)])
    write_transactions(paths.raw_dir / "transactions_2.json", [tx(1, 1, 10.0, TS), tx(2, 1, 20.0, TS)])
    run_pipeline(paths, mode="incremental")
    assert query(paths, "SELECT COUNT(*), SUM(transaction_amount) FROM fact_transactions") == [(2, 30)]


def test_file_with_identical_content_is_ignored(tmp_path):
    paths = make_paths(tmp_path)
    generate(paths, 100, 300, seed=1, now=NOW)
    shutil.copy(paths.entities_csv, paths.raw_dir / "entities_copy.csv")
    assert [b.path.name for b in discover_batches(paths.raw_dir)] == ["entities.csv", "transactions.json"]


def test_full_mode_resets_previous_state(tmp_path):
    paths = make_paths(tmp_path)
    generate(paths, 100, 300, seed=1, now=NOW)
    run_pipeline(paths, mode="incremental")
    generate(paths, 100, 300, seed=7, now=NOW)             # same file names, different content
    result = run_pipeline(paths, mode="full")
    assert query(paths, "SELECT COUNT(*) FROM etl_batches") == [(2,)]
    assert result["counts"]["bronze_entities"] == result["counts"]["entities_valid"]


def test_failed_transform_rolls_back_and_next_run_recovers(tmp_path, monkeypatch):
    paths = make_paths(tmp_path)
    generate(paths, 100, 400, seed=3, now=NOW)

    def boom(con):
        raise RuntimeError("simulated crash after Silver was built")

    with monkeypatch.context() as m:
        m.setattr(warehouse, "build_gold", boom)
        with pytest.raises(RuntimeError):
            run_pipeline(paths, mode="incremental")

    # Bronze was committed; Silver/Gold rolled back together; batches are still pending.
    assert query(paths, "SELECT status, COUNT(*) FROM etl_batches GROUP BY status") == [("LOADED", 2)]
    assert query(paths, "SELECT COUNT(*) FROM bronze_entities")[0][0] > 0
    tables = {r[0] for r in query(paths, "SELECT table_name FROM information_schema.tables")}
    assert "silver_entities" not in tables and "fact_transactions" not in tables

    recovered = run_pipeline(paths, mode="incremental")    # resumes the pending batches
    assert recovered["batches_loaded"] == [] and recovered["batches_transformed"] == 2
    assert recovered["data_quality"]["failed_errors"] == 0

    reference = make_paths(tmp_path / "ref")
    shutil.copytree(paths.raw_dir, reference.raw_dir)
    run_pipeline(reference, mode="full")
    assert fingerprint(paths) == fingerprint(reference)
