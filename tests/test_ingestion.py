"""Tests that invalid records are routed to the DLQ without stopping the run."""
import csv
import json

from src.contracts import EntityContract, TransactionContract
from src.ingestion import ingest, validate_records


def test_validate_records_splits_valid_and_invalid(valid_transaction):
    bad = {**valid_transaction, "transaction_amount": -1}
    result = validate_records([valid_transaction, bad, valid_transaction], TransactionContract)
    assert len(result.valid) == 2
    assert len(result.invalid) == 1


def test_one_bad_record_does_not_stop_processing(valid_transaction):
    records = [{"garbage": True}, valid_transaction, "not-an-object", None, valid_transaction]
    result = validate_records(records, TransactionContract)
    assert len(result.valid) == 2
    assert len(result.invalid) == 3


def test_dlq_entry_keeps_original_payload_and_error(valid_transaction):
    bad = {**valid_transaction, "entity_id": None}
    entry = validate_records([bad], TransactionContract).invalid[0]
    assert entry["record"] == bad                 # original payload preserved
    assert "entity_id" in entry["error"]          # says what failed
    assert entry["source_row"] == 1


def test_missing_key_reported_as_required(valid_transaction):
    bad = dict(valid_transaction)
    del bad["entity_id"]
    entry = validate_records([bad], TransactionContract).invalid[0]
    assert entry["error"].startswith("entity_id:")


def test_ingest_writes_dlq_files_and_only_valid_rows_survive(tmp_path, valid_entity, valid_transaction):
    entities_csv = tmp_path / "entities.csv"
    with entities_csv.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(valid_entity))
        writer.writeheader()
        writer.writerow(valid_entity)
        writer.writerow({**valid_entity, "contact_email": "john@"})

    transactions_json = tmp_path / "transactions.json"
    transactions_json.write_text(json.dumps([
        valid_transaction,
        {**valid_transaction, "currency": "XXX"},
        {**valid_transaction, "entity_id": None},
    ]))

    dlq_e, dlq_t = tmp_path / "dlq" / "e.json", tmp_path / "dlq" / "t.json"
    entities, transactions = ingest(entities_csv, transactions_json, dlq_e, dlq_t)

    assert (len(entities.valid), len(entities.invalid)) == (1, 1)
    assert (len(transactions.valid), len(transactions.invalid)) == (1, 2)

    written = json.loads(dlq_t.read_text())
    assert len(written) == 2
    assert all({"record", "error"} <= set(entry) for entry in written)
    assert json.loads(dlq_e.read_text())[0]["record"]["contact_email"] == "john@"


def test_empty_csv_value_is_rejected_by_entity_contract(valid_entity):
    """CSV gives '' (not None) for blanks; the contract must still reject it."""
    bad = {**valid_entity, "first_name": ""}
    assert len(validate_records([bad], EntityContract).invalid) == 1
