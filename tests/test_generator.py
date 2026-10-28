"""The generator must plant the data-quality issues the brief asks for."""
import random
from collections import Counter
from datetime import datetime

import pytest
from pydantic import ValidationError

from src.contracts import EntityContract, TransactionContract
from src.generate_data import generate_entities, generate_transactions

NOW = datetime(2026, 10, 27, 12, 0, 0)


def _email_is_bad(row: dict) -> bool:
    try:
        EntityContract(**row)
        return False
    except ValidationError as exc:
        return any(e["loc"][0] == "contact_email" for e in exc.errors())


@pytest.mark.parametrize("n", [100, 250, 1000])
def test_entities_have_at_least_5_per_100_duplicates_and_malformed_emails(n):
    rows = generate_entities(n, random.Random(1), NOW)
    assert len(rows) == n

    counts = Counter(tuple(sorted(r.items())) for r in rows)
    duplicate_records = sum(c - 1 for c in counts.values() if c > 1)
    malformed = [r for r in rows if _email_is_bad(r)]

    assert duplicate_records >= 5 * n / 100
    assert len(malformed) >= 5 * n / 100


def test_duplicates_and_malformed_emails_are_separate_records():
    rows = generate_entities(1000, random.Random(2), NOW)
    counts = Counter(tuple(sorted(r.items())) for r in rows)
    duplicated = [r for r in rows if counts[tuple(sorted(r.items()))] > 1]
    assert duplicated, "expected some duplicates"
    assert not any(_email_is_bad(r) for r in duplicated)


def test_transactions_contain_each_planted_issue():
    ids = [f"550e8400-e29b-41d4-a716-4466554400{i:02d}" for i in range(50)]
    rows = generate_transactions(2000, ids, random.Random(3), NOW)
    assert any(isinstance(r.get("transaction_amount"), (int, float)) and r["transaction_amount"] < 0 for r in rows)
    assert any("entity_id" not in r for r in rows)               # key missing
    assert any("entity_id" in r and r["entity_id"] is None for r in rows)  # explicit null
    assert any(r.get("currency") not in {"USD", "EUR", "GBP"} for r in rows)
    valid = 0
    for r in rows:
        try:
            TransactionContract(**r)
            valid += 1
        except ValidationError:
            pass
    assert 0 < valid < len(rows)


def test_generator_is_reproducible_for_a_given_seed():
    a = generate_entities(200, random.Random(7), NOW)
    b = generate_entities(200, random.Random(7), NOW)
    assert a == b
