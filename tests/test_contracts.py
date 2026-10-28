"""Unit tests for the validation boundary (Pydantic contracts).

Tests assert that validation *fails or succeeds*, never on exact error wording,
because message text can differ between compatible Pydantic versions.
"""
import pytest
from pydantic import ValidationError

from src.contracts import EntityContract, TransactionContract


def failing_fields(exc_info) -> set[str]:
    return {str(e["loc"][0]) for e in exc_info.value.errors()}


# ---- happy paths -----------------------------------------------------------
def test_valid_transaction_passes(valid_transaction):
    assert TransactionContract(**valid_transaction).transaction_amount == 150.50


def test_valid_entity_passes(valid_entity):
    assert EntityContract(**valid_entity).region == "North"


def test_entity_preserves_source_values(valid_entity):
    """Contracts validate, they do not clean: Bronze keeps what was received."""
    valid_entity["region"] = " north "
    assert EntityContract(**valid_entity).region == " north "


# ---- the three required tests ----------------------------------------------
def test_transaction_rejects_negative_amount(valid_transaction):
    valid_transaction["transaction_amount"] = -150.50
    with pytest.raises(ValidationError) as exc:
        TransactionContract(**valid_transaction)
    assert "transaction_amount" in failing_fields(exc)


@pytest.mark.parametrize("bad_email", [
    "john.example.com", "john@", "@example.com", "john.example", "", "john smith@example.com",
])
def test_entity_rejects_invalid_email(valid_entity, bad_email):
    valid_entity["contact_email"] = bad_email
    with pytest.raises(ValidationError) as exc:
        EntityContract(**valid_entity)
    assert "contact_email" in failing_fields(exc)


def test_transaction_rejects_null_entity_id(valid_transaction):
    valid_transaction["entity_id"] = None
    with pytest.raises(ValidationError) as exc:
        TransactionContract(**valid_transaction)
    assert "entity_id" in failing_fields(exc)


def test_transaction_rejects_missing_entity_id_key(valid_transaction):
    del valid_transaction["entity_id"]
    with pytest.raises(ValidationError) as exc:
        TransactionContract(**valid_transaction)
    assert "entity_id" in failing_fields(exc)


# ---- additional edge cases -------------------------------------------------
@pytest.mark.parametrize("amount", [0, 0.0, float("nan"), float("inf"), "abc"])
def test_transaction_rejects_non_positive_or_non_finite_amount(valid_transaction, amount):
    valid_transaction["transaction_amount"] = amount
    with pytest.raises(ValidationError):
        TransactionContract(**valid_transaction)


@pytest.mark.parametrize("currency", ["XXX", "usd", "INR", "", None])
def test_transaction_rejects_unsupported_currency(valid_transaction, currency):
    valid_transaction["currency"] = currency
    with pytest.raises(ValidationError):
        TransactionContract(**valid_transaction)


@pytest.mark.parametrize("currency", ["USD", "EUR", "GBP"])
def test_transaction_accepts_allowed_currencies(valid_transaction, currency):
    valid_transaction["currency"] = currency
    assert TransactionContract(**valid_transaction).currency == currency


@pytest.mark.parametrize("field", ["transaction_id", "entity_id"])
def test_transaction_rejects_invalid_uuid(valid_transaction, field):
    valid_transaction[field] = "not-a-uuid"
    with pytest.raises(ValidationError) as exc:
        TransactionContract(**valid_transaction)
    assert field in failing_fields(exc)


def test_entity_rejects_invalid_uuid(valid_entity):
    valid_entity["entity_id"] = "1234"
    with pytest.raises(ValidationError):
        EntityContract(**valid_entity)


@pytest.mark.parametrize("field", ["first_name", "last_name", "region"])
@pytest.mark.parametrize("value", ["", "   ", None])
def test_entity_rejects_empty_required_text(valid_entity, field, value):
    valid_entity[field] = value
    with pytest.raises(ValidationError) as exc:
        EntityContract(**valid_entity)
    assert field in failing_fields(exc)


def test_entity_rejects_invalid_registration_date(valid_entity):
    valid_entity["registration_date"] = "not a date"
    with pytest.raises(ValidationError):
        EntityContract(**valid_entity)


@pytest.mark.parametrize("field", ["transaction_id", "category", "transaction_amount",
                                   "currency", "timestamp"])
def test_transaction_rejects_missing_required_field(valid_transaction, field):
    del valid_transaction[field]
    with pytest.raises(ValidationError) as exc:
        TransactionContract(**valid_transaction)
    assert field in failing_fields(exc)


def test_transaction_rejects_blank_category(valid_transaction):
    valid_transaction["category"] = "  "
    with pytest.raises(ValidationError):
        TransactionContract(**valid_transaction)
