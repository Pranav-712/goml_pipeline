import pytest


@pytest.fixture
def valid_transaction() -> dict:
    return {
        "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
        "entity_id": "550e8400-e29b-41d4-a716-446655440001",
        "category": "Retail",
        "transaction_amount": 150.50,
        "currency": "USD",
        "timestamp": "2026-09-24T10:00:00",
    }


@pytest.fixture
def valid_entity() -> dict:
    return {
        "entity_id": "550e8400-e29b-41d4-a716-446655440001",
        "first_name": "John",
        "last_name": "Smith",
        "contact_email": "john.smith@example.com",
        "registration_date": "2025-01-15T08:30:00",
        "region": "North",
    }
