"""Pydantic data contracts for the two raw sources.

These are the validation boundary of the pipeline: nothing reaches Bronze
unless it satisfies one of these models.

Design notes
------------
* Contracts *validate* but never *mutate*: whitespace-only strings are rejected
  rather than stripped, so Bronze keeps the values exactly as received.
  Cleaning (trim / casing) is a Silver-layer concern.
* ``transaction_amount`` rejects NaN / Infinity as well as values <= 0.
* ``currency`` is matched case-sensitively against the allow-list.
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, field_validator

from src.config import ALLOWED_CURRENCIES


def _reject_blank(value: str) -> str:
    if not value.strip():
        raise ValueError("must not be blank")
    return value


class EntityContract(BaseModel):
    entity_id: UUID
    first_name: str = Field(..., min_length=1)
    last_name: str = Field(..., min_length=1)
    contact_email: EmailStr
    registration_date: datetime
    region: str = Field(..., min_length=1)

    @field_validator("first_name", "last_name", "region")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        return _reject_blank(value)


class TransactionContract(BaseModel):
    transaction_id: UUID
    entity_id: UUID  # required and non-null: a missing/None value fails validation
    category: str = Field(..., min_length=1)
    transaction_amount: float = Field(..., gt=0, allow_inf_nan=False)
    currency: str
    timestamp: datetime

    @field_validator("category")
    @classmethod
    def _category_not_blank(cls, value: str) -> str:
        return _reject_blank(value)

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, value: str) -> str:
        if value not in ALLOWED_CURRENCIES:
            raise ValueError(f"Invalid currency: {value}")
        return value
