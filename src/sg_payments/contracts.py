"""Data contract for the payment provider API. Drift => DAG fails fast."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TxnStatus(str, Enum):
    SUCCESS = "success"
    ERROR = "error"
    PENDING = "pending"
    REFUNDED = "refunded"


class PaymentTxn(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    transaction_id: str = Field(min_length=1, max_length=64)
    game: str = Field(min_length=1, max_length=128)
    payment_date: date
    price: Decimal = Field(gt=0, decimal_places=4)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    status: TxnStatus

    @field_validator("payment_date")
    @classmethod
    def _no_future(cls, v: date) -> date:
        if v > date.today():
            raise ValueError("payment_date in the future")
        return v
