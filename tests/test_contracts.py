from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from sg_payments.contracts import PaymentTxn, TxnStatus


def _ok(**over):
    return {
        "transaction_id": "tx1",
        "game": "funny-game",
        "payment_date": "2025-01-03",
        "price": "2.10",
        "currency": "EUR",
        "status": "success",
    } | over


def test_valid():
    t = PaymentTxn(**_ok())
    assert t.status is TxnStatus.SUCCESS
    assert t.price == Decimal("2.10")


@pytest.mark.parametrize(
    "patch",
    [
        {"price": "0"},
        {"price": "-1"},
        {"currency": "Eur"},
        {"status": "unknown"},
        {"payment_date": "9999-01-01"},
        {"extra_field": "boom"},
    ],
)
def test_drift_rejected(patch):
    with pytest.raises(ValidationError):
        PaymentTxn(**_ok(**patch))


def test_future_date_rejected():
    with pytest.raises(ValidationError):
        PaymentTxn(**_ok(payment_date=date.today().replace(year=date.today().year + 1)))
