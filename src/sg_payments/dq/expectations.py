"""Great Expectations-style declarative suite. Severities: fail | quarantine | warn."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_STATUS = ("success", "error", "pending", "refunded")


class Severity(str, Enum):
    FAIL = "fail"
    QUARANTINE = "quarantine"
    WARN = "warn"


@dataclass(frozen=True)
class Expectation:
    name: str
    predicate: str  # SQL boolean: TRUE for a *good* row
    severity: Severity


SUITE: tuple[Expectation, ...] = (
    Expectation("transaction_id_not_null", "transaction_id IS NOT NULL", Severity.FAIL),
    Expectation("price_positive", "price > 0", Severity.QUARANTINE),
    Expectation("status_in_enum", f"status IN {VALID_STATUS}", Severity.QUARANTINE),
    Expectation("currency_iso", "currency RLIKE '^[A-Z]{3}$'", Severity.QUARANTINE),
    Expectation("payment_date_not_future", "payment_date <= current_date()", Severity.FAIL),
    Expectation("price_sane_upper", "price < 100000", Severity.WARN),
)


@dataclass(frozen=True)
class SuiteResult:
    failed: dict[str, int]
    quarantined: dict[str, int]
    warned: dict[str, int]
    bad_ids: list[str]


def evaluate(df: DataFrame) -> SuiteResult:
    failed, quarantined, warned, bad_ids = {}, {}, {}, []
    for exp in SUITE:
        bad = df.filter(f"NOT ({exp.predicate})")
        n = bad.count()
        if not n:
            continue
        bucket = {Severity.FAIL: failed, Severity.QUARANTINE: quarantined, Severity.WARN: warned}[
            exp.severity
        ]
        bucket[exp.name] = n
        if exp.severity is not Severity.WARN:
            bad_ids += [r[0] for r in bad.select("transaction_id").limit(1000).collect()]
    return SuiteResult(failed, quarantined, warned, sorted(set(bad_ids)))
