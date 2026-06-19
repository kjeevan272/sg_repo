"""Data quality gates. Raises on failure -> Airflow task fails -> no bad data downstream."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_STATUS = {"success", "error", "pending", "refunded"}


class DQError(RuntimeError):
    pass


def assert_silver(df: DataFrame) -> None:
    checks: dict[str, int] = {
        "null_transaction_id": df.filter(F.col("transaction_id").isNull()).count(),
        "duplicate_transaction_id": df.groupBy("transaction_id")
        .count()
        .filter("count > 1")
        .count(),
        "non_positive_price": df.filter(F.col("price") <= 0).count(),
        "invalid_status": df.filter(~F.col("status").isin(*VALID_STATUS)).count(),
        "future_date": df.filter(F.col("payment_date") > F.current_date()).count(),
    }
    failed = {k: v for k, v in checks.items() if v}
    if failed:
        raise DQError(f"DQ failed: {failed}")
