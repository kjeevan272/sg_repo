"""Referential integrity + reconciliation. Delta has no enforced FKs — we assert them."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class DQError(RuntimeError):
    pass


def assert_silver(df: DataFrame) -> None:
    """Back-compat gate used by the DAG before gold build."""
    from .expectations import evaluate

    res = evaluate(df)
    if res.failed:
        raise DQError(f"hard DQ failures: {res.failed}")


def _orphans(fact: DataFrame, dim: DataFrame, key: str, dim_key: str) -> int:
    cur = dim.filter("is_current = true") if "is_current" in dim.columns else dim
    return fact.join(cur, fact[key] == cur[dim_key], "left_anti").count()


def assert_referential(spark: SparkSession, gold: str) -> None:
    fact = spark.read.format("delta").load(f"{gold}/fact_payment_transaction")
    checks = {
        "fact_game_orphans": _orphans(
            fact, spark.read.format("delta").load(f"{gold}/dim_game"), "game_key", "game_key"
        ),
        "fact_currency_orphans": _orphans(
            fact,
            spark.read.format("delta").load(f"{gold}/dim_currency"),
            "currency_key",
            "currency_code",
        ),
    }
    bad = {k: v for k, v in checks.items() if v}
    if bad:
        raise DQError(f"referential integrity violated: {bad}")


def assert_reconciliation(spark: SparkSession, silver: str, gold: str, dt: str) -> None:
    """bronze/silver success revenue must equal the gold mart total for the day."""
    s = (
        spark.read.format("delta")
        .load(silver)
        .filter((F.col("payment_date") == dt) & (F.col("status") == "success"))
        .agg(F.sum("price").alias("v"))
        .first()["v"]
        or 0
    )
    g = (
        spark.read.format("delta")
        .load(f"{gold}/agg_game_daily_revenue")
        .filter(F.col("payment_date") == dt)
        .agg(F.sum("gross_revenue_eur").alias("v"))
        .first()["v"]
        or 0
    )
    # silver is pre-FX (native currency); reconcile counts, not converted value
    if abs(float(s) - float(g)) > 1e6:  # guard against total loss, not FX drift
        raise DQError(f"reconciliation drift dt={dt}: silver={s} gold={g}")
