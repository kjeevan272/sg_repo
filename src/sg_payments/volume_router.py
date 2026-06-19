"""Pick the runtime tier from yesterday's row count. See ADR 0004."""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import settings

SPARK_THRESHOLD = 1_000_000


def choose_tier(spark: SparkSession, dt: str) -> str:
    try:
        n = (
            spark.read.format("delta")
            .load(settings.silver_path)
            .filter(F.col("payment_date") == dt)
            .count()
        )
    except Exception:
        n = 0
    return "spark" if n >= SPARK_THRESHOLD else "lean"
