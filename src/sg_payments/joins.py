"""Skew-handling join helpers. Use for whale games / hot keys."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def salted_join(
    fact: DataFrame, dim: DataFrame, key: str, n_salt: int = 16
) -> DataFrame:
    """Spread a hot key across N salts so one whale doesn't straggle the stage."""
    salts = F.array([F.lit(i) for i in range(n_salt)])
    fact_s = fact.withColumn("_salt", (F.rand() * n_salt).cast("int"))
    dim_s = dim.withColumn("_salt", F.explode(salts))
    return fact_s.join(F.broadcast(dim_s), [key, "_salt"], "left").drop("_salt")
