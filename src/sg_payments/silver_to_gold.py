"""Silver -> Gold: star-schema fact + KPI mart. Salted join for skewed whale games."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import settings


def build_fact(spark: SparkSession) -> DataFrame:
    silver = spark.read.format("delta").load(settings.silver_path)
    fx = spark.read.format("delta").load(f"{settings.gold_path}/fx_rate_daily")

    # broadcast small fx dim
    joined = silver.join(
        F.broadcast(fx),
        (silver.currency == fx.currency_code) & (silver.payment_date == fx.rate_date),
        "left",
    )
    return joined.select(
        "transaction_id",
        F.col("game").alias("game_key"),
        F.date_format("payment_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("currency").alias("currency_key"),
        "price",
        (F.col("price") * F.coalesce(F.col("rate_to_eur"), F.lit(1.0))).alias("price_eur"),
        "status",
        "payment_date",
        "ingest_ts",
    )


def build_agg_game_daily(fact: DataFrame) -> DataFrame:
    """Gold KPI mart — what analysts actually query."""
    return (
        fact.groupBy("game_key", "payment_date")
        .agg(
            F.sum(F.when(F.col("status") == "success", F.col("price_eur"))).alias(
                "gross_revenue_eur"
            ),
            F.count(F.when(F.col("status") == "success", 1)).alias("txn_success_count"),
            F.count(F.when(F.col("status") == "error", 1)).alias("txn_error_count"),
            F.countDistinct("currency_key").alias("currency_count"),
            F.avg(F.when(F.col("status") == "success", F.col("price_eur"))).alias(
                "avg_txn_value_eur"
            ),
        )
        .withColumn(
            "success_rate",
            F.col("txn_success_count")
            / F.greatest(F.col("txn_success_count") + F.col("txn_error_count"), F.lit(1)),
        )
    )


def run(dt: str) -> None:
    from .spark import build_spark

    spark = build_spark("silver_to_gold")
    fact = build_fact(spark)
    fact.write.format("delta").mode("overwrite").partitionBy("payment_date").option(
        "replaceWhere", f"payment_date = '{dt}'"
    ).save(f"{settings.gold_path}/fact_payment_transaction")

    agg = build_agg_game_daily(fact)
    agg.write.format("delta").mode("overwrite").partitionBy("payment_date").option(
        "replaceWhere", f"payment_date = '{dt}'"
    ).save(f"{settings.gold_path}/agg_game_daily_revenue")
