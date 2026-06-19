"""Row-level retry. Failed rows re-attempt with backoff; terminal -> dead_letter."""
from __future__ import annotations

from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import settings

QUEUE_PATH = f"{settings.gold_path}/_retry_queue"
DLQ_PATH = f"s3://{settings.s3_bucket}/dead_letter"
MAX_ATTEMPTS = 5


def enqueue(df: DataFrame, error: str) -> None:
    (
        df.withColumn("attempt_count", F.lit(1))
        .withColumn("last_error", F.lit(error))
        .withColumn("eligible_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .save(QUEUE_PATH)
    )


def due(spark: SparkSession) -> DataFrame:
    if not DeltaTable.isDeltaTable(spark, QUEUE_PATH):
        return spark.createDataFrame([], schema="transaction_id string")
    return (
        spark.read.format("delta")
        .load(QUEUE_PATH)
        .filter(
            (F.col("eligible_at") <= F.current_timestamp())
            & (F.col("attempt_count") < MAX_ATTEMPTS)
        )
    )


def reschedule(spark: SparkSession, failed: DataFrame) -> None:
    """Exponential backoff; move exhausted rows to the dead-letter prefix."""
    bumped = failed.withColumn("attempt_count", F.col("attempt_count") + 1).withColumn(
        "eligible_at",
        F.current_timestamp()
        + F.expr("make_interval(0,0,0,0, cast(pow(2, attempt_count) as int),0,0)"),
    )
    dead = bumped.filter(F.col("attempt_count") >= MAX_ATTEMPTS)
    if dead.head(1):
        dead.write.format("parquet").mode("append").save(
            f"{DLQ_PATH}/dt={datetime.utcnow():%Y-%m-%d}"
        )
    bumped.filter(F.col("attempt_count") < MAX_ATTEMPTS).write.format("delta").mode(
        "overwrite"
    ).save(QUEUE_PATH)
