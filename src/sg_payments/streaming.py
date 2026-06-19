"""Streaming seam: same bronze->silver logic via Trigger.AvailableNow.

Batch semantics today; flip to a continuous trigger when the provider adds a
webhook/Kafka feed — no rewrite of the MERGE logic.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from .bronze_to_silver import PAYMENT_SCHEMA, upsert_silver
from .config import settings


def run_stream(spark: SparkSession) -> None:
    src = f"s3://{settings.s3_bucket}/{settings.bronze_prefix}/"
    stream = (
        spark.readStream.schema(PAYMENT_SCHEMA)
        .option("cleanSource", "archive")
        .json(src)
    )
    (
        stream.writeStream.foreachBatch(lambda df, _id: upsert_silver(spark, df))
        .option("checkpointLocation", f"{settings.silver_path}/_checkpoints")
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
