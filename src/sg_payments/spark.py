"""Spark session factory with AQE + Delta configured."""
from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark(app_name: str = "sg-payments") -> SparkSession:
    from .logging_setup import openlineage_conf

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
        .config("spark.sql.files.maxPartitionBytes", "256m")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
    )
    for k, v in openlineage_conf().items():
        builder = builder.config(k, v)
    return builder.getOrCreate()
