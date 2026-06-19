"""Bronze JSON -> Silver Delta. Schema-enforced. MERGE upsert (handles late + status flips)."""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from .config import settings

PAYMENT_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("game", StringType(), nullable=False),
        StructField("payment_date", StringType(), nullable=False),
        StructField("price", DecimalType(18, 4), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
    ]
)


def read_bronze(spark: SparkSession, dt: str) -> DataFrame:
    path = f"s3://{settings.s3_bucket}/{settings.bronze_prefix}/dt={dt}/"
    return (
        spark.read.schema(PAYMENT_SCHEMA)
        .json(path)
        .withColumn("payment_date", F.to_date("payment_date"))
        .withColumn("ingest_ts", F.current_timestamp())
        .dropDuplicates(["transaction_id"])
    )


def upsert_silver(spark: SparkSession, df: DataFrame) -> None:
    """Idempotent MERGE on transaction_id. CDC enabled => downstream gets a change feed."""
    if not DeltaTable.isDeltaTable(spark, settings.silver_path):
        (
            df.write.format("delta")
            .partitionBy("payment_date")
            .option("delta.enableChangeDataFeed", "true")
            .save(settings.silver_path)
        )
        return

    tgt = DeltaTable.forPath(spark, settings.silver_path)
    (
        tgt.alias("t")
        .merge(df.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdate(
            condition="t.status <> s.status OR t.price <> s.price",
            set={
                "status": "s.status",
                "price": "s.price",
                "ingest_ts": "s.ingest_ts",
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def run(dt: str) -> None:
    from .spark import build_spark

    spark = build_spark("bronze_to_silver")
    df = read_bronze(spark, dt)
    upsert_silver(spark, df)
    spark.sql(f"OPTIMIZE delta.`{settings.silver_path}` ZORDER BY (game)")
