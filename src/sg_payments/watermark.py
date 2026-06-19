"""Idempotent ingestion state. (date, checksum) already landed? skip the API call."""
from __future__ import annotations

from datetime import date, datetime

from delta.tables import DeltaTable
from pyspark.sql import Row, SparkSession

from .config import settings

STATE_PATH = f"{settings.gold_path}/_ingestion_state"


def already_landed(spark: SparkSession, report_date: date, sha256: str) -> bool:
    if not DeltaTable.isDeltaTable(spark, STATE_PATH):
        return False
    return (
        spark.read.format("delta")
        .load(STATE_PATH)
        .filter(f"report_date = '{report_date}' AND sha256 = '{sha256}' AND status = 'OK'")
        .limit(1)
        .count()
        > 0
    )


def record(
    spark: SparkSession, report_date: date, run_id: str, rows: int, sha256: str, status: str
) -> None:
    df = spark.createDataFrame(
        [Row(report_date=report_date, run_id=run_id, rows=rows, sha256=sha256,
             status=status, recorded_at=datetime.utcnow())]
    )
    df.write.format("delta").mode("append").save(STATE_PATH)
