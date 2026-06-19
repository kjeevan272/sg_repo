"""Airflow DAG: fetch → land bronze → silver MERGE → DQ → gold mart.

One DAG serves both incremental (daily) and full backfill (catchup=True).
Idempotent per logical_date via S3 key & Delta MERGE.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),
}


@dag(
    dag_id="payments_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=4,
    default_args=DEFAULT_ARGS,
    tags=["payments", "medallion", "delta"],
)
def payments_etl() -> None:
    @task
    def land_bronze(logical_date: str, run_id: str) -> str:
        from datetime import date

        from sg_payments.ingest import fetch_and_land

        return fetch_and_land(date.fromisoformat(logical_date[:10]), run_id)

    @task
    def bronze_to_silver(dt: str) -> str:
        from sg_payments import bronze_to_silver as b2s

        b2s.run(dt)
        return dt

    @task
    def dq_silver(dt: str) -> str:
        from sg_payments.config import settings
        from sg_payments.dq import assert_silver
        from sg_payments.spark import build_spark

        spark = build_spark("dq")
        df = (
            spark.read.format("delta")
            .load(settings.silver_path)
            .filter(f"payment_date = '{dt}'")
        )
        assert_silver(df)
        return dt

    @task
    def silver_to_gold(dt: str) -> None:
        from sg_payments import silver_to_gold as s2g

        s2g.run(dt)

    dt = "{{ ds }}"
    silver_to_gold(dq_silver(bronze_to_silver(land_bronze(dt, "{{ run_id }}"))))


dag = payments_etl()
