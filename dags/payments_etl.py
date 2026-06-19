"""Airflow DAG: fetch -> bronze -> silver MERGE -> DQ -> gold mart.

One DAG serves both incremental (daily) and full backfill (catchup=True).
Idempotent per logical_date via S3 key, watermark table, and Delta MERGE.
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


def _on_failure(context):
    from sg_payments.alerts import slack_failure

    slack_failure(context)


@dag(
    dag_id="payments_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=4,
    default_args=DEFAULT_ARGS | {"on_failure_callback": _on_failure},
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
    def silver_to_gold(dt: str) -> str:
        from sg_payments import silver_to_gold as s2g

        s2g.run(dt)
        return dt

    @task
    def referential_checks(dt: str) -> str:
        """FK orphans + silver<->gold reconciliation. Fails the run on violation."""
        from sg_payments.config import settings
        from sg_payments.dq.referential import assert_reconciliation, assert_referential
        from sg_payments.spark import build_spark

        spark = build_spark("ri")
        assert_referential(spark, settings.gold_path)
        assert_reconciliation(spark, settings.silver_path, settings.gold_path, dt)
        return dt

    @task
    def replay_quarantine(dt: str) -> None:
        """Re-attempt due rows from the retry queue; exhausted -> dead_letter."""
        from sg_payments.retry_queue import due, reschedule
        from sg_payments.spark import build_spark

        spark = build_spark("replay")
        pending = due(spark)
        if pending.head(1):
            reschedule(spark, pending)

    @task
    def restate_window(dt: str, days: int = 7) -> None:
        """Re-process the last N days to absorb late-arriving status flips."""
        from datetime import date, timedelta

        from sg_payments import silver_to_gold as s2g

        d = date.fromisoformat(dt)
        for i in range(1, days + 1):
            s2g.run((d - timedelta(days=i)).isoformat())

    dt = "{{ ds }}"
    gold = silver_to_gold(dq_silver(bronze_to_silver(land_bronze(dt, "{{ run_id }}"))))
    checked = referential_checks(gold)
    checked >> [restate_window(dt), replay_quarantine(dt)]


dag = payments_etl()
