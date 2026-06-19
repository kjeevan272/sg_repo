"""Lean tier: DuckDB transform for sub-GB days. Runs in a Lambda/Glue-PythonShell.

JSON (bronze) -> Parquet (silver) without spinning Spark. Costs cents/day.
"""
from __future__ import annotations

import os

import duckdb

BUCKET = os.getenv("S3_BUCKET", "softgames-payments")


def transform(dt: str) -> None:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"""
        COPY (
            SELECT DISTINCT ON (transaction_id)
                transaction_id, game, CAST(payment_date AS DATE) AS payment_date,
                CAST(price AS DECIMAL(18,4)) AS price, currency, status
            FROM read_json_auto('s3://{BUCKET}/bronze/raw/dt={dt}/**/*.jsonl.gz')
        )
        TO 's3://{BUCKET}/silver/payments/payment_date={dt}/data.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
