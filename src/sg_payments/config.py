"""Runtime config — read from env / Airflow Variables / Secrets Manager."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    api_base_url: str = os.getenv("PAY_API_URL", "https://payment-provider.com/reports")
    s3_bucket: str = os.getenv("S3_BUCKET", "softgames-payments")
    bronze_prefix: str = "bronze/raw"
    silver_path: str = os.getenv("SILVER_PATH", "s3://softgames-payments/silver/payments")
    gold_path: str = os.getenv("GOLD_PATH", "s3://softgames-payments/gold")
    reporting_currency: str = "EUR"
    api_secret_id: str = os.getenv("PAY_API_SECRET_ID", "softgames/payment-provider")


settings = Settings()
