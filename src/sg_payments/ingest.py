"""API → S3 bronze. Idempotent per (date, run_id). Contract-validated."""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
from datetime import date
from typing import Any

import boto3
import requests
from pydantic import TypeAdapter
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .config import settings
from .contracts import PaymentTxn

log = logging.getLogger(__name__)
_adapter = TypeAdapter(list[PaymentTxn])


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((requests.RequestException,)),
)
def _fetch(start: date, end: date, token: str) -> list[dict[str, Any]]:
    r = requests.get(
        settings.api_base_url,
        params={"start_date": start.isoformat(), "end_date": end.isoformat()},
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _secret(secret_id: str) -> str:
    return boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)["SecretString"]


def fetch_and_land(report_date: date, run_id: str) -> str:
    """Pull one day, validate, write gzipped JSONL + manifest to bronze. Returns S3 URI."""
    raw = _fetch(report_date, report_date, _secret(settings.api_secret_id))
    validated = _adapter.validate_python(raw)  # contract gate

    body = "\n".join(t.model_dump_json() for t in validated).encode()
    payload = gzip.compress(body)
    checksum = hashlib.sha256(body).hexdigest()

    key = f"{settings.bronze_prefix}/dt={report_date}/run_id={run_id}/payments.jsonl.gz"
    s3 = boto3.client("s3")
    s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=payload)
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=f"{key}.manifest.json",
        Body=json.dumps({"rows": len(validated), "sha256": checksum}).encode(),
    )
    log.info("landed %d rows -> s3://%s/%s", len(validated), settings.s3_bucket, key)
    return f"s3://{settings.s3_bucket}/{key}"
