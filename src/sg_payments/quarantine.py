"""Bad rows -> quarantine S3 prefix with the validation error. DAG keeps moving."""
from __future__ import annotations

import json
from datetime import date
from typing import Any

import boto3
from pydantic import TypeAdapter, ValidationError

from .config import settings
from .contracts import PaymentTxn

_adapter = TypeAdapter(PaymentTxn)


def partition(raw: list[dict[str, Any]], report_date: date, run_id: str) -> list[PaymentTxn]:
    good, bad = [], []
    for row in raw:
        try:
            good.append(_adapter.validate_python(row))
        except ValidationError as e:
            bad.append({"row": row, "errors": e.errors()})
    if bad:
        key = f"quarantine/dt={report_date}/run_id={run_id}/bad.jsonl"
        body = "\n".join(json.dumps(b, default=str) for b in bad).encode()
        boto3.client("s3").put_object(Bucket=settings.s3_bucket, Key=key, Body=body)
    return good
