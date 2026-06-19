"""Lambda entrypoint for the lean tier. Reuses the same ingestion logic."""
from __future__ import annotations

from datetime import date
from typing import Any

from sg_payments.ingest import fetch_and_land


def handler(event: dict[str, Any], _ctx: object) -> dict[str, str]:
    dt = date.fromisoformat(event["report_date"])
    uri = fetch_and_land(dt, run_id=event.get("run_id", "lambda"))
    return {"landed": uri}
