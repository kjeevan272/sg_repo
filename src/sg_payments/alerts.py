"""Airflow failure callback -> Slack. Hook into on_failure_callback."""
from __future__ import annotations

import json
import os
import urllib.request
from typing import Any

_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")


def slack_failure(context: dict[str, Any]) -> None:
    if not _WEBHOOK:
        return
    ti = context["task_instance"]
    msg = {
        "text": (
            f":rotating_light: *{ti.dag_id}.{ti.task_id}* failed\n"
            f"run_id: `{context['run_id']}`  ds: `{context['ds']}`\n"
            f"<{ti.log_url}|logs>"
        )
    }
    req = urllib.request.Request(
        _WEBHOOK, data=json.dumps(msg).encode(), headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req, timeout=5)
