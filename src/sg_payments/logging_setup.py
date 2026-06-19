"""Structured JSON logging + OpenLineage emitter. CloudWatch Insights queryable."""
from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base: dict[str, Any] = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for k in ("run_id", "dag_id", "task_id", "payment_date", "contract_version"):
            v = getattr(record, k, None)
            if v is not None:
                base[k] = v
        return json.dumps(base)


def configure(level: str = "INFO") -> None:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [h]
    root.setLevel(level)


def openlineage_conf() -> dict[str, str]:
    """Spark config to emit column-level lineage to a Marquez endpoint if set."""
    url = os.getenv("OPENLINEAGE_URL")
    if not url:
        return {}
    return {
        "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        "spark.openlineage.transport.type": "http",
        "spark.openlineage.transport.url": url,
        "spark.openlineage.namespace": "sg-payments",
    }
