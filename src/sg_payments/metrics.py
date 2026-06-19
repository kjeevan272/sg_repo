"""Per-task row accounting. Detects 'pipeline succeeded but rows not processed'."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime

from pyspark.sql import SparkSession

from .config import settings

METRICS_PATH = f"{settings.gold_path}/_pipeline_metrics"


@dataclass(frozen=True)
class TaskMetrics:
    dag_id: str
    task_id: str
    run_id: str
    payment_date: str
    input_rows: int
    output_rows: int
    rejected_rows: int
    quarantined_rows: int
    recorded_at: str = ""

    def balanced(self) -> bool:
        return self.input_rows == self.output_rows + self.rejected_rows + self.quarantined_rows


def emit(spark: SparkSession, m: TaskMetrics) -> None:
    row = asdict(m) | {"recorded_at": datetime.utcnow().isoformat()}
    spark.createDataFrame([row]).write.format("delta").mode("append").save(METRICS_PATH)
    if not m.balanced():
        raise RuntimeError(
            f"row leak {m.task_id} dt={m.payment_date}: "
            f"in={m.input_rows} out={m.output_rows} "
            f"rej={m.rejected_rows} quar={m.quarantined_rows}"
        )
