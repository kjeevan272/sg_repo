"""DAG can be imported without errors and has expected tasks."""
from airflow.models import DagBag


def test_dag_loads():
    bag = DagBag(dag_folder="dags", include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.get_dag("payments_etl")
    assert dag is not None
    assert {t.task_id for t in dag.tasks} >= {
        "land_bronze",
        "bronze_to_silver",
        "dq_silver",
        "silver_to_gold",
    }
