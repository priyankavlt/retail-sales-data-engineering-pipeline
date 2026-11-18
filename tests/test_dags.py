"""DAG integrity: imports cleanly, expected tasks and dependencies (runs where Airflow is installed)."""

from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_pipeline_structure(dagbag):
    dag = dagbag.get_dag("walmart_sales_pipeline")
    assert dag is not None
    assert dag.max_active_runs == 1
    order = ["land_raw_files", "bronze_ingest", "silver_transform", "gold_dbt.source_freshness", "gold_dbt.dbt_build"]
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert downstream in dag.get_task(upstream).downstream_task_ids
    for task in dag.tasks:
        assert task.retries >= 0
