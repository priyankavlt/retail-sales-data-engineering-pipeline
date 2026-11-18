"""Weekly Delta maintenance: OPTIMIZE + Z-ORDER hot silver tables and VACUUM old files."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

from pipeline_common.alerts import notify_failure
from pipeline_common.compute import spark_layer_task

with DAG(
    dag_id="walmart_lakehouse_maintenance",
    schedule="0 3 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "retail-data-engineering", "retries": 1, "retry_delay": timedelta(minutes=10),
                  "on_failure_callback": notify_failure},
    tags=["walmart", "maintenance", "delta"],
) as dag:
    spark_layer_task("optimize_and_vacuum", "maintenance")
