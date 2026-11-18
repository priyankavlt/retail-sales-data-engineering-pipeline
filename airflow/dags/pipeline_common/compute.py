"""Builds the Spark-layer tasks for the configured compute backend.

PIPELINE_COMPUTE=databricks  -> trigger the Databricks Workflows job deployed by the asset bundle
                                (databricks/databricks.yml) and wait for it without holding a
                                worker slot (deferrable operator).
PIPELINE_COMPUTE=local       -> run the same PySpark code with a local SparkSession (docker-compose).
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.models import BaseOperator

COMPUTE = os.getenv("PIPELINE_COMPUTE", "local")
DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "databricks_default")
DATABRICKS_JOBS = {
    "bronze": os.getenv("DATABRICKS_BRONZE_JOB", "walmart-bronze-ingest"),
    "silver": os.getenv("DATABRICKS_SILVER_JOB", "walmart-silver-transform"),
    "maintenance": os.getenv("DATABRICKS_MAINTENANCE_JOB", "walmart-lakehouse-maintenance"),
}


def spark_layer_task(task_id: str, layer: str, ingest_date: str | None = None, **kwargs) -> BaseOperator:
    if COMPUTE == "databricks":
        from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

        params = {"ingest_date": ingest_date} if ingest_date else {}
        return DatabricksRunNowOperator(
            task_id=task_id,
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_name=DATABRICKS_JOBS[layer],
            job_parameters=params,
            deferrable=True,
            **kwargs,
        )

    from airflow.operators.bash import BashOperator

    args = f"--layer {layer}" + (f" --ingest-date {ingest_date}" if ingest_date else "")
    return BashOperator(
        task_id=task_id,
        bash_command=f"walmart-run-layer {args} --run-id '{{{{ run_id }}}}'",
        execution_timeout=kwargs.pop("execution_timeout", timedelta(hours=1)),
        **kwargs,
    )
