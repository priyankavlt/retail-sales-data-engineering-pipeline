"""
Walmart retail sales pipeline (weekly).

    wait for source files in S3 ─► land + validate raw ─► bronze (Delta) ─► silver (Delta, DQ)
        ─► dbt source freshness ─► dbt build (snapshot, gold marts, tests) ─► run summary

* Every step is idempotent for a given ingest date, so retries, reruns and backfills are safe.
* Spark layers run on Databricks (PIPELINE_COMPUTE=databricks) or locally in docker-compose.
* dbt runs against Databricks SQL (DBT_TARGET=databricks) or DuckDB (DBT_TARGET=docker/local).
* Failures page via Amazon SNS; the gold dataset is published for downstream, data-aware DAGs.

Trigger a specific batch manually with config {"ingest_date": "2012-10-27"}.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup

from pipeline_common.alerts import notify_failure, notify_summary
from pipeline_common.compute import spark_layer_task

DATASETS = ("stores", "features", "sales")
INCOMING_URI = os.getenv("INCOMING_URI", "s3://walmart-retail-landing/incoming")
DBT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_TARGET = os.getenv("DBT_TARGET", "docker")
GOLD_DATASET = Dataset(os.getenv("GOLD_DATASET_URI", "walmart://gold/fct_weekly_sales"))

INGEST_DATE = "{{ params.ingest_date or (data_interval_end | ds) }}"

default_args = {
    "owner": "retail-data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="walmart_sales_pipeline",
    description="S3 -> Bronze -> Silver (PySpark/Delta) -> Gold (dbt) for Walmart weekly sales",
    doc_md=__doc__,
    schedule="0 6 * * 6",  # Saturdays 06:00 UTC, after the Friday week closes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    default_args=default_args,
    params={"ingest_date": Param(None, type=["null", "string"], format="date", description="Override batch date")},
    tags=["walmart", "retail", "lakehouse", "dbt", "databricks"],
) as dag:
    with TaskGroup("wait_for_source_files") as wait_for_files:
        for dataset in DATASETS:
            S3KeySensor(
                task_id=f"wait_for_{dataset}",
                bucket_key=f"{INCOMING_URI}/walmart/{dataset}/{INGEST_DATE}/*.csv",
                wildcard_match=True,
                aws_conn_id=os.getenv("AWS_CONN_ID", "aws_default"),
                mode="reschedule",
                poke_interval=300,
                timeout=6 * 60 * 60,
                retries=0,
            )

    @task(execution_timeout=timedelta(minutes=30))
    def land_raw_files(ingest_date: str) -> dict:
        """Validate headers/row counts, copy to the immutable raw zone, write manifests."""
        from datetime import date

        from retail_pipeline.config import PipelineConfig
        from retail_pipeline.ingestion.landing import land_dataset

        cfg = PipelineConfig.from_env()
        manifests = {ds: land_dataset(cfg, ds, date.fromisoformat(ingest_date)) for ds in DATASETS}
        return {ds: m["row_count"] for ds, m in manifests.items()}

    landed = land_raw_files(INGEST_DATE)

    bronze = spark_layer_task("bronze_ingest", "bronze", INGEST_DATE)
    silver = spark_layer_task("silver_transform", "silver", INGEST_DATE)

    dbt_env = {"DBT_PROFILES_DIR": DBT_DIR, "DBT_TARGET": DBT_TARGET}
    with TaskGroup("gold_dbt") as gold:
        freshness = BashOperator(
            task_id="source_freshness",
            bash_command=f"cd {DBT_DIR} && dbt source freshness --target {DBT_TARGET}",
            env=dbt_env,
            append_env=True,
            retries=0,
        )
        build = BashOperator(
            task_id="dbt_build",
            # Seeds, snapshot (SCD2), models and tests in dependency order; a failing test
            # stops downstream models from being published.
            bash_command=f"cd {DBT_DIR} && dbt build --target {DBT_TARGET} --fail-fast",
            env=dbt_env,
            append_env=True,
            outlets=[GOLD_DATASET],
            execution_timeout=timedelta(hours=1),
        )
        freshness >> build

    @task(trigger_rule="all_success")
    def publish_run_summary(row_counts: dict, ingest_date: str, **context) -> None:
        lines = [f"Batch {ingest_date} published to gold.", "Rows landed:"]
        lines += [f"  {ds:<9} {count:>10,}" for ds, count in row_counts.items()]
        lines.append(f"Run: {context['run_id']}")
        notify_summary(f"[OK] walmart_sales_pipeline {ingest_date}", "\n".join(lines))

    wait_for_files >> landed >> bronze >> silver >> gold
    gold >> publish_run_summary(landed, INGEST_DATE)
