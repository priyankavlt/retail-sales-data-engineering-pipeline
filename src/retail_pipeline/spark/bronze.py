"""Bronze: raw CSV in the S3 raw zone -> append-only Delta tables with lineage metadata.

Bronze keeps the source exactly as delivered (every column as string, ``NA`` and malformed
values intact) so silver can always be rebuilt from it. Each batch is written to its own
``_ingest_date`` partition with ``replaceWhere``, making reruns idempotent.
"""

from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_pipeline.config import PipelineConfig, to_spark_uri
from retail_pipeline.ingestion.landing import EXPECTED_HEADERS
from retail_pipeline.spark.lakehouse import Lakehouse

log = logging.getLogger(__name__)

BRONZE_TABLES = {"stores": "walmart_stores", "features": "walmart_features", "sales": "walmart_sales"}


class SchemaDriftError(ValueError):
    pass


def read_raw_batch(spark: SparkSession, cfg: PipelineConfig, dataset: str, ingest_date: date) -> DataFrame:
    src = to_spark_uri(f"{cfg.raw_uri}/{dataset}/ingest_date={ingest_date.isoformat()}/*.csv")
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)  # everything stays a string in bronze
        .option("mode", "PERMISSIVE")
        .option("enforceSchema", False)
        .csv(src)
        .select("*", F.col("_metadata.file_path").alias("_source_file"))
    )


def to_bronze(raw: DataFrame, dataset: str, ingest_date: date) -> DataFrame:
    source_cols = [c for c in raw.columns if c != "_source_file"]
    missing = [c for c in EXPECTED_HEADERS[dataset] if c not in source_cols]
    if missing:
        raise SchemaDriftError(f"{dataset}: required columns missing from raw batch: {missing}")
    return raw.select(
        *[F.col(f"`{c}`") for c in source_cols],
        F.lit(ingest_date).cast("date").alias("_ingest_date"),
        F.current_timestamp().alias("_ingested_at"),
        "_source_file",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(f"`{c}`"), F.lit("")) for c in source_cols]), 256).alias("_row_hash"),
    )


def run_bronze(spark: SparkSession, cfg: PipelineConfig, dataset: str, ingest_date: date) -> int:
    lake = Lakehouse(spark, cfg)
    bronze = to_bronze(read_raw_batch(spark, cfg, dataset, ingest_date), dataset, ingest_date)
    lake.overwrite_partition(bronze, "bronze", BRONZE_TABLES[dataset], "_ingest_date", ingest_date.isoformat())
    rows = lake.read("bronze", BRONZE_TABLES[dataset]).where(F.col("_ingest_date") == F.lit(ingest_date)).count()
    log.info("bronze.%s: %d rows for ingest_date=%s", BRONZE_TABLES[dataset], rows, ingest_date)
    return rows
