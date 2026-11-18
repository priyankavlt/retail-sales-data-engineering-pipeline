"""Silver: typed, validated, de-duplicated, conformed Delta tables.

Per dataset:
1. cast raw strings to proper types (ANSI-safe ``try_cast``; ``NA`` -> NULL)
2. evaluate data-quality rules; rows failing ``error`` rules go to ``silver.quarantine_<dataset>``
3. de-duplicate on the natural key, keeping the most recently ingested version
4. MERGE (upsert) into the silver table, so late/corrected data replaces earlier values
5. persist rule metrics to ``ops.dq_results`` and trip a circuit breaker if too much is rejected
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from retail_pipeline.config import PipelineConfig
from retail_pipeline.spark.bronze import BRONZE_TABLES
from retail_pipeline.spark.lakehouse import Lakehouse
from retail_pipeline.spark.quality import QualityResult, Rule, evaluate, metrics_frame

log = logging.getLogger(__name__)

LINEAGE_COLS = ["_ingest_date", "_ingested_at", "_source_file", "_row_hash"]


class DataQualityError(RuntimeError):
    pass


def _num(col: str, sql_type: str):
    return F.expr(f"try_cast(nullif(nullif(trim(`{col}`), 'NA'), '') as {sql_type})")


def _date(col: str):
    return F.expr(f"try_cast(trim(`{col}`) as date)")


def _bool(col: str):
    return F.expr(
        f"case upper(trim(`{col}`)) when 'TRUE' then true when 'FALSE' then false end"
    )


@dataclass(frozen=True)
class SilverSpec:
    dataset: str
    table: str
    keys: list[str]
    rules: list[Rule]

    def conform(self, bronze: DataFrame) -> DataFrame:
        return CONFORMERS[self.dataset](bronze)


def conform_stores(df: DataFrame) -> DataFrame:
    return df.select(
        _num("Store", "int").alias("store_id"),
        F.upper(F.trim("Type")).alias("store_type"),
        _num("Size", "int").alias("store_size_sqft"),
        *LINEAGE_COLS,
    )


def conform_features(df: DataFrame) -> DataFrame:
    return df.select(
        _num("Store", "int").alias("store_id"),
        _date("Date").alias("week_date"),
        _num("Temperature", "double").alias("temperature_f"),
        _num("Fuel_Price", "double").alias("fuel_price"),
        *[_num(f"MarkDown{i}", "decimal(14,2)").alias(f"markdown_{i}") for i in range(1, 6)],
        _num("CPI", "double").alias("cpi"),
        _num("Unemployment", "double").alias("unemployment_rate"),
        _bool("IsHoliday").alias("is_holiday"),
        *LINEAGE_COLS,
    )


def conform_sales(df: DataFrame) -> DataFrame:
    return df.select(
        _num("Store", "int").alias("store_id"),
        _num("Dept", "int").alias("dept_id"),
        _date("Date").alias("week_date"),
        _num("Weekly_Sales", "decimal(14,2)").alias("weekly_sales"),
        _bool("IsHoliday").alias("is_holiday"),
        *LINEAGE_COLS,
    )


CONFORMERS = {"stores": conform_stores, "features": conform_features, "sales": conform_sales}

_FRIDAY = "week_date is null or dayofweek(week_date) = 6"

SPECS = {
    "stores": SilverSpec(
        "stores",
        "stores",
        ["store_id"],
        [
            Rule("store_id_valid", "store_id is not null and store_id > 0"),
            Rule("store_type_valid", "store_type in ('A', 'B', 'C')"),
            Rule("store_size_positive", "store_size_sqft > 0"),
        ],
    ),
    "features": SilverSpec(
        "features",
        "store_features",
        ["store_id", "week_date"],
        [
            Rule("store_id_valid", "store_id is not null and store_id > 0"),
            Rule("week_date_valid", "week_date is not null"),
            Rule("fuel_price_positive", "fuel_price is null or fuel_price > 0"),
            Rule("is_holiday_valid", "is_holiday is not null"),
            Rule("week_ends_friday", _FRIDAY, "warn"),
            Rule("temperature_plausible", "temperature_f is null or temperature_f between -60 and 130", "warn"),
            Rule("unemployment_plausible", "unemployment_rate is null or unemployment_rate between 0 and 50", "warn"),
        ],
    ),
    "sales": SilverSpec(
        "sales",
        "weekly_sales",
        ["store_id", "dept_id", "week_date"],
        [
            Rule("store_id_valid", "store_id is not null and store_id > 0"),
            Rule("dept_id_valid", "dept_id is not null and dept_id between 1 and 99"),
            Rule("week_date_valid", "week_date is not null"),
            Rule("weekly_sales_numeric", "weekly_sales is not null"),
            Rule("is_holiday_valid", "is_holiday is not null"),
            Rule("week_ends_friday", _FRIDAY, "warn"),
            Rule("weekly_sales_plausible", "weekly_sales is null or abs(weekly_sales) < 1000000", "warn"),
        ],
    ),
}


def deduplicate(df: DataFrame, keys: list[str]) -> DataFrame:
    """Keep one row per natural key: latest ingestion wins, ties broken deterministically."""
    w = Window.partitionBy(*keys).orderBy(
        F.col("_ingested_at").desc(), F.col("_ingest_date").desc(), F.col("_row_hash")
    )
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


@dataclass
class SilverOutcome:
    dataset: str
    total_rows: int
    quarantined_rows: int
    upserted_rows: int
    failed_by_rule: dict[str, int]


def build_silver(bronze: DataFrame, spec: SilverSpec) -> tuple[QualityResult, DataFrame]:
    result = evaluate(spec.conform(bronze), spec.rules)
    clean = deduplicate(result.valid, spec.keys).withColumn("_silver_updated_at", F.current_timestamp())
    return result, clean


def run_silver(
    spark: SparkSession, cfg: PipelineConfig, dataset: str, ingest_date: date | None, run_id: str | None = None
) -> SilverOutcome:
    """Process one bronze batch (``ingest_date``) or, when ``None``, rebuild from all of bronze."""
    lake = Lakehouse(spark, cfg)
    spec = SPECS[dataset]
    run_id = run_id or str(uuid.uuid4())
    bronze = lake.read("bronze", BRONZE_TABLES[dataset])
    if ingest_date is not None:
        bronze = bronze.where(F.col("_ingest_date") == F.lit(ingest_date))

    result, clean = build_silver(bronze, spec)
    if result.total_rows == 0:
        raise DataQualityError(f"bronze.{BRONZE_TABLES[dataset]} has no rows for ingest_date={ingest_date}")

    batch = ingest_date.isoformat() if ingest_date else None
    lake.append(metrics_frame(spark, result, run_id=run_id, dataset=dataset, ingest_date=batch), "ops", "dq_results")
    if result.quarantined_rows:
        quarantine = result.quarantined.withColumn("_quarantined_at", F.current_timestamp())
        if batch:
            lake.overwrite_partition(quarantine, "silver", f"quarantine_{dataset}", "_ingest_date", batch)
        else:
            lake.append(quarantine, "silver", f"quarantine_{dataset}")

    if result.quarantine_ratio > cfg.max_quarantine_ratio:
        raise DataQualityError(
            f"{dataset}: {result.quarantined_rows}/{result.total_rows} rows "
            f"({result.quarantine_ratio:.1%}) failed error rules, above the "
            f"{cfg.max_quarantine_ratio:.0%} threshold. Failures: {result.failed_by_rule}"
        )

    upserted = clean.count()
    lake.upsert(clean, "silver", spec.table, spec.keys, order_col="_ingested_at")
    result.release()
    log.info(
        "silver.%s: %d in, %d quarantined, %d upserted; rule failures %s",
        spec.table, result.total_rows, result.quarantined_rows, upserted, result.failed_by_rule,
    )
    return SilverOutcome(dataset, result.total_rows, result.quarantined_rows, upserted, result.failed_by_rule)
