"""Delta table I/O that works with Unity Catalog names (Databricks) or storage paths (local/MinIO)."""

from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from retail_pipeline.config import PipelineConfig, to_spark_uri

log = logging.getLogger(__name__)


class Lakehouse:
    def __init__(self, spark: SparkSession, cfg: PipelineConfig):
        self.spark = spark
        self.cfg = cfg
        self._schemas_ready: set[str] = set()

    # -- addressing -------------------------------------------------------------------------
    def table_name(self, layer: str, name: str) -> str:
        return f"{self.cfg.catalog}.{layer}.{name}"

    def path(self, layer: str, name: str) -> str:
        return to_spark_uri(f"{self.cfg.lakehouse_uri.rstrip('/')}/{layer}/{name}")

    def _ensure_schema(self, layer: str) -> None:
        if self.cfg.uses_catalog and layer not in self._schemas_ready:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.cfg.catalog}.{layer}")
            self._schemas_ready.add(layer)

    def exists(self, layer: str, name: str) -> bool:
        if self.cfg.uses_catalog:
            return self.spark.catalog.tableExists(self.table_name(layer, name))
        return DeltaTable.isDeltaTable(self.spark, self.path(layer, name))

    def delta_table(self, layer: str, name: str) -> DeltaTable:
        if self.cfg.uses_catalog:
            return DeltaTable.forName(self.spark, self.table_name(layer, name))
        return DeltaTable.forPath(self.spark, self.path(layer, name))

    # -- reads ------------------------------------------------------------------------------
    def read(self, layer: str, name: str) -> DataFrame:
        if self.cfg.uses_catalog:
            return self.spark.table(self.table_name(layer, name))
        return self.spark.read.format("delta").load(self.path(layer, name))

    # -- writes -----------------------------------------------------------------------------
    def _save(self, writer, layer: str, name: str) -> None:
        self._ensure_schema(layer)
        if self.cfg.uses_catalog:
            writer.saveAsTable(self.table_name(layer, name))
        else:
            writer.save(self.path(layer, name))

    def overwrite_partition(self, df: DataFrame, layer: str, name: str, column: str, value: str) -> None:
        """Idempotent write of one partition (re-running a batch replaces, never duplicates)."""
        writer = (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy(column)
            .option("replaceWhere", f"{column} = '{value}'")
            .option("mergeSchema", "true")
        )
        self._save(writer, layer, name)

    def append(self, df: DataFrame, layer: str, name: str) -> None:
        self._save(df.write.format("delta").mode("append").option("mergeSchema", "true"), layer, name)

    def upsert(self, df: DataFrame, layer: str, name: str, keys: list[str], order_col: str) -> None:
        """MERGE on natural keys. A row only overwrites the target if it is at least as recent,
        so replaying an old batch (backfill) can never clobber newer data."""
        if not self.exists(layer, name):
            log.info("creating %s.%s", layer, name)
            self._save(df.write.format("delta").mode("overwrite"), layer, name)
            return
        on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (
            self.delta_table(layer, name)
            .alias("t")
            .merge(df.alias("s"), on)
            .whenMatchedUpdateAll(condition=f"s.{order_col} >= t.{order_col}")
            .whenNotMatchedInsertAll()
            .execute()
        )
