"""Single entrypoint for every Spark layer.

Used identically by Databricks (``python_wheel_task`` in ``databricks/databricks.yml``),
by Airflow in local/docker mode, and from the command line:

    walmart-run-layer --layer bronze --ingest-date 2012-10-27
    walmart-run-layer --layer silver --dataset sales --ingest-date 2012-10-27
    walmart-run-layer --layer silver --full-refresh        # rebuild silver from all of bronze
    walmart-run-layer --layer maintenance                  # OPTIMIZE + VACUUM
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

from retail_pipeline.config import DATASETS, PipelineConfig

log = logging.getLogger("retail_pipeline.jobs")

MAINTAINED_TABLES = {
    ("silver", "weekly_sales"): "store_id, week_date",
    ("silver", "store_features"): "store_id, week_date",
}


def run_maintenance(spark, cfg: PipelineConfig) -> None:
    from retail_pipeline.spark.lakehouse import Lakehouse

    lake = Lakehouse(spark, cfg)
    for (layer, name), zorder in MAINTAINED_TABLES.items():
        if not lake.exists(layer, name):
            continue
        target = lake.table_name(layer, name) if cfg.uses_catalog else f"delta.`{lake.path(layer, name)}`"
        spark.sql(f"OPTIMIZE {target} ZORDER BY ({zorder})")
        spark.sql(f"VACUUM {target} RETAIN 168 HOURS")
        log.info("optimized %s", target)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run a lakehouse layer")
    parser.add_argument("--layer", choices=["bronze", "silver", "maintenance"], required=True)
    parser.add_argument("--dataset", choices=[*DATASETS, "all"], default="all")
    parser.add_argument("--ingest-date", type=lambda s: date.fromisoformat(s) if s else None)
    parser.add_argument("--full-refresh", action="store_true", help="silver only: rebuild from all bronze")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    from retail_pipeline.spark.session import get_spark

    cfg = PipelineConfig.from_env()
    spark = get_spark(f"walmart-{args.layer}")
    datasets = DATASETS if args.dataset == "all" else (args.dataset,)

    if args.layer == "maintenance":
        run_maintenance(spark, cfg)
        return
    if (args.layer == "bronze" or not args.full_refresh) and args.ingest_date is None:
        parser.error("--ingest-date is required (or --full-refresh for silver)")

    if args.layer == "bronze":
        from retail_pipeline.spark.bronze import run_bronze

        for ds in datasets:
            run_bronze(spark, cfg, ds, args.ingest_date)
    else:
        from retail_pipeline.spark.silver import run_silver

        for ds in datasets:
            run_silver(spark, cfg, ds, None if args.full_refresh else args.ingest_date, args.run_id)


if __name__ == "__main__":
    main()
