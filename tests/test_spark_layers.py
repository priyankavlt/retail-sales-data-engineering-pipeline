"""Bronze -> silver behaviour on a local Delta lakehouse (requires Java)."""

from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_pipeline.generator.walmart import GeneratorSpec, generate
from retail_pipeline.ingestion.landing import land_dataset, publish
from retail_pipeline.spark.bronze import SchemaDriftError, run_bronze, to_bronze
from retail_pipeline.spark.lakehouse import Lakehouse
from retail_pipeline.spark.silver import SPECS, DataQualityError, build_silver, run_silver

pytestmark = pytest.mark.spark

BATCH_1 = date(2012, 10, 13)
BATCH_2 = date(2012, 10, 20)


def _load_batch(cfg, tmp_path, batch, start, end, defect_rate=0.0):
    src = tmp_path / f"src_{batch}"
    generate(GeneratorSpec(start, end, n_stores=3, defect_rate=defect_rate), src)
    publish(cfg, src, batch)
    for ds in cfg.datasets:
        land_dataset(cfg, ds, batch)


def _bronze_frame(spark, rows, columns):
    return spark.createDataFrame(rows, columns)


def test_rules_quarantine_bad_rows_and_dedupe(spark):
    cols = ["Store", "Dept", "Date", "Weekly_Sales", "IsHoliday"]
    raw = _bronze_frame(
        spark,
        [
            ("1", "1", "2012-10-05", "100.50", "FALSE"),
            ("1", "1", "2012-10-05", "100.50", "FALSE"),  # exact duplicate
            ("1", "2", "2011-13-45", "10.00", "FALSE"),   # invalid date
            ("1", "NA", "2012-10-05", "10.00", "FALSE"),  # missing dept
            ("1", "3", "2012-10-05", "N/A", "FALSE"),     # non-numeric sales
            ("2", "1", "2012-10-04", "5.00", "FALSE"),    # Thursday: warn only
        ],
        cols,
    )
    bronze = to_bronze(raw.withColumn("_source_file", F.lit("f.csv")), "sales", BATCH_1)
    result, clean = build_silver(bronze, SPECS["sales"])

    assert result.total_rows == 6
    assert result.quarantined_rows == 3
    assert result.failed_by_rule["week_date_valid"] == 1
    assert result.failed_by_rule["dept_id_valid"] == 1
    assert result.failed_by_rule["weekly_sales_numeric"] == 1
    assert result.failed_by_rule["week_ends_friday"] == 1  # Thursday (unparseable dates fail week_date_valid)
    assert clean.count() == 2
    reasons = {r[0] for r in result.quarantined.select(F.explode("_dq_failed_rules")).collect()}
    assert reasons == {"week_date_valid", "dept_id_valid", "weekly_sales_numeric"}


def test_bronze_rejects_schema_drift(spark):
    raw = spark.createDataFrame([("1", "A")], ["Store", "Type"]).withColumn("_source_file", F.lit("x"))
    with pytest.raises(SchemaDriftError):
        to_bronze(raw, "stores", BATCH_1)


def test_end_to_end_incremental_batches(spark, local_cfg, tmp_path):
    lake = Lakehouse(spark, local_cfg)
    _load_batch(local_cfg, tmp_path, BATCH_1, date(2012, 9, 28), date(2012, 10, 12), defect_rate=0.004)
    for ds in local_cfg.datasets:
        run_bronze(spark, local_cfg, ds, BATCH_1)
        run_bronze(spark, local_cfg, ds, BATCH_1)  # rerun is idempotent
        run_silver(spark, local_cfg, ds, BATCH_1)

    bronze_rows = lake.read("bronze", "walmart_sales").count()
    silver_1 = lake.read("silver", "weekly_sales")
    weeks_1 = silver_1.select("week_date").distinct().count()
    assert weeks_1 == 3
    assert silver_1.groupBy("store_id", "dept_id", "week_date").count().where("count > 1").count() == 0
    assert silver_1.count() < bronze_rows  # duplicates/bad rows removed

    # Second batch: one new week plus a corrected value for an existing key.
    _load_batch(local_cfg, tmp_path, BATCH_2, date(2012, 10, 19), date(2012, 10, 19))
    for ds in local_cfg.datasets:
        run_bronze(spark, local_cfg, ds, BATCH_2)
        run_silver(spark, local_cfg, ds, BATCH_2)
    silver_2 = lake.read("silver", "weekly_sales")
    assert silver_2.select("week_date").distinct().count() == 4
    assert lake.read("bronze", "walmart_sales").where(F.col("_ingest_date") == BATCH_1).count() == bronze_rows

    dq = lake.read("ops", "dq_results")
    assert dq.where("dataset = 'sales'").select("ingest_date").distinct().count() == 2


def test_backfill_of_older_batch_does_not_clobber_newer_rows(spark, local_cfg, tmp_path):
    lake = Lakehouse(spark, local_cfg)
    _load_batch(local_cfg, tmp_path, BATCH_1, date(2012, 10, 12), date(2012, 10, 12))
    _load_batch(local_cfg, tmp_path, BATCH_2, date(2012, 10, 12), date(2012, 10, 12))
    run_bronze(spark, local_cfg, "stores", BATCH_1)
    run_bronze(spark, local_cfg, "stores", BATCH_2)
    run_silver(spark, local_cfg, "stores", BATCH_2)
    newest = lake.read("silver", "stores").agg(F.max("_ingested_at")).first()[0]

    run_silver(spark, local_cfg, "stores", BATCH_1)  # replay the older batch
    assert lake.read("silver", "stores").agg(F.max("_ingested_at")).first()[0] == newest
    assert lake.read("silver", "stores").agg(F.min("_ingest_date")).first()[0] == BATCH_2


def test_circuit_breaker_trips_on_bad_batch(spark, local_cfg, tmp_path):
    f = tmp_path / "incoming" / "walmart" / "stores" / BATCH_1.isoformat() / "stores.csv"
    f.parent.mkdir(parents=True)
    f.write_text("Store,Type,Size\n1,A,150000\n2,Z,-5\n3,Q,0\n")
    land_dataset(local_cfg, "stores", BATCH_1)
    run_bronze(spark, local_cfg, "stores", BATCH_1)
    with pytest.raises(DataQualityError, match="threshold"):
        run_silver(spark, local_cfg, "stores", BATCH_1)
    assert Lakehouse(spark, local_cfg).read("silver", "quarantine_stores").count() == 2
