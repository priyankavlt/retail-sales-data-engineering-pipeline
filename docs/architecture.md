# Architecture and design decisions

## Layers

| Layer | Where | Format | Written by | Contract |
|---|---|---|---|---|
| Drop zone | `s3://<landing>/incoming/walmart/<dataset>/<batch_date>/` | CSV | source system | whatever the source sends |
| Raw | `s3://<lakehouse>/raw/walmart/<dataset>/ingest_date=<d>/` | CSV + `_manifest.json` | Airflow `land_raw_files` | header validated, checksummed, immutable |
| Bronze | `<catalog>.bronze.walmart_*` | Delta, partitioned by `_ingest_date` | PySpark | all strings, exactly as delivered, plus lineage columns |
| Silver | `<catalog>.silver.{stores,store_features,weekly_sales}` | Delta | PySpark | typed, validated, one row per natural key |
| Quarantine | `<catalog>.silver.quarantine_*` | Delta | PySpark | rejected rows + `_dq_failed_rules` |
| Ops | `<catalog>.ops.dq_results` | Delta | PySpark | per-batch, per-rule failure counts |
| Gold | `<catalog>.gold.*` | Delta (dbt-databricks) | dbt | star schema + marts, tested |

## Key decisions

**Why a raw zone before bronze?** The drop zone belongs to the source and is expired after 30
days. Copying validated files into an immutable, versioned raw zone with a manifest gives a
replayable record of exactly what was received, independent of Spark.

**Why is bronze all strings?** Type coercion is where data gets silently lost. Keeping bronze
untyped means a bad value (`"N/A"` sales, `2011-13-45` dates) is preserved and visible, and
silver can be rebuilt (`walmart-run-layer --layer silver --full-refresh`) after a rule change.

**Why `try_cast`?** Databricks SQL and newer Spark run with ANSI mode, where a plain `cast`
raises on bad input. `try_cast` yields NULL, which the DQ rules then catch and quarantine with a
reason, instead of failing the whole job.

**Why MERGE with a recency guard?** Retail data gets restated. Upserting on the natural key
applies corrections; the `s._ingested_at >= t._ingested_at` condition makes backfills of older
batches safe.

**Why quarantine and a circuit breaker?** Dropping a few malformed rows is normal; losing a
large share of a batch means something upstream broke. Rows failing `error` rules are kept with
reasons for triage, and the job fails if more than `MAX_QUARANTINE_RATIO` (5%) of a batch is
rejected, so bad data never reaches gold.

**Why PySpark for bronze/silver but dbt for gold?** Bronze/silver are file-oriented, procedural
and need Delta MERGE, metadata columns and custom quality logic: Spark's strengths. Gold is
relational modelling that analysts should be able to read and change: dbt gives SQL, tests,
documentation, lineage, snapshots and incremental models.

**Why Airflow and Databricks Workflows?** Airflow owns cross-system orchestration (S3 sensing,
landing, Databricks, dbt, alerting, backfills). Databricks Workflows (deployed as code with an
Asset Bundle) owns the intra-Spark fan-out on one job cluster. Airflow triggers jobs by name with
deferrable operators, so no worker slot is held while Spark runs.

**Why DuckDB locally?** It reads the same Delta tables directly (`delta_scan`), so the whole
pipeline, including every dbt test, runs in CI in minutes with no cloud spend. A handful of
dispatch macros cover the few SQL dialect differences.

## Incremental processing

* Airflow passes one `ingest_date` per run; landing, bronze and silver only touch that batch.
* `fct_weekly_sales` is an incremental model filtered on `_silver_updated_at`, merged on
  `(store_id, dept_id, week_date)`.
* Dimensions and marts are small and rebuilt each run; the SCD2 snapshot captures store changes.
* Full rebuild: `walmart-run-layer --layer silver --full-refresh` then `dbt build --full-refresh`.

## Scaling notes

* Silver tables are Z-ordered on `(store_id, week_date)` weekly; the gold fact uses liquid
  clustering on Databricks.
* Bronze is partitioned by ingest date (low cardinality, aligned with the write pattern).
  Silver is not partitioned: at this volume partitioning only adds small files.
* Job clusters autoscale 1–4 Graviton workers with spot-with-fallback, plus optimized writes and
  auto-compaction.
