# Runbook

## A run failed. Where do I look?

| Failed task | Likely cause | Action |
|---|---|---|
| `wait_for_source_files.*` | Source files late or missing | Check the drop-zone prefix for the date; re-trigger once they arrive. Sensors run in reschedule mode for up to 6 h. |
| `land_raw_files` | `LandingValidationError`: empty file, header only, or missing columns | Ask the source team for a corrected file. Nothing reached the lake. |
| `bronze_ingest` | `SchemaDriftError` or cluster or permissions problem | Check the Databricks run linked in the task log. |
| `silver_transform` | `DataQualityError`: circuit breaker tripped | Inspect `silver.quarantine_<dataset>` for that `_ingest_date` and `_dq_failed_rules`; check trends in `ops.dq_results`. |
| `gold_dbt.source_freshness` | Silver not updated in more than 15 days | Upstream stopped; check earlier runs. |
| `gold_dbt.dbt_build` | A dbt test failed | `dbt build --select <model>+` locally; run results are in `dbt/target/run_results.json`. |

Useful queries:

```sql
-- rule failures for the latest batches
select ingest_date, dataset, rule_name, severity, failed_rows, total_rows
from ops.dq_results
where failed_rows > 0
order by evaluated_at desc;

-- why rows were rejected
select _dq_failed_rules, count(*) from silver.quarantine_sales
where _ingest_date = '2012-10-27' group by 1;
```

## Reprocess a batch

All steps are idempotent. Trigger `walmart_sales_pipeline` with `{"ingest_date": "YYYY-MM-DD"}`,
or clear the failed task and downstream tasks in the Airflow UI.

## Backfill many batches

```bash
airflow dags backfill walmart_sales_pipeline -s 2012-01-07 -e 2012-03-31
```
`max_active_runs=1` keeps batches in order. Silver's recency guard keeps older replays from
overwriting newer corrections.

## Rebuild after changing a quality rule or silver logic

```bash
walmart-run-layer --layer silver --full-refresh      # rebuild silver from all of bronze
cd dbt && dbt build --full-refresh                    # rebuild gold
```

## Accepting a corrected file for a batch that already landed

Put the corrected file in the drop zone and re-run the batch. The checksum differs, so landing
copies it again (raw is versioned), and bronze replaces that `_ingest_date` partition.
