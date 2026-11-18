"""Declarative row-level data-quality rules for PySpark.

Each rule is a SQL boolean expression that is TRUE for a valid row.

* ``error`` rules route failing rows to a quarantine table (they never reach silver).
* ``warn`` rules let rows through but are counted and reported.

Every evaluation also yields per-rule metrics, persisted to ``ops.dq_results`` for trending.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

Severity = Literal["error", "warn"]


@dataclass(frozen=True)
class Rule:
    name: str
    condition: str
    severity: Severity = "error"


@dataclass
class QualityResult:
    valid: DataFrame
    quarantined: DataFrame
    total_rows: int
    quarantined_rows: int
    failed_by_rule: dict[str, int] = field(default_factory=dict)
    rules: list[Rule] = field(default_factory=list)
    evaluated: DataFrame | None = None  # cached frame backing valid/quarantined

    def release(self) -> None:
        if self.evaluated is not None:
            self.evaluated.unpersist()

    @property
    def quarantine_ratio(self) -> float:
        return self.quarantined_rows / self.total_rows if self.total_rows else 0.0


def _failed_names(rules: list[Rule]) -> Column:
    flags = [F.when(~F.col(f"_dq_{r.name}"), F.lit(r.name)) for r in rules]
    if not flags:
        return F.array().cast("array<string>")
    return F.filter(F.array(*flags), lambda x: x.isNotNull())


def evaluate(df: DataFrame, rules: list[Rule]) -> QualityResult:
    # NULL results count as failures: a rule that cannot be evaluated is not satisfied.
    checked = df
    for r in rules:
        checked = checked.withColumn(f"_dq_{r.name}", F.coalesce(F.expr(r.condition), F.lit(False)))
    errors = [r for r in rules if r.severity == "error"]
    checked = checked.withColumn("_dq_failed_rules", _failed_names(errors)).cache()

    metrics = checked.agg(
        F.count(F.lit(1)).alias("__total"),
        F.sum(F.when(F.size("_dq_failed_rules") > 0, 1).otherwise(0)).alias("__quarantined"),
        *[F.sum(F.when(~F.col(f"_dq_{r.name}"), 1).otherwise(0)).alias(r.name) for r in rules],
    ).first()

    dq_cols = [f"_dq_{r.name}" for r in rules]
    return QualityResult(
        valid=checked.where(F.size("_dq_failed_rules") == 0).drop("_dq_failed_rules", *dq_cols),
        quarantined=checked.where(F.size("_dq_failed_rules") > 0).drop(*dq_cols),
        total_rows=int(metrics["__total"]),
        quarantined_rows=int(metrics["__quarantined"] or 0),
        failed_by_rule={r.name: int(metrics[r.name] or 0) for r in rules},
        rules=rules,
        evaluated=checked,
    )


def metrics_frame(spark, result: QualityResult, *, run_id: str, dataset: str, ingest_date: str | None) -> DataFrame:
    rows = [
        (run_id, dataset, ingest_date, r.name, r.severity, r.condition, result.failed_by_rule[r.name], result.total_rows)
        for r in result.rules
    ]
    schema = (
        "run_id string, dataset string, ingest_date string, rule_name string, severity string, "
        "rule_condition string, failed_rows long, total_rows long"
    )
    return spark.createDataFrame(rows, schema).withColumn("evaluated_at", F.current_timestamp())
