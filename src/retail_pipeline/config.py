"""Runtime configuration, resolved from environment variables.

The same code runs in three environments; only configuration changes:

* ``local``      - local filesystem lakehouse, local Spark, dbt-duckdb
* ``docker``     - MinIO (S3 API) lakehouse, local Spark via s3a, dbt-duckdb (docker-compose)
* ``databricks`` - AWS S3 + Unity Catalog tables on Databricks, dbt-databricks
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

DATASETS = ("stores", "features", "sales")


@dataclass(frozen=True)
class PipelineConfig:
    env: str = "local"
    # Where the source system drops files (simulated by the generator).
    incoming_uri: str = "data/incoming"
    # Root of the lakehouse. Raw files land under ``<root>/raw``; Delta tables live under
    # ``<root>/bronze``, ``<root>/silver`` and ``<root>/ops`` when running path-based.
    lakehouse_uri: str = "data/lakehouse"
    # Unity Catalog name. When set, Delta tables are addressed as ``catalog.schema.table``
    # instead of by path (Databricks).
    catalog: str | None = None
    s3_endpoint_url: str | None = None
    # Circuit breaker: fail the silver job when more than this share of a batch is quarantined.
    max_quarantine_ratio: float = 0.05
    datasets: tuple[str, ...] = field(default=DATASETS)

    @property
    def raw_uri(self) -> str:
        return f"{self.lakehouse_uri.rstrip('/')}/raw/walmart"

    @property
    def uses_catalog(self) -> bool:
        return bool(self.catalog)

    @classmethod
    def from_env(cls) -> PipelineConfig:
        return cls(
            env=os.getenv("PIPELINE_ENV", "local"),
            incoming_uri=os.getenv("INCOMING_URI", "data/incoming"),
            lakehouse_uri=os.getenv("LAKEHOUSE_URI", "data/lakehouse"),
            catalog=os.getenv("UC_CATALOG") or None,
            s3_endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
            max_quarantine_ratio=float(os.getenv("MAX_QUARANTINE_RATIO", "0.05")),
        )


def to_spark_uri(uri: str) -> str:
    """Hadoop's S3 connector uses the ``s3a://`` scheme outside Databricks."""
    if uri.startswith("s3://") and os.getenv("PIPELINE_ENV", "local") != "databricks":
        return "s3a://" + uri[len("s3://") :]
    return uri
