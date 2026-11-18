"""Landing zone ingestion: source drop zone -> immutable raw zone on S3.

Two commands:

``publish``  Simulates the source system: uploads a folder of Walmart CSVs (generated or the
             real Kaggle files) into the drop zone ``<incoming>/walmart/<dataset>/<batch_date>/``.
``land``     Run by Airflow for each batch. Validates every file (non-empty, expected header,
             parseable line count), copies it to ``<lakehouse>/raw/walmart/<dataset>/ingest_date=<d>/``
             and writes a ``_manifest.json`` (sha256, bytes, rows). Re-running with identical
             files is a no-op, so retries and backfills are idempotent.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from retail_pipeline.config import DATASETS, PipelineConfig
from retail_pipeline.ingestion.storage import Storage, join, storage_for

log = logging.getLogger(__name__)

EXPECTED_HEADERS = {
    "stores": ["Store", "Type", "Size"],
    "features": [
        "Store", "Date", "Temperature", "Fuel_Price",
        "MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4", "MarkDown5",
        "CPI", "Unemployment", "IsHoliday",
    ],
    "sales": ["Store", "Dept", "Date", "Weekly_Sales", "IsHoliday"],
}
# File names used by the Kaggle dataset, mapped to pipeline dataset names.
SOURCE_FILE_NAMES = {"stores": ["stores.csv"], "features": ["features.csv"], "sales": ["train.csv", "sales.csv"]}


class LandingValidationError(ValueError):
    """Raised when a source file must not enter the lake."""


@dataclass
class LandedFile:
    source_uri: str
    raw_uri: str
    sha256: str
    bytes: int
    rows: int


def _profile(storage: Storage, uri: str, dataset: str) -> tuple[str, int, int]:
    digest, size, rows, header = hashlib.sha256(), 0, 0, None
    for line in storage.open_lines(uri):
        digest.update(line)
        size += len(line)
        if header is None:
            header = line.decode("utf-8-sig").strip()
        elif line.strip():
            rows += 1
    if header is None:
        raise LandingValidationError(f"{uri}: file is empty")
    columns = [c.strip().strip('"') for c in header.split(",")]
    missing = [c for c in EXPECTED_HEADERS[dataset] if c not in columns]
    if missing:
        raise LandingValidationError(f"{uri}: missing required columns {missing}; header={columns}")
    if rows == 0:
        raise LandingValidationError(f"{uri}: header only, no data rows")
    extra = [c for c in columns if c not in EXPECTED_HEADERS[dataset]]
    if extra:
        log.warning("%s: additive schema drift, new columns %s (accepted into bronze)", uri, extra)
    return digest.hexdigest(), size, rows


def land_dataset(cfg: PipelineConfig, dataset: str, ingest_date: date) -> dict:
    source_prefix = join(cfg.incoming_uri, "walmart", dataset, ingest_date.isoformat())
    raw_prefix = join(cfg.raw_uri, dataset, f"ingest_date={ingest_date.isoformat()}")
    src_store = storage_for(source_prefix, cfg.s3_endpoint_url)
    dst_store = storage_for(raw_prefix, cfg.s3_endpoint_url)

    sources = [u for u in src_store.list(source_prefix) if u.lower().endswith(".csv")]
    if not sources:
        raise LandingValidationError(f"No CSV files found for {dataset} under {source_prefix}")

    manifest_uri = join(raw_prefix, "_manifest.json")
    profiled = [(uri, *_profile(src_store, uri, dataset)) for uri in sources]

    if dst_store.exists(manifest_uri):
        previous = json.loads(dst_store.read_bytes(manifest_uri))
        if sorted(f["sha256"] for f in previous["files"]) == sorted(p[1] for p in profiled):
            log.info("%s %s already landed with identical content; skipping", dataset, ingest_date)
            return previous

    landed = []
    for uri, sha, size, rows in profiled:
        target = join(raw_prefix, uri.rsplit("/", 1)[-1])
        if type(src_store) is type(dst_store):
            dst_store.copy(uri, target)
        else:
            dst_store.write_bytes(target, src_store.read_bytes(uri))
        landed.append(LandedFile(uri, target, sha, size, rows))

    manifest = {
        "dataset": dataset,
        "ingest_date": ingest_date.isoformat(),
        "landed_at": datetime.now(timezone.utc).isoformat(),
        "row_count": sum(f.rows for f in landed),
        "files": [asdict(f) for f in landed],
    }
    dst_store.write_bytes(manifest_uri, json.dumps(manifest, indent=2).encode())
    log.info("landed %s %s: %d file(s), %d rows", dataset, ingest_date, len(landed), manifest["row_count"])
    return manifest


def publish(cfg: PipelineConfig, source_dir: Path, batch_date: date) -> list[str]:
    published = []
    for dataset, names in SOURCE_FILE_NAMES.items():
        path = next((source_dir / n for n in names if (source_dir / n).exists()), None)
        if path is None:
            log.warning("no %s file in %s; skipping", dataset, source_dir)
            continue
        target = join(cfg.incoming_uri, "walmart", dataset, batch_date.isoformat(), path.name)
        storage_for(target, cfg.s3_endpoint_url).upload_file(path, target)
        published.append(target)
        log.info("published %s -> %s", path, target)
    return published


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Walmart landing zone ingestion")
    sub = parser.add_subparsers(dest="command", required=True)
    p_pub = sub.add_parser("publish", help="upload local CSVs into the source drop zone")
    p_pub.add_argument("--source-dir", type=Path, required=True)
    p_pub.add_argument("--batch-date", type=date.fromisoformat, required=True)
    p_land = sub.add_parser("land", help="validate and copy a batch into the raw zone")
    p_land.add_argument("--ingest-date", type=date.fromisoformat, required=True)
    p_land.add_argument("--dataset", choices=[*DATASETS, "all"], default="all")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = PipelineConfig.from_env()
    if args.command == "publish":
        publish(cfg, args.source_dir, args.batch_date)
    else:
        datasets = DATASETS if args.dataset == "all" else (args.dataset,)
        for ds in datasets:
            land_dataset(cfg, ds, args.ingest_date)


if __name__ == "__main__":
    main()
