import json
from datetime import date

import boto3
import pytest
from moto import mock_aws

from retail_pipeline.config import PipelineConfig
from retail_pipeline.generator.walmart import GeneratorSpec, generate
from retail_pipeline.ingestion.landing import LandingValidationError, land_dataset, publish

BATCH = date(2012, 10, 27)


@pytest.fixture
def source_dir(tmp_path):
    return tmp_path / "src", generate(
        GeneratorSpec(date(2012, 10, 5), date(2012, 10, 26), n_stores=2, defect_rate=0), tmp_path / "src"
    )


def test_publish_and_land_local(local_cfg, source_dir):
    src, _ = source_dir
    published = publish(local_cfg, src, BATCH)
    assert len(published) == 3

    manifest = land_dataset(local_cfg, "sales", BATCH)
    assert manifest["row_count"] > 0
    raw_file = manifest["files"][0]["raw_uri"]
    assert "raw/walmart/sales/ingest_date=2012-10-27/train.csv" in raw_file


def test_landing_is_idempotent(local_cfg, source_dir):
    src, _ = source_dir
    publish(local_cfg, src, BATCH)
    first = land_dataset(local_cfg, "stores", BATCH)
    second = land_dataset(local_cfg, "stores", BATCH)
    assert first["landed_at"] == second["landed_at"]  # skipped, manifest unchanged


def test_rejects_missing_columns(local_cfg, tmp_path):
    bad = tmp_path / "incoming" / "walmart" / "sales" / BATCH.isoformat() / "train.csv"
    bad.parent.mkdir(parents=True)
    bad.write_text("Store,Date,Weekly_Sales\n1,2012-10-26,100.0\n")
    with pytest.raises(LandingValidationError, match="missing required columns"):
        land_dataset(local_cfg, "sales", BATCH)


def test_rejects_header_only_file(local_cfg, tmp_path):
    f = tmp_path / "incoming" / "walmart" / "stores" / BATCH.isoformat() / "stores.csv"
    f.parent.mkdir(parents=True)
    f.write_text("Store,Type,Size\n")
    with pytest.raises(LandingValidationError, match="no data rows"):
        land_dataset(local_cfg, "stores", BATCH)


@mock_aws
def test_land_on_s3(source_dir, monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")
    for b in ("landing", "lake"):
        s3.create_bucket(Bucket=b)
    cfg = PipelineConfig(incoming_uri="s3://landing/incoming", lakehouse_uri="s3://lake")
    src, _ = source_dir
    publish(cfg, src, BATCH)
    manifest = land_dataset(cfg, "features", BATCH)

    key = "raw/walmart/features/ingest_date=2012-10-27/_manifest.json"
    stored = json.loads(s3.get_object(Bucket="lake", Key=key)["Body"].read())
    assert stored["row_count"] == manifest["row_count"] == 2 * 4
