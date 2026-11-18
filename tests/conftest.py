from __future__ import annotations

import shutil

import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    if not shutil.which("java"):
        pytest.skip("Java runtime not available for local Spark")
    from retail_pipeline.spark.session import get_spark

    session = get_spark("walmart-tests")
    yield session
    session.stop()


@pytest.fixture
def local_cfg(tmp_path, monkeypatch):
    from retail_pipeline.config import PipelineConfig

    monkeypatch.setenv("PIPELINE_ENV", "local")
    return PipelineConfig(
        incoming_uri=str(tmp_path / "incoming"),
        lakehouse_uri=str(tmp_path / "lakehouse"),
    )
