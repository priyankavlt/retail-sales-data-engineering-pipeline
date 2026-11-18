# Local, no-cloud run of the full pipeline: generator -> landing -> bronze -> silver -> dbt (DuckDB).
# Requires Python 3.10-3.12, Java 11/17 (for Spark) and uv (or swap UV for plain pip).
SHELL := /bin/bash
PY_VERSION ?= 3.11
VENV := .venv
BIN := $(VENV)/bin
ROOT := $(shell pwd)

HISTORY_START ?= 2010-02-05
HISTORY_END   ?= 2012-10-19
HISTORY_BATCH ?= 2012-10-20
WEEK          ?= 2012-10-26
WEEK_BATCH    ?= 2012-10-27

export PIPELINE_ENV     := local
export INCOMING_URI     := $(ROOT)/data/incoming
export LAKEHOUSE_URI    := $(ROOT)/data/lakehouse
export DBT_PROFILES_DIR := $(ROOT)/dbt
export DBT_TARGET       ?= local
export DBT_SILVER_ROOT  := $(ROOT)/data/lakehouse/silver
export DBT_DUCKDB_PATH  := $(ROOT)/data/warehouse/walmart.duckdb

.PHONY: help setup lint test test-fast pipeline history weekly-batch batch dbt dbt-docs \
        up down tf-validate bundle-validate clean

help:  ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n",$$1,$$2}'

setup:  ## Create .venv with Spark, dbt and dev tooling
	uv venv --python $(PY_VERSION) $(VENV)
	uv pip install --python $(BIN)/python -e ".[spark,dbt,dev]"
	cd dbt && ../$(BIN)/dbt deps

lint:  ## Ruff lint
	$(BIN)/ruff check src tests airflow/dags

test:  ## Unit + Spark integration tests
	$(BIN)/pytest --cov=retail_pipeline --cov-report=term-missing

test-fast:  ## Tests that do not need Spark
	$(BIN)/pytest -m "not spark"

pipeline: history weekly-batch  ## Full demo: historical backfill, then one incremental week

history:  ## Load ~2.7 years of history as a single batch
	$(BIN)/walmart-generate --out data/generated/history --start $(HISTORY_START) --end $(HISTORY_END)
	$(MAKE) batch SRC=data/generated/history BATCH=$(HISTORY_BATCH)

weekly-batch:  ## Load one new week incrementally (and a store remodel -> SCD2 change)
	$(BIN)/walmart-generate --out data/generated/$(WEEK) --start $(WEEK) --end $(WEEK)
	$(MAKE) batch SRC=data/generated/$(WEEK) BATCH=$(WEEK_BATCH)

batch:  ## Run one batch end to end: SRC=<dir with csvs> BATCH=<yyyy-mm-dd>
	$(BIN)/walmart-land publish --source-dir $(SRC) --batch-date $(BATCH)
	$(BIN)/walmart-land land --ingest-date $(BATCH)
	$(BIN)/walmart-run-layer --layer bronze --ingest-date $(BATCH)
	$(BIN)/walmart-run-layer --layer silver --ingest-date $(BATCH)
	$(MAKE) dbt

dbt:  ## dbt build (seeds, snapshot, models, tests, unit tests) on DuckDB
	mkdir -p data/warehouse
	cd dbt && ../$(BIN)/dbt source freshness && ../$(BIN)/dbt build

dbt-docs:  ## Generate and serve dbt docs + lineage graph
	cd dbt && ../$(BIN)/dbt docs generate && ../$(BIN)/dbt docs serve --port 8081

up:  ## Start MinIO + Airflow (http://localhost:8080, admin/admin)
	docker compose up -d --build

down:  ## Stop the docker stack
	docker compose down

seed-minio:  ## Generate history and drop it into the MinIO source zone for the Airflow DAG
	$(BIN)/walmart-generate --out data/generated/history --start $(HISTORY_START) --end $(HISTORY_END)
	S3_ENDPOINT_URL=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
	  INCOMING_URI=s3://walmart-retail-landing/incoming \
	  $(BIN)/walmart-land publish --source-dir data/generated/history --batch-date $(HISTORY_BATCH)

tf-validate:  ## terraform fmt + validate
	cd infra/terraform && terraform fmt -check -recursive && terraform init -backend=false && terraform validate

bundle-validate:  ## Validate the Databricks asset bundle
	databricks bundle validate

clean:  ## Remove generated data and build artefacts
	rm -rf data/generated data/incoming data/lakehouse data/warehouse dbt/target dbt/logs spark-warehouse metastore_db
