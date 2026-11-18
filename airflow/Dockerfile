# Airflow + local Spark runtime for docker-compose. In production the Spark layers run on
# Databricks and Airflow only orchestrates, so this image is for development and CI.
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt gets its own virtualenv so its pins never fight Airflow's.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
         "dbt-core>=1.10,<2" "dbt-duckdb>=1.10,<2" "dbt-databricks>=1.10,<2" \
    && mkdir -p /home/airflow/.local/bin \
    && ln -s /home/airflow/dbt-venv/bin/dbt /home/airflow/.local/bin/dbt

# Project package (ingestion, Spark jobs, CLIs). src/ is also bind-mounted for live edits.
COPY --chown=airflow:root pyproject.toml README.md /opt/project/
COPY --chown=airflow:root src /opt/project/src
RUN pip install --no-cache-dir --no-deps /opt/project
