"""SparkSession factory: reuses the Databricks session, or builds a local Delta-enabled one."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


def get_spark(app_name: str = "walmart-retail-pipeline") -> SparkSession:
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        # Databricks runtime ships Delta, Unity Catalog and S3 access via instance profile.
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    # Python workers must match the driver's interpreter (avoids PYTHON_VERSION_MISMATCH).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
    )
    extra_packages = []
    endpoint = os.getenv("S3_ENDPOINT_URL")
    if endpoint or os.getenv("LAKEHOUSE_URI", "").startswith("s3"):
        extra_packages.append(HADOOP_AWS_PACKAGE)
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        if endpoint:  # MinIO
            builder = (
                builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
            )
    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
