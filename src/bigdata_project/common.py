"""Outils partagés : bannière de trace et session Spark (S3 + catalogue Iceberg hadoop)."""
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession

BUCKET = os.environ["LAB_BUCKET_NAME"]
BRONZE = f"s3a://{BUCKET}/{os.environ.get('BRONZE_PREFIX', 'bronze/openflights')}"
WAREHOUSE = f"s3a://{BUCKET}/lakehouse"
EXPORT = f"s3a://{BUCKET}/export"
ICEBERG_PACKAGE = os.environ.get(
    "ICEBERG_PACKAGE", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0")


def banner(msg):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{now}] namespace={os.environ['KUBERNETES_NAMESPACE']} {msg}", flush=True)


def build_spark(app_name):
    spark = (
        SparkSession.builder.appName(app_name).master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.of", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.of.type", "hadoop")
        .config("spark.sql.catalog.of.warehouse", WAREHOUSE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
