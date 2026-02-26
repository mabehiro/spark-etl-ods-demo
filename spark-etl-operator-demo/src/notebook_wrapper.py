from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

from analytics_logic import run_analytics_job
from config import load_analytics_config, load_etl_config
from etl_logic import run_etl_job
from spark_runtime import configure_s3_for_spark


def create_spark(app_name: str = "cdr-wrapper-notebook") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def run_etl(spark: Optional[SparkSession] = None):
    own_session = spark is None
    spark = spark or create_spark("cdr-wrapper-etl")
    cfg = load_etl_config()
    configure_s3_for_spark(spark, cfg.s3)

    try:
        return run_etl_job(spark, cfg)
    finally:
        if own_session:
            spark.stop()


def run_analytics(spark: Optional[SparkSession] = None):
    own_session = spark is None
    spark = spark or create_spark("cdr-wrapper-analytics")
    cfg = load_analytics_config()
    configure_s3_for_spark(spark, cfg.s3)

    try:
        return run_analytics_job(spark, cfg)
    finally:
        if own_session:
            spark.stop()
