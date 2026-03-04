from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession

from analytics_logic import run_analytics_job
from config import load_analytics_config, load_etl_config
from etl_logic import run_etl_job
from spark_runtime import configure_s3_for_spark


SUPPORTED_SPARK_VERSION = "3.3.1"


def _as_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _ensure_supported_runtime(spark: SparkSession) -> None:
    spark_home = spark.sparkContext.getConf().get("spark.home", "")
    if spark.version != SUPPORTED_SPARK_VERSION or "hadoop2.7" in spark_home:
        raise RuntimeError(
            "Unsupported Spark runtime detected. "
            f"Expected Spark {SUPPORTED_SPARK_VERSION} on kernel image "
            "'quay.io/rh-ee-mxavier/kernel-spark-py:3.3.1-h3.3.4-eg1'. "
            "Restart the notebook kernel and select 'Spark Operator (Python)'."
        )


def _get_existing_session() -> Optional[SparkSession]:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return getattr(SparkSession, "_instantiatedSession", None)


def _clear_cached_session_refs(session: SparkSession) -> None:
    # EG kernels may keep a default instantiated session even when there is no active session.
    if getattr(SparkSession, "_instantiatedSession", None) is session:
        SparkSession._instantiatedSession = None
    if getattr(SparkSession, "_activeSession", None) is session:
        SparkSession._activeSession = None


def create_spark(app_name: str = "cdr-wrapper-notebook") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    force_recreate = _as_bool(os.getenv("SPARK_FORCE_RECREATE", "false"))

    existing = _get_existing_session()
    if existing is not None:
        if force_recreate:
            existing.stop()
            _clear_cached_session_refs(existing)
            spark = builder.getOrCreate()
            _ensure_supported_runtime(spark)
            return spark

        _ensure_supported_runtime(existing)
        return existing

    spark = builder.getOrCreate()
    _ensure_supported_runtime(spark)
    return spark


def running_via_enterprise_gateway() -> bool:
    # EG kernels expose at least one of these vars in most deployments.
    return bool(
        os.getenv("KERNEL_ID")
        or os.getenv("KERNEL_USERNAME")
        or os.getenv("EG_RESPONSE_ADDRESS")
    )


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
