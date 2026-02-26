from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, lit

from config import EtlConfig


CDR_COLUMNS = [
    "cdr_id",
    "customer_id",
    "call_start_time",
    "call_end_time",
    "duration_seconds",
    "call_type",
    "service_type",
    "cost",
    "destination_number",
    "created_at",
]


def _jdbc_read(spark: SparkSession, cfg: EtlConfig, dbtable: str):
    return (
        spark.read.format("jdbc")
        .option("url", cfg.jdbc_url)
        .option("driver", cfg.mysql_jdbc_driver)
        .option("user", cfg.mysql_user)
        .option("password", cfg.mysql_password)
        .option("dbtable", dbtable)
        .load()
    )


def _safe_cast(df, name: str, target_type: str):
    if name in df.columns:
        return col(name).cast(target_type)
    return lit(None).cast(target_type)


def run_etl_job(spark: SparkSession, cfg: EtlConfig) -> Dict[str, object]:
    window_end = datetime.now().replace(microsecond=0)

    customers_raw = _jdbc_read(spark, cfg, "customers")
    customers_df = customers_raw.select(
        _safe_cast(customers_raw, "customer_id", "string").alias("customer_id"),
        _safe_cast(customers_raw, "name", "string").alias("name"),
        _safe_cast(customers_raw, "phone_number", "string").alias("phone_number"),
        _safe_cast(customers_raw, "plan_type", "string").alias("plan_type"),
        date_format(
            _safe_cast(customers_raw, "registration_date", "timestamp"),
            "yyyy-MM-dd HH:mm:ss",
        ).alias("registration_date"),
        _safe_cast(customers_raw, "status", "string").alias("status"),
    )

    if cfg.extract_mode == "incremental":
        window_start = window_end - timedelta(hours=cfg.incremental_hours)
        start_text = window_start.strftime("%Y-%m-%d %H:%M:%S")
        end_text = window_end.strftime("%Y-%m-%d %H:%M:%S")
        cdr_query = f"""
            (SELECT cdr_id, customer_id, call_start_time, call_end_time, duration_seconds,
                    call_type, service_type, cost, destination_number, created_at
             FROM cdr_records
             WHERE call_start_time >= '{start_text}'
               AND call_start_time < '{end_text}'
             ORDER BY call_start_time) cdr_window
        """
    else:
        window_start = window_end
        cdr_query = (
            "(SELECT cdr_id, customer_id, call_start_time, call_end_time, duration_seconds, "
            "call_type, service_type, cost, destination_number, created_at "
            "FROM cdr_records) cdr_window"
        )

    cdr_raw = _jdbc_read(spark, cfg, cdr_query)
    cdr_df = cdr_raw.select(
        _safe_cast(cdr_raw, "cdr_id", "string").alias("cdr_id"),
        _safe_cast(cdr_raw, "customer_id", "string").alias("customer_id"),
        date_format(_safe_cast(cdr_raw, "call_start_time", "timestamp"), "yyyy-MM-dd HH:mm:ss").alias(
            "call_start_time"
        ),
        date_format(_safe_cast(cdr_raw, "call_end_time", "timestamp"), "yyyy-MM-dd HH:mm:ss").alias(
            "call_end_time"
        ),
        _safe_cast(cdr_raw, "duration_seconds", "int").alias("duration_seconds"),
        _safe_cast(cdr_raw, "call_type", "string").alias("call_type"),
        _safe_cast(cdr_raw, "service_type", "string").alias("service_type"),
        _safe_cast(cdr_raw, "cost", "float").alias("cost"),
        _safe_cast(cdr_raw, "destination_number", "string").alias("destination_number"),
        date_format(_safe_cast(cdr_raw, "created_at", "timestamp"), "yyyy-MM-dd HH:mm:ss").alias("created_at"),
    )

    cdr_count = cdr_df.count()
    customers_count = customers_df.count()

    run_id = cfg.run_id
    year = window_start.year
    month = window_start.month
    day = window_start.day

    cdr_out = f"s3a://{cfg.s3.bucket}/{cfg.cdr_prefix}/{year}/{month:02d}/{day:02d}/run_{run_id}"
    customers_out = (
        f"s3a://{cfg.s3.bucket}/{cfg.customer_prefix}/{year}/{month:02d}/{day:02d}/run_{run_id}"
    )

    if cdr_count > 0:
        cdr_writer = cdr_df.coalesce(1) if cfg.single_file_output else cdr_df
        cdr_writer.write.mode("overwrite").parquet(cdr_out)

    customers_writer = customers_df.coalesce(1) if cfg.single_file_output else customers_df
    customers_writer.write.mode("overwrite").parquet(customers_out)

    return {
        "extract_mode": cfg.extract_mode,
        "run_id": run_id,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "counts": {"cdr_records": cdr_count, "customers": customers_count},
        "outputs": {
            "cdr_path": cdr_out if cdr_count > 0 else None,
            "customers_path": customers_out,
        },
        "schema_columns": CDR_COLUMNS,
    }
