from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, lit
from py4j.protocol import Py4JJavaError

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

_MYSQL_DRIVER_READY = False


def _register_mysql_driver_from_jar(spark: SparkSession, driver_class: str, jar_path: str) -> bool:
    if not os.path.exists(jar_path):
        return False

    jvm = spark.sparkContext._jvm
    gateway = spark.sparkContext._gateway
    url_arr = gateway.new_array(jvm.java.net.URL, 1)
    url_arr[0] = jvm.java.io.File(jar_path).toURI().toURL()
    parent = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    loader = jvm.java.net.URLClassLoader(url_arr, parent)
    driver_cls = jvm.java.lang.Class.forName(driver_class, True, loader)
    driver_obj = driver_cls.getDeclaredConstructor().newInstance()
    wrapper = jvm.org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper(driver_obj)
    jvm.java.sql.DriverManager.registerDriver(wrapper)
    return True


def _ensure_mysql_driver_ready(spark: SparkSession, cfg: EtlConfig) -> None:
    global _MYSQL_DRIVER_READY
    if _MYSQL_DRIVER_READY:
        return

    jvm = spark.sparkContext._jvm
    driver_candidates = [cfg.mysql_jdbc_driver]
    if cfg.mysql_jdbc_driver == "com.mysql.cj.jdbc.Driver":
        driver_candidates.append("com.mysql.jdbc.Driver")

    for driver_name in driver_candidates:
        try:
            jvm.java.lang.Class.forName(driver_name)
            _MYSQL_DRIVER_READY = True
            return
        except Exception:
            pass

    jar_candidates = [
        "/tmp/.ivy2/jars/com.mysql_mysql-connector-j-8.0.33.jar",
        "/tmp/.ivy2/jars/mysql_mysql-connector-java-8.0.33.jar",
    ]
    jar_env = os.getenv("SPARK_JARS", "").strip()
    if jar_env:
        for value in jar_env.split(","):
            item = value.strip()
            if item.startswith("file:"):
                item = item[5:]
            if item and item not in jar_candidates:
                jar_candidates.append(item)

    for driver_name in driver_candidates:
        for jar_path in jar_candidates:
            try:
                if _register_mysql_driver_from_jar(spark, driver_name, jar_path):
                    _MYSQL_DRIVER_READY = True
                    return
            except Exception:
                continue


def _jdbc_read(spark: SparkSession, cfg: EtlConfig, dbtable: str):
    _ensure_mysql_driver_ready(spark, cfg)

    reader = (
        spark.read.format("jdbc")
        .option("url", cfg.jdbc_url)
        .option("user", cfg.mysql_user)
        .option("password", cfg.mysql_password)
        .option("dbtable", dbtable)
    )

    driver_candidates = [cfg.mysql_jdbc_driver]
    if cfg.mysql_jdbc_driver == "com.mysql.cj.jdbc.Driver":
        driver_candidates.append("com.mysql.jdbc.Driver")

    for driver_name in driver_candidates:
        try:
            return reader.option("driver", driver_name).load()
        except Py4JJavaError as exc:
            if "ClassNotFoundException" in str(exc) and driver_name in str(exc):
                continue
            raise

    # Final fallback: rely on JDBC service discovery from jars if explicit driver registration fails.
    return reader.load()


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
