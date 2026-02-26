from __future__ import annotations

from datetime import datetime
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct, desc, lit, sum, when
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

from config import AnalyticsConfig


def _cdr_schema() -> StructType:
    return StructType(
        [
            StructField("cdr_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("call_start_time", StringType(), True),
            StructField("call_end_time", StringType(), True),
            StructField("duration_seconds", IntegerType(), True),
            StructField("call_type", StringType(), True),
            StructField("service_type", StringType(), True),
            StructField("cost", FloatType(), True),
            StructField("destination_number", StringType(), True),
            StructField("created_at", StringType(), True),
        ]
    )


def _maybe_single_file(df, enabled: bool):
    return df.coalesce(1) if enabled else df


def _find_latest_parquet_path(spark: SparkSession, bucket: str, prefix: str):
    sc = spark.sparkContext
    jvm = sc._jvm
    hadoop_conf = sc._jsc.hadoopConfiguration()

    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI.create(f"s3a://{bucket}"),
        hadoop_conf,
    )
    base_path = jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/{prefix}/")

    if not fs.exists(base_path):
        return None

    iterator = fs.listFiles(base_path, True)
    latest_path = None
    latest_mtime = -1

    while iterator.hasNext():
        status = iterator.next()
        full_path = status.getPath().toString()
        if not full_path.endswith(".parquet"):
            continue

        mtime = status.getModificationTime()
        if mtime > latest_mtime or (mtime == latest_mtime and (latest_path is None or full_path > latest_path)):
            latest_mtime = mtime
            latest_path = full_path

    return latest_path


def run_analytics_job(spark: SparkSession, cfg: AnalyticsConfig) -> Dict[str, object]:
    latest_parquet_path = _find_latest_parquet_path(
        spark=spark,
        bucket=cfg.s3.bucket,
        prefix=cfg.cdr_prefix,
    )
    if latest_parquet_path is None:
        raise RuntimeError(
            f"No parquet input found under s3://{cfg.s3.bucket}/{cfg.cdr_prefix}/"
        )

    source_path = latest_parquet_path.rsplit("/", 1)[0]
    latest_key = latest_parquet_path.replace(f"s3a://{cfg.s3.bucket}/", "", 1)

    raw_df = spark.read.schema(_cdr_schema()).parquet(source_path)
    cdr_df = raw_df.select(
        "cdr_id",
        "customer_id",
        "duration_seconds",
        "call_type",
        "service_type",
        "cost",
        "destination_number",
    )

    revenue_by_service = cdr_df.groupBy("service_type").agg(
        sum("cost").alias("total_revenue"),
        avg("cost").alias("avg_revenue_per_call"),
        count("*").alias("total_calls"),
        countDistinct("customer_id").alias("unique_customers"),
    ).orderBy(desc("total_revenue"))

    revenue_by_call_type = cdr_df.groupBy("call_type").agg(
        sum("cost").alias("total_revenue"),
        avg("cost").alias("avg_revenue_per_call"),
        count("*").alias("total_calls"),
    ).orderBy(desc("total_revenue"))

    cdr_with_usage = cdr_df.withColumn("call_duration_minutes", col("duration_seconds") / lit(60.0))

    customer_usage = cdr_with_usage.groupBy("customer_id").agg(
        count("*").alias("total_calls"),
        sum("call_duration_minutes").alias("total_minutes"),
        sum("cost").alias("total_spent"),
        countDistinct("service_type").alias("service_types_used"),
        countDistinct("call_type").alias("call_types_used"),
    ).orderBy(desc("total_spent"))

    service_metrics = cdr_with_usage.groupBy("service_type").agg(
        count("*").alias("total_calls"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("call_duration_minutes").alias("total_minutes"),
        avg("call_duration_minutes").alias("avg_call_duration"),
        sum("cost").alias("total_revenue"),
        avg("cost").alias("avg_cost_per_call"),
    ).orderBy(desc("total_revenue"))

    customer_activity = cdr_df.groupBy("customer_id").agg(
        count("*").alias("total_calls"),
        sum("cost").alias("total_spent"),
        countDistinct("service_type").alias("service_types_used"),
        countDistinct("call_type").alias("call_types_used"),
    )

    churn_risk = customer_activity.withColumn(
        "churn_risk_level",
        when(col("total_calls") < 5, "High")
        .when(col("total_calls") < 10, "Medium")
        .otherwise("Low"),
    )

    total_records = cdr_df.count()
    total_customers = cdr_df.select("customer_id").distinct().count()
    total_revenue = cdr_df.agg(sum("cost").alias("total_revenue")).collect()[0]["total_revenue"] or 0.0
    avg_call_duration = cdr_df.agg(avg("duration_seconds").alias("avg_duration")).collect()[0]["avg_duration"] or 0.0
    high_risk_count = churn_risk.filter(col("churn_risk_level") == "High").count()

    output_root = f"s3a://{cfg.s3.bucket}/{cfg.report_prefix}/{cfg.run_id}"

    output_frames = {
        "revenue_by_service": revenue_by_service,
        "revenue_by_call_type": revenue_by_call_type,
        "customer_usage": customer_usage,
        "service_metrics": service_metrics,
        "churn_risk": churn_risk,
    }

    for name, frame in output_frames.items():
        (
            _maybe_single_file(frame, cfg.single_file_output)
            .write.mode("overwrite")
            .option("header", True)
            .csv(f"{output_root}/{name}")
        )

    summary_df = spark.createDataFrame(
        [
            {
                "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "source_object": latest_key,
                "total_records": int(total_records),
                "unique_customers": int(total_customers),
                "total_revenue": float(total_revenue),
                "avg_call_duration_seconds": float(avg_call_duration),
                "high_churn_risk_customers": int(high_risk_count),
            }
        ]
    )
    _maybe_single_file(summary_df, cfg.single_file_output).write.mode("overwrite").json(
        f"{output_root}/summary"
    )

    return {
        "run_id": cfg.run_id,
        "source": {"latest_object": latest_key, "path": source_path},
        "metrics": {
            "total_records": total_records,
            "unique_customers": total_customers,
            "total_revenue": total_revenue,
            "avg_call_duration_seconds": avg_call_duration,
            "high_churn_risk_customers": high_risk_count,
        },
        "outputs": {"report_root": output_root},
    }
