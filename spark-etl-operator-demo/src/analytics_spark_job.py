from __future__ import annotations

import json

from pyspark.sql import SparkSession

from analytics_logic import run_analytics_job
from config import load_analytics_config
from spark_runtime import configure_s3_for_spark


def main() -> None:
    cfg = load_analytics_config()
    spark = SparkSession.builder.appName("cdr-analytics-sparkapplication").getOrCreate()
    configure_s3_for_spark(spark, cfg.s3)

    try:
        result = run_analytics_job(spark, cfg)
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
