from __future__ import annotations

import json

from pyspark.sql import SparkSession

from config import load_etl_config
from etl_logic import run_etl_job
from spark_runtime import configure_s3_for_spark


def main() -> None:
    cfg = load_etl_config()
    spark = SparkSession.builder.appName("cdr-etl-sparkapplication").getOrCreate()
    configure_s3_for_spark(spark, cfg.s3)

    try:
        result = run_etl_job(spark, cfg)
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
