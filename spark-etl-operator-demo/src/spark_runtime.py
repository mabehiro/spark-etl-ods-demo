from __future__ import annotations

from config import S3Config


def configure_s3_for_spark(spark, s3: S3Config) -> None:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", s3.endpoint_without_scheme)
    hadoop_conf.set("fs.s3a.access.key", s3.access_key)
    hadoop_conf.set("fs.s3a.secret.key", s3.secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", str(s3.ssl_enabled).lower())
