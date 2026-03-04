from __future__ import annotations

from config import S3Config


S3A_CREDENTIALS_PROVIDER = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"


def configure_s3_for_spark(spark, s3: S3Config) -> None:
    endpoint = s3.endpoint_without_scheme
    bucket_prefix = f"fs.s3a.bucket.{s3.bucket}"
    settings = {
        "fs.s3a.endpoint": endpoint,
        "fs.s3a.access.key": s3.access_key,
        "fs.s3a.secret.key": s3.secret_key,
        "fs.s3a.aws.credentials.provider": S3A_CREDENTIALS_PROVIDER,
        "fs.s3a.path.style.access": "true",
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.connection.ssl.enabled": str(s3.ssl_enabled).lower(),
        # Skip strict bucket HEAD probing against custom endpoints to avoid false negatives.
        "fs.s3a.bucket.probe": "0",
        f"{bucket_prefix}.endpoint": endpoint,
        f"{bucket_prefix}.access.key": s3.access_key,
        f"{bucket_prefix}.secret.key": s3.secret_key,
        f"{bucket_prefix}.aws.credentials.provider": S3A_CREDENTIALS_PROVIDER,
        f"{bucket_prefix}.path.style.access": "true",
        f"{bucket_prefix}.connection.ssl.enabled": str(s3.ssl_enabled).lower(),
    }

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    for key, value in settings.items():
        hadoop_conf.set(key, value)
        spark.conf.set(f"spark.hadoop.{key}", value)

    # Ensure previously cached FileSystem clients do not retain stale S3A settings.
    try:
        spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.closeAll()
    except Exception:
        pass
