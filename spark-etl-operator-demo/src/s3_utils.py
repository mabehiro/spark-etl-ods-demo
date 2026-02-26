from __future__ import annotations

from typing import Dict, Optional

import boto3
from botocore.config import Config as BotoConfig

from config import S3Config


def build_s3_client(s3: S3Config):
    return boto3.client(
        "s3",
        endpoint_url=s3.endpoint_url,
        aws_access_key_id=s3.access_key,
        aws_secret_access_key=s3.secret_key,
        use_ssl=s3.ssl_enabled,
        verify=s3.ssl_enabled,
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def find_latest_parquet_object(s3_client, bucket: str, prefix: str) -> Optional[Dict[str, object]]:
    paginator = s3_client.get_paginator("list_objects_v2")
    latest_object: Optional[Dict[str, object]] = None
    latest_marker = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key", "")
            if not key.endswith(".parquet"):
                continue
            last_modified = obj.get("LastModified")
            marker = (last_modified, key)
            if latest_marker is None or marker > latest_marker:
                latest_marker = marker
                latest_object = obj

    return latest_object
