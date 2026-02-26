from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _strip_scheme(endpoint: str) -> str:
    return endpoint.split("://", 1)[1] if "://" in endpoint else endpoint


@dataclass(frozen=True)
class S3Config:
    bucket: str
    endpoint: str
    access_key: str
    secret_key: str
    ssl_enabled: bool

    @property
    def endpoint_without_scheme(self) -> str:
        return _strip_scheme(self.endpoint)

    @property
    def endpoint_url(self) -> str:
        if "://" in self.endpoint:
            return self.endpoint
        scheme = "https" if self.ssl_enabled else "http"
        return f"{scheme}://{self.endpoint}"


@dataclass(frozen=True)
class EtlConfig:
    mysql_host: str
    mysql_port: int
    mysql_database: str
    mysql_user: str
    mysql_password: str
    mysql_jdbc_driver: str
    extract_mode: str
    incremental_hours: int
    cdr_prefix: str
    customer_prefix: str
    single_file_output: bool
    run_id: str
    s3: S3Config

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
            "?useSSL=false&allowPublicKeyRetrieval=true"
        )


@dataclass(frozen=True)
class AnalyticsConfig:
    cdr_prefix: str
    report_prefix: str
    single_file_output: bool
    run_id: str
    s3: S3Config


def _default_run_id() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def load_s3_config() -> S3Config:
    return S3Config(
        bucket=_env("S3_BUCKET", "telecom-cdr-data"),
        endpoint=_env("S3_ENDPOINT", "http://minio-service.minio.svc.cluster.local:9000"),
        access_key=_env("S3_ACCESS_KEY", _env("AWS_ACCESS_KEY_ID", "minio")),
        secret_key=_env("S3_SECRET_KEY", _env("AWS_SECRET_ACCESS_KEY", "minio123")),
        ssl_enabled=_env_bool("S3_SSL_ENABLED", False),
    )


def load_etl_config() -> EtlConfig:
    extract_mode = _env("EXTRACT_MODE", "incremental").strip().lower()
    if extract_mode not in {"incremental", "full"}:
        raise ValueError("EXTRACT_MODE must be either 'incremental' or 'full'")

    return EtlConfig(
        mysql_host=_env("MYSQL_HOST", "mysql-service.data-simulator.svc.cluster.local"),
        mysql_port=_env_int("MYSQL_PORT", 3306),
        mysql_database=_env("MYSQL_DATABASE", "telecom_data"),
        mysql_user=_env("MYSQL_USER", "telecom_user"),
        mysql_password=_env("MYSQL_PASSWORD", "telecom_password"),
        mysql_jdbc_driver=_env("MYSQL_JDBC_DRIVER", "com.mysql.cj.jdbc.Driver"),
        extract_mode=extract_mode,
        incremental_hours=_env_int("INCREMENTAL_HOURS", 1),
        cdr_prefix=_env("CDR_PREFIX", "cdr-data"),
        customer_prefix=_env("CUSTOMER_PREFIX", "customer-data"),
        single_file_output=_env_bool("SINGLE_FILE_OUTPUT", True),
        run_id=_env("RUN_ID", _default_run_id()),
        s3=load_s3_config(),
    )


def load_analytics_config() -> AnalyticsConfig:
    return AnalyticsConfig(
        cdr_prefix=_env("CDR_PREFIX", "cdr-data"),
        report_prefix=_env("CDR_REPORT_PREFIX", "cdr-report-csv"),
        single_file_output=_env_bool("SINGLE_FILE_OUTPUT", True),
        run_id=_env("RUN_ID", _default_run_id()),
        s3=load_s3_config(),
    )
