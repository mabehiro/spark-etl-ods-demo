# Spark ETL Operator Demo

This folder is an independent operator-first version of the ETL and analytics flow currently implemented in `spark-etl-datascience-demo/notebooks`.

Key changes:
- ETL and analytics run as `SparkApplication` CRDs (Spark Operator).
- Python job code is mounted from a Kubernetes `ConfigMap`.
- No Docker build is required for script changes.
- A lightweight Jupyter notebook wrapper is included for local/test iteration.

## Supported runtime (single path)

This demo now supports only one kernel/runtime combination:
- Kernel image: `quay.io/rh-ee-mxavier/kernel-spark-py:3.3.1-h3.3.4-eg1`
- Spark: `3.3.1`
- Hadoop: `3.3.4`

Legacy Spark 3.2 / Hadoop 2.7 kernels are intentionally not supported.

## Folder layout

- `src/`: reusable Python logic and Spark job entrypoints.
- `manifests/`: service account/RBAC and SparkApplication CRDs.
- `kustomization.yaml`: builds the script ConfigMap from `src/*.py`.
- `notebooks/`: wrapper notebook for test-driven iteration.
- `docs/`: runbook and migration notes.

## Logic parity with existing notebooks

ETL (`etl_extract_mysql_to_s3_raw.ipynb` equivalent):
- Reads `customers` and `cdr_records` from MySQL.
- Supports `EXTRACT_MODE=incremental|full` with `INCREMENTAL_HOURS`.
- Writes Parquet to MinIO/S3 under:
  - `cdr-data/YYYY/MM/DD/run_<RUN_ID>/`
  - `customer-data/YYYY/MM/DD/run_<RUN_ID>/`

Analytics (`cdr_analytics_report.ipynb` equivalent):
- Finds latest CDR parquet object under `cdr-data/`.
- Runs the same aggregate metrics (revenue, usage, churn-risk buckets).
- Writes CSV reports and summary JSON under:
  - `cdr-report-csv/<RUN_ID>/...`

## Deploy

From repo root:

```bash
oc apply -k spark-etl-operator-demo
```

Submit ETL:

```bash
oc apply -n spark-etl-project -f spark-etl-operator-demo/manifests/sparkapplication-etl.yaml
oc get sparkapplication cdr-etl-job -n spark-etl-project -w
```

Read ETL driver logs:

```bash
ETL_DRIVER_POD=$(oc get sparkapplication cdr-etl-job -n spark-etl-project -o jsonpath='{.status.driverInfo.podName}')
oc logs -n spark-etl-project "$ETL_DRIVER_POD" --all-containers=true --tail=-1
```

Submit analytics:

```bash
oc apply -n spark-etl-project -f spark-etl-operator-demo/manifests/sparkapplication-analytics.yaml
oc get sparkapplication cdr-analytics-job -n spark-etl-project -w
```

Read analytics driver logs:

```bash
ANALYTICS_DRIVER_POD=$(oc get sparkapplication cdr-analytics-job -n spark-etl-project -o jsonpath='{.status.driverInfo.podName}')
oc logs -n spark-etl-project "$ANALYTICS_DRIVER_POD" --all-containers=true --tail=-1
```

Re-run jobs:

```bash
oc delete sparkapplication cdr-etl-job -n spark-etl-project --ignore-not-found
oc apply -n spark-etl-project -f spark-etl-operator-demo/manifests/sparkapplication-etl.yaml
```

```bash
oc delete sparkapplication cdr-analytics-job -n spark-etl-project --ignore-not-found
oc apply -n spark-etl-project -f spark-etl-operator-demo/manifests/sparkapplication-analytics.yaml
```

## Edit workflow (no image rebuild)

1. Change Python files in `spark-etl-operator-demo/src/`.
2. Re-apply Kustomize to refresh the ConfigMap:
   ```bash
   oc apply -k spark-etl-operator-demo
   ```
3. Recreate and run the SparkApplication CRD.

## Configuration

You can modify defaults in the SparkApplication manifests using environment variables:
- MySQL: `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD`
- ETL behavior: `EXTRACT_MODE`, `INCREMENTAL_HOURS`, `RUN_ID`
- Storage: `S3_BUCKET`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`
- Paths: `CDR_PREFIX`, `CUSTOMER_PREFIX`, `CDR_REPORT_PREFIX`

For production, replace inline credentials with `Secret` references.
MySQL JDBC is baked into the Spark kernel image used by the manifests.

## Troubleshooting

`oc logs` says `POD or TYPE/NAME is a required argument`:
- Your pod-substitution command returned empty output.
- Use the `status.driverInfo.podName` command shown above instead of pod label selection.

`ClassNotFoundException: com.mysql.cj.jdbc.Driver` in ETL:
- Ensure SparkApplication image is `quay.io/rh-ee-mxavier/kernel-spark-py:3.3.1-h3.3.4-eg1`.
- Recreate the SparkApplication (or restart notebook kernel for EG), then rerun ETL.

Notebook cell shows `CWD: /opt/spark-3.2.1-bin-hadoop2.7/work-dir`:
- You are on a legacy kernel pod.
- Restart notebook kernel and select `Spark Operator (Python)` so EG launches the supported image.
- Verify active EG kernel SparkApplication image:
  ```bash
  oc get sparkapplication default-<KERNEL_ID> -n spark-etl-project -o jsonpath='{.spec.image}{"\n"}'
  ```
  Expected: `quay.io/rh-ee-mxavier/kernel-spark-py:3.3.1-h3.3.4-eg1`

Driver fails with `Error opening zip file or JAR manifest missing : /prometheus/jmx_prometheus_javaagent-0.17.0.jar`:
- Ensure ConfigMap `spark-jmx-exporter-jar` exists in `spark-etl-project`.
- Re-apply manifests from this folder; ETL/analytics now mount that ConfigMap at `/prometheus`.

Analytics fails quickly after `RUNNING`:
- This can happen if `cdr-data/` has no parquet files yet.
- Verify ETL wrote CDR parquet output, then rerun analytics.

## Notebook wrapper

Use `spark-etl-operator-demo/notebooks/operator_wrapper_test.ipynb` to call:
- `run_etl(...)`
- `run_analytics(...)`

This keeps the code path consistent with SparkApplication jobs while still allowing notebook-based testing during development.

## Enterprise Gateway

If notebooks are executed through Jupyter Enterprise Gateway, see:
- `spark-etl-operator-demo/docs/enterprise-gateway-plan.md`

This plan covers kernelspec requirements, ConfigMap mounting of wrapper scripts, and validation steps for EG-based notebook execution.
