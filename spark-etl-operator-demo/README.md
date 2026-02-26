# Spark ETL Operator Demo

This folder is an independent operator-first version of the ETL and analytics flow currently implemented in `spark-etl-datascience-demo/notebooks`.

Key changes:
- ETL and analytics run as `SparkApplication` CRDs (Spark Operator).
- Python job code is mounted from a Kubernetes `ConfigMap`.
- No Docker build is required for script changes.
- A lightweight Jupyter notebook wrapper is included for local/test iteration.

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
The ETL SparkApplication pulls MySQL JDBC from Maven via `deps.jars`; this avoids rebuilding images for JDBC updates.

## Troubleshooting

`oc logs` says `POD or TYPE/NAME is a required argument`:
- Your pod-substitution command returned empty output.
- Use the `status.driverInfo.podName` command shown above instead of pod label selection.

`ClassNotFoundException: com.mysql.cj.jdbc.Driver` in ETL:
- Verify ETL manifest includes `spec.deps.jars` for MySQL connector JAR.
- Re-apply manifests and rerun the ETL SparkApplication.

Analytics fails quickly after `RUNNING`:
- This can happen if `cdr-data/` has no parquet files yet.
- Verify ETL wrote CDR parquet output, then rerun analytics.

## Notebook wrapper

Use `spark-etl-operator-demo/notebooks/operator_wrapper_test.ipynb` to call:
- `run_etl(...)`
- `run_analytics(...)`

This keeps the code path consistent with SparkApplication jobs while still allowing notebook-based testing during development.
