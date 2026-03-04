# Runbook

## 1) Prerequisites

- Spark Operator installed.
- Namespace `spark-etl-project` exists.
- MySQL dataset available (same schema as existing demo).
- MinIO/S3 endpoint reachable from Spark pods.
- Supported kernel/runtime image only:
  - `quay.io/rh-ee-mxavier/kernel-spark-py:3.3.1-h3.3.4-eg1`

## 2) Install resources

```bash
oc apply -k spark-etl-operator-demo
```

This installs:
- Service account and RBAC.
- Script ConfigMap generated from `src/*.py`.
- SparkApplication manifests.

## 3) Run ETL

```bash
oc delete sparkapplication cdr-etl-job -n spark-etl-project --ignore-not-found
oc apply -n spark-etl-project -f spark-etl-operator-demo/manifests/sparkapplication-etl.yaml
oc get sparkapplication cdr-etl-job -n spark-etl-project -w
```

Inspect driver logs:

```bash
ETL_DRIVER_POD=$(oc get sparkapplication cdr-etl-job -n spark-etl-project -o jsonpath='{.status.driverInfo.podName}')
oc logs -n spark-etl-project "$ETL_DRIVER_POD" --all-containers=true --tail=-1
```

## 4) Run analytics

```bash
oc delete sparkapplication cdr-analytics-job -n spark-etl-project --ignore-not-found
oc apply -n spark-etl-project -f spark-etl-operator-demo/manifests/sparkapplication-analytics.yaml
oc get sparkapplication cdr-analytics-job -n spark-etl-project -w
```

Inspect driver logs:

```bash
ANALYTICS_DRIVER_POD=$(oc get sparkapplication cdr-analytics-job -n spark-etl-project -o jsonpath='{.status.driverInfo.podName}')
oc logs -n spark-etl-project "$ANALYTICS_DRIVER_POD" --all-containers=true --tail=-1
```

## 5) Validate outputs

- ETL output:
  - `s3://telecom-cdr-data/cdr-data/YYYY/MM/DD/run_<RUN_ID>/`
  - `s3://telecom-cdr-data/customer-data/YYYY/MM/DD/run_<RUN_ID>/`
- Analytics output:
  - `s3://telecom-cdr-data/cdr-report-csv/<RUN_ID>/revenue_by_service/`
  - `s3://telecom-cdr-data/cdr-report-csv/<RUN_ID>/...`
  - `s3://telecom-cdr-data/cdr-report-csv/<RUN_ID>/summary/`

## 6) Change script logic

1. Edit files under `src/`.
2. Re-apply:
   ```bash
   oc apply -k spark-etl-operator-demo
   ```
3. Delete and re-run the relevant SparkApplication.

No Docker image build is needed for these script changes.

Notebook wrapper testing is supported only via Enterprise Gateway kernel `Spark Operator (Python)` with scripts mounted at `/opt/spark/jobs`.

## 7) Troubleshooting

`oc logs` returns `POD or TYPE/NAME is a required argument`:
- This usually means the pod-selection command returned empty output.
- Use SparkApplication status (`.status.driverInfo.podName`) to resolve the driver pod name.

ETL fails with `ClassNotFoundException: com.mysql.cj.jdbc.Driver`:
- This demo uses baked JDBC in the kernel image, not `spec.deps.jars`.
- Verify ETL/analytics image is:
  - `quay.io/rh-ee-mxavier/kernel-spark-py:3.3.1-h3.3.4-eg1`
- Re-apply and rerun:
  ```bash
  oc apply -k spark-etl-operator-demo
  oc delete sparkapplication cdr-etl-job -n spark-etl-project --ignore-not-found
  oc apply -n spark-etl-project -f spark-etl-operator-demo/manifests/sparkapplication-etl.yaml
  ```

Notebook/EG kernel shows unexpected runtime/path:
- You are attached to an unsupported kernel SparkApplication.
- Restart notebook kernel and select `Spark Operator (Python)`.
- Optionally clean active legacy default kernels:
  ```bash
  oc get sparkapplication -n spark-etl-project | grep '^default-'
  oc delete sparkapplication default-<KERNEL_ID> -n spark-etl-project
  ```

Analytics enters `FAILING` quickly:
- Check analytics driver logs first:
  ```bash
  ANALYTICS_DRIVER_POD=$(oc get sparkapplication cdr-analytics-job -n spark-etl-project -o jsonpath='{.status.driverInfo.podName}')
  oc logs -n spark-etl-project "$ANALYTICS_DRIVER_POD" --all-containers=true --tail=-1
  ```
- If logs say no parquet under `cdr-data/`, rerun ETL with a wider window (or full mode) and then rerun analytics.
