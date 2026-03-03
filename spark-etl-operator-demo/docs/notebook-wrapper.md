# Notebook Wrapper Usage

The wrapper notebook is intentionally thin. It imports the same `src/` functions used by SparkApplication jobs, so notebook tests and CRD runs stay aligned.

Supported runtime:
- `quay.io/rh-ee-mxavier/kernel-spark-py:3.3.1-h3.3.4-eg1`
- Spark `3.3.1` / Hadoop `3.3.4`
- Legacy Spark 3.2 / Hadoop 2.7 kernels are not supported.

Notebook file:
- `spark-etl-operator-demo/notebooks/operator_wrapper_test.ipynb`

Main callable functions:
- `run_etl(spark=None)`
- `run_analytics(spark=None)`

## Typical workflow

1. Open the notebook in Jupyter.
2. Set environment variables for MySQL and S3 if different from defaults.
3. Execute ETL test call.
4. Execute analytics test call.
5. If results look good, re-apply ConfigMap and run the SparkApplication manifests.

## Enterprise Gateway note

When using Enterprise Gateway, mount `spark-etl-operator-scripts` into kernel pods at `/opt/spark/jobs` and set `PYTHONPATH=/opt/spark/jobs`.

The wrapper notebook already checks `/opt/spark/jobs` first, then local repo paths.
It also fails fast if a legacy Spark 3.2 runtime is detected.

For full setup details, see:
- `spark-etl-operator-demo/docs/enterprise-gateway-plan.md`

## Why this helps

- Fast iteration while developing data transformations.
- Single codebase for notebook tests and operator execution.
- Easy transition from test to production-style Spark CRD run.
