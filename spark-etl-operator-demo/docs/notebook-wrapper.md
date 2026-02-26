# Notebook Wrapper Usage

The wrapper notebook is intentionally thin. It imports the same `src/` functions used by SparkApplication jobs, so notebook tests and CRD runs stay aligned.

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

## Why this helps

- Fast iteration while developing data transformations.
- Single codebase for notebook tests and operator execution.
- Easy transition from test to production-style Spark CRD run.
