# Enterprise Gateway Plan (Notebook Wrapper)

This document defines what is needed to run the `spark-etl-operator-demo` wrapper notebook through Jupyter Enterprise Gateway (EG), while keeping production execution on SparkApplication CRDs.

## Target outcome

- Notebook executes remotely via EG kernels.
- Wrapper functions (`run_etl`, `run_analytics`) remain the same user experience.
- Python job code is still sourced from `spark-etl-operator-scripts` ConfigMap (no image rebuild for script edits).
- Promotion to production still uses:
  - `spark-etl-operator-demo/manifests/sparkapplication-etl.yaml`
  - `spark-etl-operator-demo/manifests/sparkapplication-analytics.yaml`
- Runtime support is single-path only (no legacy compatibility):
  - `quay.io/rh-ee-mxavier/kernel-spark-py:3.3.1-h3.3.4-eg1`

## Architecture decision

Use EG for interactive notebook compute, but keep SparkApplication manifests as the deployment contract.

- Dev/test path: Notebook -> Enterprise Gateway kernel pod -> wrapper functions.
- Deploy path: `oc apply -f sparkapplication-*.yaml`.

## Required workstreams

## 1) Enterprise Gateway deployment settings

1. Expose EG endpoint reachable by your notebook client.
2. Enable/confirm allowed kernels include your custom kernel spec.
3. Confirm webhook-enabled Spark Operator deployment when using operator-oriented kernelspecs.

Reference:
- [Enterprise Gateway Kubernetes deployments](https://jupyter-enterprise-gateway.readthedocs.io/en/main/operators/deploy-kubernetes.html)

## 2) Kernelspec strategy

You need a Kubernetes-targeted kernelspec in EG that sets:
- `KERNEL_NAMESPACE=spark-etl-project`
- `KERNEL_SERVICE_ACCOUNT_NAME=spark-etl-operator`
- Runtime image appropriate for your EG kernel model.

Reference:
- [Enterprise Gateway Kubernetes process proxy notes](https://jupyter-enterprise-gateway.readthedocs.io/en/v2.6.0/kernel-kubernetes.html)
- [Enterprise Gateway config options (`KERNEL_NAMESPACE`, `KERNEL_SERVICE_ACCOUNT_NAME`, `KERNEL_IMAGE`)](https://jupyter-enterprise-gateway.readthedocs.io/en/v2.5.2/config-options.html)

## 3) Make wrapper code available in EG kernel pods

Current wrapper expects local `src/` files. For EG kernels, expose scripts from ConfigMap to kernel pods.

Recommended pattern:
1. Keep `spark-etl-operator-scripts` ConfigMap as source of truth.
2. Ensure kernel pod template mounts that ConfigMap to `/opt/spark/jobs`.
3. Set `PYTHONPATH=/opt/spark/jobs` in kernel environment.
4. Keep kernel image fixed to `quay.io/rh-ee-mxavier/kernel-spark-py:3.3.1-h3.3.4-eg1`.

Notes:
- EG kernel pod templates support conditional volume constructs (`kernel_volume_mounts`, `kernel_volumes`) in template logic. Implement this in your EG kernelspec/template path.
- Keep namespace fixed to `spark-etl-project` for this demo.

Reference:
- [Enterprise Gateway kernel pod template customization concepts](https://jupyter-enterprise-gateway.readthedocs.io/en/v2.1.0/kernel-kubernetes.html)

## 4) RBAC and networking

Minimum checks:
- Kernel service account can create/read pods/services/configmaps needed by Spark jobs.
- Kernel namespace can reach:
  - MySQL service (`mysql-service.data-simulator.svc.cluster.local`)
  - MinIO endpoint (`minio-service.minio.svc.cluster.local:9000`)

If notebook users submit SparkApplication resources directly, grant `sparkoperator.k8s.io` verbs as needed.

## 5) Notebook client configuration

On the notebook/Jupyter side:
- Set `JUPYTER_GATEWAY_URL` (or equivalent GatewayClient config) to EG endpoint.
- Select the EG kernelspec in notebook UI.

Wrapper notebook in this repo is updated to resolve code from:
1. `/opt/spark/jobs` (EG ConfigMap mount)
2. local repo paths (fallback)

## 6) Validation checklist

1. EG kernel starts in `spark-etl-project` context.
2. `import notebook_wrapper` succeeds.
3. `run_etl(...)` completes and writes parquet to:
   - `cdr-data/...`
   - `customer-data/...`
4. `run_analytics(...)` completes and writes CSV outputs to:
   - `cdr-report-csv/<run_id>/...`
5. SparkApplication production run still works unchanged.

## 7) Rollout order

1. EG kernel spec and pod-template customization.
2. Mount ConfigMap + `PYTHONPATH` in kernel pods.
3. Wrapper notebook validation.
4. Team handoff: keep notebook for testing, CRDs for promotion.
