# Enterprise Gateway (user-workload)

Deploys Jupyter Enterprise Gateway for Kubeflow Notebooks + Spark Operator (remote PySpark kernels).

- **Namespace:** `spark-etl-project`
- **Resources:** namespace, PVC `pvc-kernelspecs`, OpenShift RBAC (privileged SCC for `enterprise-gateway-sa`), and an Argo CD Application that installs the Helm chart from `chart/` with `values.yaml`.

## Run-as-root patch (OpenShift)

The upstream chart does not render a pod `securityContext`. If the gateway needs to run as root to write to the kernelspecs PVC, apply the patch after the Helm sync, then restart:

```bash
kubectl patch deployment enterprise-gateway -n spark-etl-project --patch-file <path-to>/enterprise-gateway-run-as-root-patch.yaml
kubectl rollout restart deployment/enterprise-gateway -n spark-etl-project
```

Patch content (for reference):

```yaml
spec:
  template:
    spec:
      securityContext:
        runAsUser: 0
        runAsGroup: 0
```

Store the patch in your repo or blog/tmp and point `--patch-file` at it.
