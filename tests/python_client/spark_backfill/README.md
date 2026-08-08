# Spark-Milvus Backfill pytest

This directory contains the Spark-Milvus Read/Backfill pytest suite. It supports two Spark execution modes:

- `toolbox`: reuses a Toolbox Pod whose Connector was compiled in the target environment through Kubernetes exec. This is suitable for manual feature validation and future Nightly builds from the latest Connector source.
- `job`: creates a one-shot Kubernetes Job for each Spark invocation and downloads the Connector from an HTTPS bundle. This is suitable for environments that publish pinned Connector artifacts.

Both modes run `spark-submit --master local[2]` in a single Pod. A Spark Master/Worker cluster is not required.

## Execution boundaries

- Normal pytest, PR CI, and regular E2E runs do not collect this directory.
- Tests are collected only when `--run-spark-backfill` is passed explicitly.
- Every case uses the dedicated `SparkBackfill` tag and is outside the normal Nightly `L0 L1 L2 ClusterOnly` selection.
- pytest-xdist is not supported. Run with `-n 0` or omit `-n`.
- Every case creates an independent collection, snapshot, object-storage prefix, and Result path.
- In `job` mode, pytest creates, waits for, collects logs from, and deletes the Spark Job.
- In `toolbox` mode, pytest preserves the Toolbox Pod and writes independent evidence for each invocation. Tests must still run serially.
- V3 and V2 must run against separate Milvus deployments and be selected with separate markers.

## Runtime architecture

```text
pytest/developer machine/Jenkins Agent
  ├─ pymilvus: collection setup, data insertion, flush, snapshot, commit, and readback
  ├─ PyArrow: deterministic Parquet generation
  ├─ MinIO SDK: upload input, read Result, inspect artifacts, and clean prefixes
  └─ Kubernetes SDK
       ├─ toolbox mode: exec into an existing Toolbox Pod
       └─ job mode: create a one-shot Job Pod
            └─ spark-submit --master local[2]
                 ├─ BackfillApp
                 └─ PySpark Read Probe
```

## Toolbox mode (recommended for current development)

The Toolbox Pod must have completed the Connector build and be `Running 1/1`. The runner discovers it by this label by default:

```text
app=spark-milvus-toolbox
```

You can specify a Pod with `--spark-toolbox-pod`, but the name changes after a Pod rebuild. Label discovery is more stable.

When running pytest locally, first port-forward the ClusterIP services:

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig -n default \
  port-forward svc/eric-spark-milvus 19530:19530 19091:9091

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig -n default \
  port-forward svc/eric-spark-minio 19000:9000
```

Export the credentials needed by the local pytest MinIO client from the existing MinIO Secret:

```bash
export SPARK_BACKFILL_S3_ACCESS_KEY="$(
  kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig -n default \
    get secret eric-spark-minio -o jsonpath='{.data.accesskey}' | base64 -d
)"
export SPARK_BACKFILL_S3_SECRET_KEY="$(
  kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig -n default \
    get secret eric-spark-minio -o jsonpath='{.data.secretkey}' | base64 -d
)"
```

Run a minimal V3 coalesce Backfill:

```bash
python3 -m pytest -p no:rerunfailures \
  'tests/python_client/spark_backfill/test_v3_backfill_e2e.py::test_v3_backfill_modes_publish_and_become_visible[coalesce]' \
  --run-spark-backfill \
  --tags SparkBackfill \
  --spark-runner-mode toolbox \
  --uri http://127.0.0.1:19530 \
  --token 'root:Milvus' \
  --minio_host 127.0.0.1:19000 \
  --minio_bucket milvus-bucket \
  --management-endpoint http://127.0.0.1:19091 \
  --spark-k8s-context my-vcluster \
  --spark-k8s-namespace default \
  --spark-milvus-uri http://eric-spark-milvus:19530 \
  --spark-minio-endpoint eric-spark-minio:9000 \
  --spark-toolbox-label app=spark-milvus-toolbox \
  --spark-evidence-root /tmp/spark-backfill-evidence \
  -n 0 -v --tb=short
```

The Toolbox runner:

1. Finds the single Ready Toolbox Pod.
2. Checks the wrapper, Connector JAR, and native libraries.
3. Injects `contracts.py` and `read_probe.py` into `/workspace/spark-backfill-pytest`.
4. Reads runtime credentials from the Pod's `MILVUS_TOKEN`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY` environment variables.
5. Runs Spark and writes the redacted command, complete logs, exit code, and results to the evidence directory.
6. Does not delete or restart the Toolbox Pod.

Minimum Toolbox RBAC:

```text
get/list pods
get pods/exec
```

If pytest must read `--spark-storage-secret-name` because local AK/SK variables are absent, it also requires `get secrets`.

## Job-mode runtime

Pinned Spark image:

```text
apache/spark:4.0.1-scala2.13-java21-python3-ubuntu@sha256:fb5c5e61e7bb1be94b7f3a31afe1f73c5b4d20b6008f4ffa7278fc085da08a9e
```

The Job is pinned to Linux AMD64, `restartPolicy: Never`, `backoffLimit: 0`, 2 CPU / 8 GiB, and a default timeout of 30 minutes.

The remote entrypoint always adds `--packages org.apache.hadoop:hadoop-aws:3.4.1`. The Nightly namespace must be able to reach the Maven/Ivy repositories. If public egress is blocked, prewarm the Ivy cache or include compatible Hadoop AWS/AWS SDK JARs in the Spark image, then verify dependency resolution before integrating with Jenkins.

## Connector bundle contract for Job mode

Only `--spark-runner-mode job` needs a Connector bundle. `--spark-connector-url` must be an HTTPS `tar.gz` accessible from the Job Pod, and `--spark-connector-sha256` pins the archive contents. The archive must contain at least:

```text
manifest.json
connector-assembly.jar
lib/libmilvus-storage.so
lib/libmilvus-storage-jni.so
```

`manifest.json` must declare the Connector revision, file SHA256 values, Spark/Scala/Java versions, OS/architecture, Assembly JAR, and Backfill main class. Before starting Spark, the remote entrypoint verifies the archive SHA256, safe extraction, individual file SHA256 values, and runtime compatibility.

## Credentials

In both modes, the local pytest MinIO client reads credentials from:

```bash
export SPARK_BACKFILL_S3_ACCESS_KEY='...'
export SPARK_BACKFILL_S3_SECRET_KEY='...'
```

When `--spark-storage-secret-name` is not specified, `job` mode creates a temporary Kubernetes Secret from these variables. The Milvus token comes from `--token`. Credentials are injected through Secret-backed environment variables and are not written to the Job manifest, pytest evidence, or remote command logs.

`toolbox` mode does not create a Secret. The Spark process uses the Toolbox Pod's existing `MILVUS_TOKEN`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY`. The local variables are used only by pytest to upload Parquet, read Results, and clean objects.

When `--spark-storage-secret-name` is provided, that Secret should use the keys `s3-access-key`, `s3-secret-key`, and `milvus-token`. The two S3 keys may both be omitted in IAM mode. `milvus-token` is required when Milvus authentication is enabled.

The local MinIO SDK currently requires static AK/SK. Even when Spark uses IAM, pytest still needs static test credentials for upload, Result reads, and cleanup until the local object-storage client supports another authentication mode.

## Minimum Kubernetes RBAC for Job mode

The kubeconfig or ServiceAccount running pytest needs at least:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-backfill-nightly
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "get", "delete"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["create", "get", "delete"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["create", "delete"]
```

For an existing `--spark-storage-secret-name`, replace the Secret `create/delete` verbs with `get` when local AK/SK variables are absent and pytest must read the Secret. If both local credential variables are set, pytest does not read the Secret and the `get` verb may be omitted. Startup uses SelfSubjectAccessReview to fail fast on missing permissions.

## V3 deployment prerequisites

The online function-output case adds a function field to a collection that already has sealed rows. Milvus requires Storage V3, storage-version compaction, and schema-bump compaction to admit this DDL:

```yaml
common:
  storage:
    useLoonFFI: true
dataCoord:
  compaction:
    storageVersion:
      enabled: true
    bumpSchemaVersion:
      enabled: true
```

The test creates a protected snapshot immediately after the DDL so schema-bump compaction cannot replace the snapshot segments while Spark generates and commits the backfill result.

## Manual V3 run in Job mode

```bash
python3 -m pytest -p no:rerunfailures tests/python_client/spark_backfill \
  --run-spark-backfill \
  --tags SparkBackfill \
  --spark-runner-mode job \
  --host <milvus-host> \
  --port 19530 \
  --token 'root:Milvus' \
  --minio_host <agent-reachable-minio-host> \
  --minio_bucket <bucket> \
  --management-endpoint http://<management-host>:9091 \
  --spark-k8s-context <context> \
  --spark-k8s-namespace <namespace> \
  --spark-milvus-uri http://<pod-reachable-milvus>:19530 \
  --spark-minio-endpoint <pod-reachable-minio>:9000 \
  --spark-connector-url <public-bundle-url> \
  --spark-connector-sha256 <64-char-sha256> \
  -m "spark_backfill_v3 and (spark_backfill_core or spark_backfill_negative)" \
  -n 0 -v --tb=short
```

In some sandbox environments, the locally installed `pytest-rerunfailures` plugin attempts to open a local socket and is blocked. The development examples disable it with `-p no:rerunfailures`; Jenkins does not need to copy that flag unconditionally.

## Manual V2 run

V2 uses a separate deployment. At minimum, disable Loon FFI and Storage V3 upgrade compaction:

```yaml
common:
  storage:
    useLoonFFI: false
dataCoord:
  compaction:
    storageVersion:
      enabled: false
```

Run:

```bash
python3 -m pytest -p no:rerunfailures tests/python_client/spark_backfill \
  --run-spark-backfill \
  --tags SparkBackfill \
  <same-environment-options-as-above> \
  -m "spark_backfill_v2 and spark_backfill_core" \
  -n 0 -v --tb=short
```

The fixture reads the real `storage_version` from `MilvusClient.list_persistent_segments()`. The V2 suite fails immediately if a Snapshot contains V1, V3, mixed versions, or incomplete segment evidence. It does not infer the version from the legacy `storagev2_manifest_list` field name.

The V2 case runs two Backfills against the same field IDs. The second run uses different artifacts and values. After Commit, the test does not force a Reload and must observe the second values online. This verifies Column Group replacement, metadata propagation for a loaded segment, and the DataVersion/Reopen path. The public persistent-segment API does not currently expose DataVersion, so the test records segment evidence before and after Commit and verifies behavior. Add an explicit `after > before` assertion if the API exposes DataVersion later.

Connector Read for a specific Snapshot needs more than the Snapshot URL. It also needs `milvus.snapshot.manifests`, `milvus.snapshot.v2.segments`, and the snapshot schema bytes. The current bundle contract does not expose argument construction for `ReadSourceOnlyApp` or `ListV2SegmentsApp`, so the initial V2 suite uses pymilvus for online readback rather than misclassifying the known live client-mode V2 planner limitation as a Backfill write failure.

## Evidence and cleanup

Each Spark invocation writes an independent directory under `--spark-evidence-root`, which defaults to:

```text
${CI_LOG_PATH:-/tmp/ci_logs}/spark_backfill/<job-name>/
```

It contains:

- A redacted Job manifest in Job mode, or a redacted exec command in Toolbox mode.
- Complete redacted Pod logs, exit code, and failure reason.
- Raw Snapshot metadata.
- Backfill Result JSON.
- Object listings from the Result directory.
- Manifest/Column Group paths and object sizes.
- Commit response and per-segment status.
- Persistent segment evidence before and after V2 commits.

On normal completion, both modes delete the test collection, snapshots, and object prefix. Job mode also deletes the ConfigMap, temporary Secret, and Job. Toolbox mode always preserves the Toolbox Pod. `--spark-keep-failed-job` affects Job mode only.

## Nightly integration

Nightly should not depend on a daily spark-milvus release. Use two stages:

```text
Builder/Toolbox Pod
  → checkout a pinned spark-milvus commit
  → compile Connector/JNI/native libraries in the target environment
  → verify artifact readiness

pytest
  → --spark-runner-mode toolbox
  → connect to the Builder/Toolbox Pod by label
  → run Read/Backfill cases
```

The Toolbox runner does not manage the Pod lifecycle. Jenkins or a dedicated fixture may use a Deployment, Job + PVC, or prebuilt image. Recommended workflow:

1. Jenkins provides the kubeconfig/ServiceAccount, Milvus/MinIO/Management endpoints, and credentials.
2. The builder checks out an explicit Connector commit, starts the Toolbox, and waits for `Running 1/1`.
3. pytest uses the Toolbox label and does not require a Connector URL/SHA.
4. Run the complete V3 core + negative selection every night.
5. After the V2 environment is available, run the separate V3 and V2 deployments sequentially. Never mix them in one deployment.
6. Archive JUnit, the complete evidence root, builder logs, and pytest console logs.

Check the normal collection gate with:

```bash
python3 -m pytest -p no:rerunfailures tests/python_client/spark_backfill --collect-only -q
```

Explicitly selecting this directory may return pytest exit code 5 because zero tests were collected. That is pytest's standard behavior for an empty selection. During normal repository collection, this directory is ignored and other tests continue to collect normally.
