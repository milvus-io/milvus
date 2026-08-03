# Spark-Milvus Backfill Pipeline Handoff

## 1. Purpose

This document hands off the remaining CI/CD work needed to run the Spark-Milvus Backfill E2E suite in a dedicated pipeline.

The suite must not become part of the existing general PR CI, Milvus Nightly CI, weekly jobs, or the normal `tests/python_client` pytest run. It requires a specially prepared Spark Toolbox Pod, serial execution, and separate Milvus deployments for Storage V3 and Storage V2.

The primary operational references are:

- [Spark Backfill pytest README](README.md): pytest architecture, command-line options, credentials, runner modes, evidence, cleanup, and V2/V3 execution requirements.
- [Spark Toolbox deployment runbook](deploy/manual_toolbox/README.md): validated Kubernetes deployment procedure, connector build environment, readiness checks, troubleshooting, and cleanup.
- [Manual toolbox deployment](deploy/manual_toolbox/deployment.yaml): current Deployment template.
- [Connector build script](deploy/manual_toolbox/build-connector.sh): source checkout and build steps for `zilliztech/spark-milvus`.
- [Spark wrapper](deploy/manual_toolbox/spark-submit-milvus.sh): required Spark submit and classloader configuration.

## 2. Decisions already made

The following decisions were agreed before this handoff:

- The dedicated pipeline targets the Milvus `master` branch only.
- The Spark connector source is [zilliztech/spark-milvus](https://github.com/zilliztech/spark-milvus).
- The connector must be built inside a Kubernetes pod. The existing Spark Toolbox implementation is the starting point.
- General pipelines use pytest tags including `L0`, `L1`, `L2`, and `ClusterOnly`.
- Spark Backfill unit-test modules were removed. Only the three E2E modules remain.
- All Spark Backfill tests use the dedicated pytest tag `SparkBackfill`, which is outside the general nightly tag list.
- Existing custom markers remain the authoritative V2/V3 and scenario selectors.
- Jenkins/shared-library changes, schedules, credentials, cluster allocation, and notification configuration will be completed by the DevOps/pipeline owners.

## 3. Current test inventory

Only these test modules remain:

| Module | Tag | Important markers | Collected cases |
| --- | --- | --- | ---: |
| `test_v3_backfill_e2e.py` | `SparkBackfill` | `spark_backfill_v3`, `spark_backfill_core` | 19 |
| `test_v3_negative_e2e.py` | `SparkBackfill` | `spark_backfill_v3`, `spark_backfill_negative` | 11 |
| `test_v2_backfill_e2e.py` | `SparkBackfill` | `spark_backfill_v2`, `spark_backfill_core` | 1 |

Total: 31 E2E cases.

The suite also uses these markers for narrower selections:

- `spark_e2e`: requires a real remote Spark runtime.
- `spark_backfill_compaction`: compaction-protection and stale-result scenarios.
- `spark_backfill_known_gap`: strict expected failures for known server-side protection gaps.

## 4. Collection boundary and CI exclusion

### 4.1 Current repository gate

The parent `tests/python_client/conftest.py` ignores the `spark_backfill` directory unless pytest receives:

```text
--run-spark-backfill
```

Therefore, a normal recursive pytest invocation does not collect this suite, even when it uses the normal nightly tag list.

Expected safety check:

```bash
python3 -m pytest -p no:rerunfailures \
  tests/python_client/spark_backfill \
  --collect-only -q \
  --tags L0 L1 L2 ClusterOnly
```

Expected result: zero tests collected. When this directory is the only requested path, pytest returns exit code 5 because the selected suite is empty. That exit code is expected for this safety check.

### 4.2 Required general-pipeline protection

The DevOps owner should apply defense in depth in the external Jenkins shared library and any other general pytest wrappers:

1. Never pass `--run-spark-backfill` from a general PR, nightly, weekly, ARM, GPU, or ordinary E2E job.
2. Add an explicit ignore to general pytest commands:
   - From the repository root: `--ignore=tests/python_client/spark_backfill`
   - From `tests/python_client`: `--ignore=spark_backfill`
3. Do not generate an explicit list of Spark Backfill filenames or pytest node IDs in a general job.
4. Keep the dedicated Spark Backfill job separate from the shared general `ciMode=e2e` and `ciMode=nightly` selection logic.

Known caveat: pytest's current directory-ignore hook protects recursive discovery, but pytest may still collect a file that is explicitly named on the command line. The explicit pipeline `--ignore` and the rule against enumerating these files are therefore important. A future repository-side hardening change may add a second item-level deselection gate.

## 5. Recommended pipeline architecture

Use one dedicated master-only pipeline with serial V3 and V2 phases. V3 and V2 must not run against the same Milvus deployment.

Suggested stages:

1. **Resolve revisions**
   - Check out Milvus `master` at the revision under test.
   - Resolve and record an explicit `zilliztech/spark-milvus` commit.
   - Record the Milvus image, Helm chart, connector commit, Spark image digest, and pipeline build ID.
2. **Deploy Storage V3 Milvus**
   - Deploy a dedicated Milvus release and object store.
   - Expose Milvus client port 19530, management port 9091, and MinIO port 9000 to the pytest agent as required.
3. **Build and start the V3 Spark Toolbox**
   - Create the credentials Secret and scripts ConfigMap.
   - Start one toolbox Deployment with label `app=spark-milvus-toolbox`.
   - Build the selected connector commit in the init container.
   - Wait for the toolbox to become `Running 1/1` and complete all readiness checks in Section 8.
4. **Run V3 E2E**
   - Run the 30 V3 cases serially.
   - Archive JUnit, pytest console output, toolbox build logs, and the complete Spark Backfill evidence directory.
5. **Clean up V3**
   - Always remove the V3 Milvus release and its toolbox resources after evidence collection.
6. **Deploy Storage V2 Milvus**
   - Use a new release/namespace or a fully cleaned and independently configured environment.
   - Apply the required Storage V2 settings described in Section 9.
7. **Build or attach a V2 Spark Toolbox**
   - The same connector revision may be reused only if the toolbox can reach the V2 services and uses the correct credentials/endpoints.
   - The safest first implementation is to recreate or separately configure the toolbox for the V2 environment.
8. **Run V2 E2E**
   - Run the single V2 case serially.
   - Archive the same evidence categories as V3.
9. **Final cleanup and reporting**
   - Cleanup must run in `always`/`finally` handling.
   - Publish results and notify the configured owners.

Do not run V3 and V2 in parallel. Do not enable pytest-xdist.

## 6. Information the DevOps engineer must supply

### 6.1 Jenkins and source control

- Jenkins job name and Jenkinsfile/shared-library location.
- Master-only trigger policy: manual, cron, source change, or another event.
- Jenkins Kubernetes cloud and agent template.
- Git credentials, if required to access `zilliztech/spark-milvus`.
- Connector revision policy:
  - fixed commit;
  - connector default branch HEAD;
  - a pipeline parameter;
  - or a revision derived from another release process.
- Concurrency policy. A single environment must not be shared by concurrent pipeline runs.

### 6.2 Milvus deployments

- Milvus image tag/digest under test.
- Helm chart version and values source.
- Unique V3 and V2 release names and namespaces.
- Service names for Milvus, management API, and MinIO.
- V3 storage configuration.
- V2-specific Helm overrides.
- Cleanup ownership and maximum environment lifetime.

### 6.3 Kubernetes and networking

- Kubeconfig or in-cluster ServiceAccount presented to pytest.
- Namespace where the toolbox will run.
- AMD64 node availability.
- At least 4 CPU and 12 GiB schedulable for the build init container.
- At least 2 CPU and 8 GiB for the Spark toolbox container.
- Network access from the pytest agent to Milvus 19530, management 9091, and MinIO 9000.
- Network access from the toolbox pod to the Milvus and MinIO Kubernetes Services.
- Build-time egress to Ubuntu repositories, GitHub, JFrog/Conan, SDKMAN, Maven/Ivy, pip, Rust registries, and source archives used by the build script.
- Protection from autoscaler eviction or vcluster sleep during the connector build and test run.

### 6.4 Credentials and secrets

- Milvus token.
- MinIO access key and secret key for the local pytest process.
- MinIO credentials exposed to the toolbox pod, or an approved IAM alternative.
- Jenkins credential IDs and masking rules.
- A decision on whether the pipeline creates temporary Secrets or consumes existing ones.

The local pytest process currently requires static S3 credentials through:

```text
SPARK_BACKFILL_S3_ACCESS_KEY
SPARK_BACKFILL_S3_SECRET_KEY
```

The toolbox container expects:

```text
MILVUS_TOKEN
S3_ACCESS_KEY
S3_SECRET_KEY
```

### 6.5 Results and operations

- Per-test and total pipeline timeout.
- Artifact storage and retention duration.
- JUnit publication location.
- Notification recipients or on-call routing.
- Whether failed toolbox pods are retained temporarily for diagnosis.
- Log redaction requirements.

## 7. Spark Toolbox deployment contract

### 7.1 Current validated implementation

The current Deployment uses one init container and one long-running main container:

```text
build-connector init container
  -> clones zilliztech/spark-milvus
  -> checks out CONNECTOR_COMMIT
  -> builds milvus-storage, JNI/native libraries, and the assembly JAR
  -> copies artifacts to the shared /artifacts volume

spark-toolbox main container
  -> mounts the artifacts read-only at /opt/spark-milvus
  -> remains available for Kubernetes exec
  -> runs each test through /usr/local/bin/spark-submit-milvus
```

Current pinned runtime:

| Component | Value |
| --- | --- |
| Spark image | `apache/spark:4.0.1-scala2.13-java21-python3-ubuntu@sha256:fb5c5e61e7bb1be94b7f3a31afe1f73c5b4d20b6008f4ffa7278fc085da08a9e` |
| Spark | 4.0.1 |
| Scala | 2.13.16 |
| Java | 21 |
| Rust | 1.96.0 |
| Hadoop AWS package | `org.apache.hadoop:hadoop-aws:3.4.1` |
| OS/architecture | Linux AMD64 |
| Validated connector commit | `dfcec3d564e78be771b6d41fc04632db77d8d507` |

The validated connector commit is a known-good starting point, not necessarily the final pipeline revision policy.

### 7.2 Stable discovery contract

The pytest runner discovers exactly one Ready pod using:

```text
label:     app=spark-milvus-toolbox
container: spark-toolbox
wrapper:   /usr/local/bin/spark-submit-milvus
workspace: /workspace/spark-backfill-pytest
```

Prefer label discovery over a fixed pod name because Deployment pod names change after recreation.

### 7.3 Minimum pytest-side Kubernetes RBAC

Toolbox mode requires:

```text
get/list pods
get pods/exec
```

Additional permissions are needed if the pipeline itself creates the toolbox Deployment, ConfigMap, or Secret.

### 7.4 Current source-build limitations

A clean source build has taken approximately 56 minutes in the validated environment. The build and dependency caches currently use `emptyDir`, so they are lost when the pod is recreated.

For the first pipeline implementation, the existing source-build pod is acceptable if the timeout, egress, and node stability requirements are satisfied. For a stable recurring pipeline, the runbook recommends building a digest-pinned Spark Toolbox image that already contains the connector JAR, native libraries, Python packages, and prewarmed Hadoop AWS/Ivy dependencies.

## 8. Mandatory toolbox readiness checks

Do not treat `Running 1/1` alone as sufficient. Before pytest starts, verify:

1. The init container completed with exit code 0.
2. Spark reports Spark 4.0.1, Scala 2.13.16, and Java 21.
3. These artifacts exist:
   - `/opt/spark-milvus/jars/spark-connector-assembly.jar`
   - `/opt/spark-milvus/native/libmilvus-storage.so`
   - `/opt/spark-milvus/native/libmilvus-storage-jni.so`
4. Connector revision and SHA256 evidence are archived.
5. `ldd` reports no missing native libraries.
6. The pod can connect to Milvus 19530, management API 9091 where applicable, and MinIO 9000.
7. The wrapper can run the Spark Pi example with exit code 0.
8. Exactly one Ready toolbox pod matches the configured label.

All Read and Backfill operations must use the supplied wrapper. Do not invoke raw `spark-submit` with an independently assembled set of arguments.

Important wrapper invariants:

```text
--master local[2]
--packages org.apache.hadoop:hadoop-aws:3.4.1
--exclude-packages software.amazon.awssdk:bundle
spark.driver.userClassPathFirst=true
spark.executor.userClassPathFirst=false
```

Changing `spark.executor.userClassPathFirst` to `true` has previously caused duplicate `BackfillConfig` classloading failures.

## 9. Milvus environment requirements

### 9.1 Storage V3

The V3 phase must produce real Storage V3 persistent segments. The test fixture inspects persistent segment metadata and fails if the expected storage kind is not present.

The phase runs the V3 core, negative, compaction, and known-gap cases selected from the two V3 modules.

### 9.2 Storage V2

The V2 phase requires an independent Milvus deployment with at least:

```yaml
common:
  storage:
    useLoonFFI: false
dataCoord:
  compaction:
    storageVersion:
      enabled: false
```

The fixture validates the actual storage versions of the snapshot's persistent segments. V1, V3, mixed, or incomplete segment evidence causes an immediate failure.

## 10. Endpoint model

Several endpoints have two forms. The pipeline must not assume the pytest agent and toolbox pod use the same address.

| Purpose | Pytest-agent address | Toolbox-pod address |
| --- | --- | --- |
| Milvus client API | `--uri` or `--host`/`--port` | `--spark-milvus-uri` |
| MinIO | `--minio_host` | `--spark-minio-endpoint` |
| Management API | `--management-endpoint` | Normally called by pytest; must be agent-reachable |
| Kubernetes | kubeconfig/context or in-cluster config | Kubernetes Service networking |

The pytest agent uses pymilvus and the MinIO SDK to prepare input, create snapshots, commit results, validate online visibility, and clean up. The toolbox pod runs Spark against pod-reachable Kubernetes Service addresses.

## 11. Suggested pytest commands

Replace all angle-bracket placeholders with pipeline-provided values. Both phases must run with `-n 0` or without `-n`.

### 11.1 V3

```bash
python3 -m pytest tests/python_client/spark_backfill \
  --run-spark-backfill \
  --spark-runner-mode toolbox \
  --uri <agent-reachable-milvus-uri> \
  --token <milvus-token> \
  --minio_host <agent-reachable-minio-host:port> \
  --minio_bucket <bucket> \
  --management-endpoint <agent-reachable-management-url> \
  --spark-k8s-context <context-if-needed> \
  --spark-k8s-namespace <namespace> \
  --spark-milvus-uri <pod-reachable-milvus-uri> \
  --spark-minio-endpoint <pod-reachable-minio-host:port> \
  --spark-toolbox-label app=spark-milvus-toolbox \
  --spark-evidence-root <evidence-directory> \
  --tags SparkBackfill \
  -m "spark_backfill_v3 and (spark_backfill_core or spark_backfill_negative)" \
  -n 0 -v --tb=short \
  --junitxml=<v3-junit-path>
```

Expected collection: 30 cases.

### 11.2 V2

```bash
python3 -m pytest tests/python_client/spark_backfill \
  --run-spark-backfill \
  --spark-runner-mode toolbox \
  --uri <agent-reachable-v2-milvus-uri> \
  --token <milvus-token> \
  --minio_host <agent-reachable-v2-minio-host:port> \
  --minio_bucket <bucket> \
  --management-endpoint <agent-reachable-v2-management-url> \
  --spark-k8s-context <context-if-needed> \
  --spark-k8s-namespace <namespace> \
  --spark-milvus-uri <pod-reachable-v2-milvus-uri> \
  --spark-minio-endpoint <pod-reachable-v2-minio-host:port> \
  --spark-toolbox-label app=spark-milvus-toolbox \
  --spark-evidence-root <evidence-directory> \
  --tags SparkBackfill \
  -m "spark_backfill_v2 and spark_backfill_core" \
  -n 0 -v --tb=short \
  --junitxml=<v2-junit-path>
```

Expected collection: 1 case.

## 12. Evidence and cleanup

Archive the entire directory passed through `--spark-evidence-root`. It contains per-Spark-invocation command metadata, redacted logs, exit status, snapshots, Backfill results, artifact inventories, commit responses, and segment evidence.

The pipeline should additionally archive:

- JUnit XML for V3 and V2;
- pytest console logs;
- toolbox init-container build logs;
- toolbox Deployment and pod descriptions;
- connector revision and artifact SHA256 files;
- Milvus pod logs when test or infrastructure failures require them;
- Helm values used for each deployment.

Pytest normally removes its collections, snapshots, and object-store prefixes. The pipeline remains responsible for deleting Milvus releases, toolbox Deployments, ConfigMaps, Secrets, PVCs if introduced, and any resources left after an interrupted run.

Cleanup must not run before evidence is archived.

## 13. Pipeline acceptance criteria

The handoff is complete when the DevOps implementation demonstrates all of the following:

### General pipeline isolation

- General PR/nightly/weekly jobs do not pass `--run-spark-backfill`.
- General pytest commands explicitly ignore `tests/python_client/spark_backfill`.
- A normal collection run selects zero Spark Backfill cases.
- The dedicated job is not accidentally included in the generic nightly matrix.

### Dedicated pipeline collection

- The complete dedicated tag list collects 31 cases.
- V3 selection collects 30 cases.
- V2 selection collects 1 case.
- Any use of pytest-xdist is rejected or absent.

### Runtime validation

- The connector commit is explicit and recorded.
- The toolbox passes every readiness check in Section 8.
- V3 runs on real Storage V3 segments.
- V2 runs on real Storage V2 segments in a separate deployment.
- V3 and V2 are serial.
- Credentials do not appear in console logs or archived command metadata.
- Failure paths still archive enough evidence to diagnose connector, Spark, Milvus, MinIO, or infrastructure errors.
- Cleanup succeeds after pass, failure, timeout, and aborted builds.

## 14. Ownership boundary

### Completed in the Milvus repository

- Spark Backfill pytest implementation and toolbox runner.
- Manual toolbox deployment assets and validated runbook.
- Removal of the standalone unit-test modules from this suite.
- Dedicated `SparkBackfill` tag for all suite cases.
- V3/V2 marker separation.
- Default opt-in gate using `--run-spark-backfill`.

### Remaining for DevOps/pipeline owners

- Jenkins job and external shared-library implementation.
- Explicit exclusion in all general pipelines.
- Master-only trigger and concurrency policy.
- Milvus V3/V2 deployment automation.
- Spark Toolbox lifecycle and connector revision selection.
- Kubernetes resources, RBAC, credentials, networking, and egress.
- Artifact publication, retention, timeout, notifications, and cleanup guarantees.
- End-to-end execution and acceptance evidence.

### Remaining joint decision

The Milvus, DevOps, and Spark connector owners should agree on whether the recurring pipeline continues building the connector in an init container or moves to a digest-pinned prebuilt toolbox image. The source-build pod is already validated, while a prebuilt image is the recommended stable long-term model.
