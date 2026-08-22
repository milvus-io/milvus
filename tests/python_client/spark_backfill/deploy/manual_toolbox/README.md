# Repeatable Spark-Milvus Toolbox Deployment Runbook

This runbook records the Spark-Milvus Toolbox deployment flow that was validated in a real vcluster. The goal is to deploy a temporary Spark workstation next to an existing Milvus and MinIO installation and use it for manual Spark Connector Read and Storage V3 Backfill validation.

The document covers two deployment levels:

1. **Currently validated approach**: build the Connector from source in an init container whenever the Pod starts. This is suitable for feature development and temporary testing.
2. **Long-term stable approach**: prebuild an image that contains the Connector so runtime Pods do not compile from source. Use this for frequent rebuilds, Nightly, or CI.

## 1. What the current approach provides

Existing environment:

```text
Milvus:            eric-spark-milvus:19530
Management API:    eric-spark-milvus:9091
MinIO:             eric-spark-minio:9000
Namespace:         default
```

The deployment adds this Kubernetes Deployment:

```text
spark-milvus-toolbox
```

The Pod contains:

```text
init container: build-connector
  └─ downloads and compiles spark-milvus, milvus-storage, JNI, and native dependencies

main container: spark-toolbox
  └─ keeps Spark, the Connector, and test dependencies available for manual spark-submit runs
```

Spark does not use a Master/Worker cluster. Jobs run inside the single Pod with:

```text
spark-submit --master local[2]
```

## 2. Validated versions

Do not upgrade any item casually. After an upgrade, repeat the complete acceptance procedure in this document.

| Item | Pinned version |
| --- | --- |
| Spark image | `apache/spark:4.0.1-scala2.13-java21-python3-ubuntu@sha256:fb5c5e61e7bb1be94b7f3a31afe1f73c5b4d20b6008f4ffa7278fc085da08a9e` |
| Spark | `4.0.1` |
| Scala | `2.13.16` |
| Java | `21.0.8` |
| Connector commit | `dfcec3d564e78be771b6d41fc04632db77d8d507` |
| Rust | `1.96.0` |
| Hadoop AWS package | `org.apache.hadoop:hadoop-aws:3.4.1` |
| OS/architecture | Linux AMD64 |

Spark comes from the official Apache Scala 2.13 image and is not installed through SDKMAN. SDKMAN installs only Scala `2.13.16` and SBT `1.11.1`, which are needed to build the Connector.

Validated artifact SHA256 values:

```text
9218e0fe462e7f0b24d4579878d1b3171d60c64975bbc672777704a670c5461b  spark-connector-assembly.jar
1f69303f1dce46897cc5138781ade1663c03dee7019bcbdeec184766be237404  libmilvus-storage.so
8ac8dbab9d36635d546b138a4e75d0ce440109d52a1c9b847e1a4620db4017cf  libmilvus-storage-jni.so
```

## 3. File responsibilities

| File | Responsibility |
| --- | --- |
| `deployment.yaml` | Defines the Toolbox Deployment, init container, main container, resources, and volumes |
| `deploy.sh` | Creates the Secret and ConfigMap, applies the Deployment, and waits for Ready |
| `build-connector.sh` | Installs tools, downloads source, and builds the Connector/JNI/native runtime |
| `spark-submit-milvus.sh` | Starts Spark with the validated JAR, native library, S3A, and classloader options |

Run every command from the Milvus repository root.

## 4. Required inputs before deployment

### 4.1 kubeconfig and namespace

Validated values:

```text
Kubeconfig: /Users/zilliz/Desktop/kubecon/kubeconfig
Namespace:  default
```

Check connectivity:

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig get nodes

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default get pods
```

All nodes must be `Ready`.

### 4.2 Milvus Service

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default get svc eric-spark-milvus
```

The Service must expose:

```text
19530/TCP  Milvus Client API
9091/TCP   Management API
```

If the Milvus release is not named `eric-spark`, update this value in `deployment.yaml`:

```yaml
MILVUS_URI: http://<release>-milvus:19530
```

### 4.3 MinIO Service and Secret

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default get svc eric-spark-minio

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default get secret eric-spark-minio
```

The current configuration expects these Secret keys:

```text
accesskey
secretkey
```

Current object-storage settings:

```text
Endpoint:  eric-spark-minio:9000
Bucket:    milvus-bucket
Root path: file
SSL:       false
```

If the Service, Secret, bucket, or root path differs, update `deployment.yaml` before deploying.

### 4.4 Node resources

Build init container:

```text
request: 4 CPU / 12 GiB
limit:   6 CPU / 16 GiB
```

Spark main container:

```text
request: 2 CPU / 8 GiB
limit:   2 CPU / 8 GiB
```

The namespace or cluster must have an AMD64 node that can satisfy the init-container request.

### 4.5 External network access

The source build needs access to:

- Ubuntu apt repositories.
- GitHub.
- The Milvus JFrog Conan remote.
- SDKMAN.
- Maven/Ivy.
- A pip package index.
- The Rust toolchain and crate registry.

The current script contains compatibility workarounds for bzip2, Boost, Thrift, and Arrow source downloads. Do not remove them without repeating the full build validation.

## 5. Deploy

Run from the repository root:

```bash
bash tests/python_client/spark_backfill/deploy/manual_toolbox/deploy.sh \
  /Users/zilliz/Desktop/kubecon/kubeconfig \
  default
```

The script creates or updates:

```text
Secret:     spark-milvus-toolbox-credentials
ConfigMap:  spark-milvus-toolbox-scripts
Deployment: spark-milvus-toolbox
```

It then waits up to 180 minutes for the Deployment to become Ready.

## 6. Observe deployment progress

Find the Pod:

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default get pod -l app=spark-milvus-toolbox -o wide
```

If the status is:

```text
Init:0/1
```

the Pod has started but the Connector is still building. Follow the init-container logs:

```bash
POD=$(kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default get pod -l app=spark-milvus-toolbox \
  -o jsonpath='{.items[0].metadata.name}')

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default logs -f "$POD" -c build-connector
```

A clean build without cache took approximately 56 minutes during validation:

```text
started:  2026-07-24 13:44:25 CST
finished: 2026-07-24 14:40:33 CST
exit:     0
```

CPU activity from `gcc`, `g++`, `rustc`, `cmake`, or `sbt` usually means the build is still progressing.

The final successful status must be:

```text
READY   STATUS    RESTARTS
1/1     Running   0
```

## 7. Mandatory acceptance after deployment

A `Running` Pod alone is insufficient. The Toolbox is usable only after every check below passes.

### 7.1 Spark/Scala/Java versions

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default exec "$POD" -- /opt/spark/bin/spark-submit --version
```

Expected:

```text
Spark 4.0.1
Scala 2.13.16
Java 21
```

### 7.2 Connector artifacts

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default exec "$POD" -- bash -lc '
    test -f /opt/spark-milvus/jars/spark-connector-assembly.jar
    test -f /opt/spark-milvus/native/libmilvus-storage.so
    test -f /opt/spark-milvus/native/libmilvus-storage-jni.so
  '
```

### 7.3 SHA256

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default exec "$POD" -- sha256sum \
  /opt/spark-milvus/jars/spark-connector-assembly.jar \
  /opt/spark-milvus/native/libmilvus-storage.so \
  /opt/spark-milvus/native/libmilvus-storage-jni.so
```

The output must match the SHA256 values in section 2.

### 7.4 Native library closure

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default exec "$POD" -- bash -lc '
    ldd /opt/spark-milvus/native/libmilvus-storage.so
    ldd /opt/spark-milvus/native/libmilvus-storage-jni.so
  '
```

The output must not contain:

```text
not found
```

### 7.5 Milvus and MinIO network connectivity

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default exec "$POD" -- python3 -c '
import socket
for host, port in [
    ("eric-spark-milvus", 19530),
    ("eric-spark-milvus", 9091),
    ("eric-spark-minio", 9000),
]:
    socket.create_connection((host, port), timeout=5).close()
    print(f"OK {host}:{port}")
'
```

### 7.6 Minimal Spark Job

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default exec "$POD" -- \
  /usr/local/bin/spark-submit-milvus \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples_2.13-4.0.1.jar \
  10
```

The command must exit with code 0 and print the Pi result.

## 8. Validated Spark launch configuration

Start every Read and Backfill through:

```text
/usr/local/bin/spark-submit-milvus
```

Do not bypass the wrapper and assemble different options manually. The validated settings are:

```text
--master local[2]
--packages org.apache.hadoop:hadoop-aws:3.4.1
--exclude-packages software.amazon.awssdk:bundle

spark.driver.userClassPathFirst=true
spark.executor.userClassPathFirst=false
```

Do not change `spark.executor.userClassPathFirst=false` back to `true`. Backfill previously failed because the Driver and Executor loaded separate copies of `BackfillConfig`:

```text
BackfillConfig cannot be cast to BackfillConfig
```

For a JVM `--class` application, the Connector JAR is the main application JAR. Do not also add the same JAR through `--jars`.

MinIO/S3A must use:

```text
fs.use_ssl=false
fs.use_virtual_host=false
fs.region=us-east-1
```

If the Snapshot API returns a relative object key, normalize it before running Backfill:

```text
s3a://<bucket>/<snapshot-key>
```

## 8.1 Connect pytest directly to the Toolbox

Spark Backfill pytest can reuse this Pod through Kubernetes exec and does not need a Connector bundle URL/SHA. Discover the Pod by its stable label:

```bash
python3 -m pytest -p no:rerunfailures \
  'tests/python_client/spark_backfill/test_v3_backfill_e2e.py::test_v3_backfill_modes_publish_and_become_visible[coalesce]' \
  --run-spark-backfill \
  --tags SparkBackfill \
  --spark-runner-mode toolbox \
  --spark-k8s-context my-vcluster \
  --spark-k8s-namespace default \
  --spark-toolbox-label app=spark-milvus-toolbox \
  <local-Milvus-MinIO-Management-and-in-Pod-Service-options> \
  -n 0 -v --tb=short
```

For debugging, you may pin a Pod:

```text
--spark-toolbox-pod spark-milvus-toolbox-<replicaset>-<suffix>
```

The pinned name becomes invalid after a Pod rebuild, so Nightly and routine repeated runs should use label discovery.

The runner writes these files dynamically:

```text
/workspace/spark-backfill-pytest/contracts.py
/workspace/spark-backfill-pytest/read_probe.py
```

There is no need to modify the ConfigMap or rebuild the Toolbox. The runner executes Spark but does not manage the Deployment lifecycle.

## 9. Why the current approach is not fully stable

These directories are `emptyDir` volumes:

```text
/build
/artifacts
/root/.conan
/root/.sdkman
/root/.cache
/root/.sbt
/root/.ivy2
/workspace
```

`emptyDir` has the same lifecycle as the Pod. All build output is lost after:

- Manual Pod deletion.
- Deployment rollout.
- Node scale-down.
- Pod recreation after vcluster sleep/wake.
- A Pod-template change.
- Node failure or eviction.

After a Pod rebuild, the complete source build runs again and takes about an hour. This is expected for the current architecture and does not mean Kubernetes is stuck.

Do not delete the Pod or modify the Deployment during the build. If the cluster autoscaler may scale the node down, add at least this annotation to the Pod template:

```yaml
metadata:
  annotations:
    cluster-autoscaler.kubernetes.io/safe-to-evict: "false"
```

The annotation only reduces the chance of an autoscaler-initiated eviction during the build. It cannot protect against a full vcluster sleep or node failure.

## 10. Recommended paths to a stable deployment

### Option A: Keep the current source-build Pod

Use for one-time development validation.

Advantages:

- Does not require an image registry.
- Allows a rebuild immediately after a Connector change.

Disadvantages:

- Each new Pod takes about an hour.
- Strongly depends on public networks and third-party download endpoints.
- A Pod rebuild loses every cache.

### Option B: Persist caches and artifacts with a PVC

Use for short-term iterative development.

Persist at least:

```text
/root/.conan
/root/.cache
/root/.sbt
/root/.ivy2
/artifacts
```

Advantage: a rebuilt Pod can reuse most dependencies.

Disadvantage: the init container must still check and build artifacts, and the PVC introduces access-mode, node-mount, and stale-cache consistency concerns.

### Option C: Prebuild a Connector Spark image (recommended)

Build the image once and include:

```text
/opt/spark-milvus/jars/spark-connector-assembly.jar
/opt/spark-milvus/native/*.so
/opt/spark-milvus/python/*
prewarmed Hadoop AWS/Ivy dependencies
```

Pin the image by digest. Runtime Pods no longer contain the `build-connector` init container and perform only SHA256 and `ldd` readiness checks after startup.

Advantages:

- Pods usually become Ready in minutes or seconds.
- Runtime no longer depends on public downloads or source repositories.
- Every run uses identical Connector artifacts.
- This is the best fit for CI, Nightly, and shared use.

This is the preferred long-term direction for repeatable deployments.

## 11. Common states and handling

| State | Meaning | Check |
| --- | --- | --- |
| `Pending` with no Node | Scheduling failed, usually because CPU/memory is insufficient or the nodeSelector does not match | Use `kubectl describe pod` and inspect `FailedScheduling` |
| `Init:0/1` with init Running | The Connector is building | Inspect `build-connector` logs and CPU processes |
| `Init:CrashLoopBackOff` | The build script failed | Inspect current and `--previous` logs |
| `PodInitializing` | The init container finished or Kubernetes is switching to the main container | Wait a few seconds and inspect Events |
| `Running 0/1` | The main container started, but artifacts or readiness checks are incomplete | Inspect `/opt/spark-milvus` and the readiness probe |
| `Running 1/1` | Kubernetes-level readiness passed | Continue with the functional acceptance checks in section 7 |

Build failure logs:

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default logs "$POD" -c build-connector --tail=300

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default logs "$POD" -c build-connector --previous --tail=300
```

Pod Events:

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default describe pod "$POD"
```

## 12. ConfigMap update caveat

`spark-submit-milvus.sh` is mounted from the ConfigMap through `subPath` at:

```text
/usr/local/bin/spark-submit-milvus
```

After a ConfigMap update, the `subPath` file in a running Pod does not refresh automatically. Rebuild the Pod to update the official path, but remember that rebuilding deletes `emptyDir` and recompiles the Connector.

During debugging, copy a new wrapper temporarily to:

```text
/workspace/spark-submit-milvus
```

and run it from that path. This is only a debugging technique and is not a repeatable deployment procedure.

## 13. Cleanup

Delete only the Spark Toolbox. These commands do not delete the Milvus release:

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default delete deployment spark-milvus-toolbox

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default delete configmap spark-milvus-toolbox-scripts

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default delete secret spark-milvus-toolbox-credentials
```

Deleting the Deployment or Pod permanently removes the Connector artifacts and build caches stored in the current `emptyDir` volumes.

## 14. Final checklist for every deployment

Before deployment:

- [ ] kubeconfig works and nodes are Ready.
- [ ] The namespace is correct.
- [ ] The Milvus 19530/9091 Service name is correct.
- [ ] The MinIO 9000 Service name is correct.
- [ ] The MinIO Secret and key names are correct.
- [ ] The bucket and root path are correct.
- [ ] An AMD64 node can provide at least 4 CPU / 12 GiB.
- [ ] vcluster sleep will not trigger during the build.
- [ ] External dependencies are reachable.

After deployment:

- [ ] The init container exit code is 0.
- [ ] The Pod is `Running 1/1`.
- [ ] Spark/Scala/Java versions are correct.
- [ ] The Connector revision is correct.
- [ ] The three primary artifact SHA256 values are correct.
- [ ] `ldd` contains no `not found`.
- [ ] Milvus, the Management API, and MinIO are reachable.
- [ ] Spark Pi exits with code 0.
- [ ] Connector Read smoke passes.
- [ ] Backfill Result, Commit segment status, and online visibility all pass.

The deployment is not considered stable until every applicable item is verified.
