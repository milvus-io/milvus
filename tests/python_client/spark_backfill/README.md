# Spark-Milvus Backfill pytest

本目录实现 Spark-Milvus Read/Backfill pytest。它支持两种 Spark 执行模式：

- `toolbox`：通过 Kubernetes exec 复用一个已经现场编译好 Connector 的 Toolbox Pod，适合当前手工功能验证和未来 Nightly 的最新 Connector 源码构建。
- `job`：每次 Spark 调用创建一次性 Kubernetes Job，并从 HTTPS bundle 下载 Connector，适合已经发布固定 Connector 产物的环境。

两种模式都在单 Pod 中使用 `spark-submit --master local[2]`，不需要 Spark Master/Worker。

## 执行边界

- 普通 pytest、PR CI 和常规 E2E 默认完全不收集本目录。
- 只有显式传入 `--run-spark-backfill` 才会收集用例。
- 禁止 pytest-xdist；运行时使用 `-n 0` 或不传 `-n`。
- 每个用例创建独立 Collection、Snapshot、对象存储 prefix 和 Result 路径。
- `job` 模式由 pytest 创建、等待、取日志和删除 Spark Job。
- `toolbox` 模式保留 Toolbox Pod，仅为每次调用保存独立 evidence；测试仍必须串行执行。
- V3 和 V2 必须在独立 Milvus 部署中运行，并用 marker 分开选择。

## 运行架构

```text
pytest/开发机/Jenkins Agent
  ├─ pymilvus: Collection、数据、Flush、Snapshot、Commit 后回读
  ├─ PyArrow: 生成确定性 Parquet
  ├─ MinIO SDK: 上传输入、读取 Result、检查 Artifact、清理 prefix
  └─ Kubernetes SDK
       ├─ toolbox mode: exec 已有 Toolbox Pod
       └─ job mode: 创建 one-shot Job Pod
            └─ spark-submit --master local[2]
                 ├─ BackfillApp
                 └─ PySpark Read Probe
```

## Toolbox 模式（当前推荐）

Toolbox Pod 必须已经完成 Connector 构建并处于 `Running 1/1`。Runner 默认按 label 自动发现：

```text
app=spark-milvus-toolbox
```

也可以通过 `--spark-toolbox-pod` 指定 Pod 名，但 Pod 重建后名字会变化，因此稳定运行更推荐 label。

本机运行 pytest 时，先把 ClusterIP Service port-forward 到本机：

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig -n default \
  port-forward svc/eric-spark-milvus 19530:19530 19091:9091

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig -n default \
  port-forward svc/eric-spark-minio 19000:9000
```

从现有 MinIO Secret 设置本地 pytest 所需凭证：

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

运行一个最小 V3 coalesce Backfill：

```bash
python3 -m pytest -p no:rerunfailures \
  'tests/python_client/spark_backfill/test_v3_backfill_e2e.py::test_v3_backfill_modes_publish_and_become_visible[coalesce]' \
  --run-spark-backfill \
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

Toolbox Runner 会：

1. 找到唯一 Ready Toolbox Pod；
2. 检查 wrapper、Connector JAR 和 native libraries；
3. 将 `contracts.py`、`read_probe.py` 注入 `/workspace/spark-backfill-pytest`；
4. 从 Pod 自己的 `MILVUS_TOKEN`、`S3_ACCESS_KEY`、`S3_SECRET_KEY` 环境变量取运行凭证；
5. 执行 Spark，并把脱敏命令、完整日志、退出码和结果写入 evidence；
6. 不删除或重启 Toolbox Pod。

Toolbox 模式最低 RBAC：

```text
get/list pods
get pods/exec
```

## Job 模式运行时

固定 Spark 镜像：

```text
apache/spark:4.0.1-scala2.13-java21-python3-ubuntu@sha256:fb5c5e61e7bb1be94b7f3a31afe1f73c5b4d20b6008f4ffa7278fc085da08a9e
```

Job 固定为 Linux AMD64、`restartPolicy: Never`、`backoffLimit: 0`、2 CPU / 8 GiB，默认超时 30 分钟。

远程入口固定添加 `--packages org.apache.hadoop:hadoop-aws:3.4.1`。Nightly namespace 需要能够访问 Maven/Ivy 仓库；如果集群禁止公网 egress，应在 Spark 镜像中预热 Ivy cache 或预置兼容的 Hadoop AWS/AWS SDK JAR，并在接入 Jenkins 前验证解析过程。

## Job 模式的 Connector bundle 契约

只有 `--spark-runner-mode job` 需要 Connector bundle。`--spark-connector-url` 必须是 Job Pod 可访问的 HTTPS `tar.gz`，并通过 `--spark-connector-sha256` 固定归档内容。归档至少包含：

```text
manifest.json
connector-assembly.jar
lib/libmilvus-storage.so
lib/libmilvus-storage-jni.so
```

`manifest.json` 必须声明 Connector revision、文件 SHA256、Spark/Scala/Java、OS/架构、Assembly JAR 和 Backfill 主类。远程入口会在执行 Spark 前校验归档 SHA256、安全解压、文件 SHA256 和运行时兼容性。

## 凭证

本地 pytest 的 MinIO SDK在两种模式下都从以下环境变量读取凭证：

```bash
export SPARK_BACKFILL_S3_ACCESS_KEY='...'
export SPARK_BACKFILL_S3_SECRET_KEY='...'
```

`job` 模式在未指定 `--spark-storage-secret-name` 时，用这些环境变量创建临时 K8s Secret；Milvus token 来自 `--token`。凭证通过 Secret env 注入，不写入 Job manifest、pytest 证据或远程命令日志。

`toolbox` 模式不创建 Secret。Spark 进程使用 Toolbox Pod 已有的 `MILVUS_TOKEN`、`S3_ACCESS_KEY` 和 `S3_SECRET_KEY`；本地环境变量只供 pytest 上传 Parquet、读取 Result 和清理对象使用。

如果传入 `--spark-storage-secret-name`，该 Secret 应使用同样的 key：`s3-access-key`、`s3-secret-key`、`milvus-token`。S3 两个 key 可以在 IAM 模式下同时省略；启用 Milvus 鉴权时不能省略 `milvus-token`。

本地 MinIO SDK 当前需要静态 AK/SK。如果 Spark 侧使用 IAM，仍需为 pytest 的上传、Result 读取和清理提供可用的静态测试凭证，或后续扩展本地对象存储客户端认证方式。

## Job 模式最小 Kubernetes RBAC

执行 pytest 的 kubeconfig/ServiceAccount 至少需要：

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

如果使用已有 `--spark-storage-secret-name`，可以移除 Secrets 创建和删除权限。pytest 启动时会用 SelfSubjectAccessReview fail-fast 检查权限。

## V3 Job 模式手工运行

```bash
python3 -m pytest -p no:rerunfailures tests/python_client/spark_backfill \
  --run-spark-backfill \
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

本机已安装的 `pytest-rerunfailures` 在部分沙箱环境会尝试打开本地 socket，因此本地开发示例使用 `-p no:rerunfailures`；Jenkins 环境不需要无条件照搬。

## V2 手工运行

V2 使用独立部署，至少关闭 Loon FFI 和 Storage V3 compaction upgrade：

```yaml
common:
  storage:
    useLoonFFI: false
dataCoord:
  compaction:
    storageVersion:
      enabled: false
```

运行：

```bash
python3 -m pytest -p no:rerunfailures tests/python_client/spark_backfill \
  --run-spark-backfill \
  <与上面相同的环境参数> \
  -m "spark_backfill_v2 and spark_backfill_core" \
  -n 0 -v --tb=short
```

fixture 会读取 `MilvusClient.list_persistent_segments()` 的真实 `storage_version`。V2 Snapshot 中只要出现 V1、V3、混合版本或 Segment 证据不完整，整组立即失败；不会根据 `storagev2_manifest_list` 的历史字段名猜版本。

V2 用例执行两轮相同 Field ID 的 Backfill：第二轮使用不同 Artifact 和值，Commit 后不主动 Reload，必须在线看到第二轮值。这个行为同时验证 Column Group 替换、加载中 Segment 的元数据传播和 DataVersion/Reopen 链路。当前公开持久 Segment API不返回 DataVersion 数字，因此测试保存提交前后 Segment 证据并做行为级验证；若后续 API 暴露 DataVersion，应再增加显式 `after > before` 断言。

指定 Snapshot 的 Connector Read 不能只传 Snapshot URL。它还需要 `milvus.snapshot.manifests`、`milvus.snapshot.v2.segments` 和 snapshot schema bytes。当前 bundle 契约没有包含 `ReadSourceOnlyApp`/`ListV2SegmentsApp` 的参数组装接口，因此 V2 初期使用 pymilvus 在线回读，不把 live client-mode 的已知 V2 planner 限制误判为 Backfill 写入失败。

## 证据与清理

每次 Spark 调用在 `--spark-evidence-root` 下保存独立目录，默认：

```text
${CI_LOG_PATH:-/tmp/ci_logs}/spark_backfill/<job-name>/
```

包含：

- Job 模式保存脱敏 Job manifest；Toolbox 模式保存脱敏 exec command；
- Pod 完整脱敏日志、退出码和失败原因；
- Snapshot 原始元数据；
- Backfill Result JSON；
- Result 同目录对象清单；
- Manifest/Column Group 文件路径和对象大小；
- Commit 响应和逐 Segment 状态；
- V2 提交前后持久 Segment 证据。

两种模式正常结束都会删除测试 Collection、Snapshot 和对象 prefix。Job 模式额外删除 ConfigMap、临时 Secret 和 Job；Toolbox 模式始终保留 Toolbox Pod。传 `--spark-keep-failed-job` 只影响 Job 模式。

## Nightly 接入

Nightly 不应依赖 spark-milvus Release 每日发布。推荐分成两个阶段：

```text
Builder/Toolbox Pod
  → checkout 指定 spark-milvus commit
  → 现场编译 Connector/JNI/native libraries
  → readiness 验证产物

pytest
  → --spark-runner-mode toolbox
  → 按 label 连接 Builder/Toolbox Pod
  → 执行 Read/Backfill cases
```

Toolbox Runner 不负责 Pod 生命周期，因此 Jenkins/独立 fixture 可以自由选择 Deployment、Job + PVC 或预构建镜像。当前建议：

1. Jenkins 提供 kubeconfig/ServiceAccount、Milvus/MinIO/Management 地址和凭证。
2. Builder 阶段 checkout 明确的 Connector commit 并启动 Toolbox，等待 `Running 1/1`。
3. pytest 使用 Toolbox label，不需要 Connector URL/SHA。
4. V3 每晚执行完整 core + negative。
5. V2 环境准备完成后，顺序执行独立 V3 和 V2 部署，禁止在同一部署中混跑。
6. Jenkins 归档 JUnit、整个 evidence root、Builder 日志和 pytest 控制台日志。

普通收集门禁可用下面的命令检查：

```bash
python3 -m pytest -p no:rerunfailures tests/python_client/spark_backfill --collect-only -q
```

显式选择该目录时因为 0 tests 可能返回 pytest exit code 5，这是 pytest 对“所选目录为空”的标准行为；在仓库常规测试收集中，本目录被忽略且其他测试仍正常收集。
