# Spark-Milvus Backfill Nightly pytest

本目录实现 Spark-Milvus Read/Backfill 的 Nightly-only pytest。pytest 在本地或 Jenkins Agent 运行，通过 Kubernetes Python SDK 创建一次性 Job；Job Pod 内使用 `spark-submit --master local[2]`，不需要部署 Spark Master/Worker。

## 执行边界

- 普通 pytest、PR CI 和常规 E2E 默认完全不收集本目录。
- 只有显式传入 `--run-spark-backfill` 才会收集用例。
- 禁止 pytest-xdist；运行时使用 `-n 0` 或不传 `-n`。
- 每个用例创建独立 Collection、Snapshot、对象存储 prefix、Result 路径和 K8s Job。
- pytest 负责创建、等待、取日志和删除 Spark Job；Jenkins 不重复实现 Spark 编排。
- V3 和 V2 必须在独立 Milvus 部署中运行，并用 marker 分开选择。

## 运行架构

```text
pytest/Jenkins Agent
  ├─ pymilvus: Collection、数据、Flush、Snapshot、Commit 后回读
  ├─ PyArrow: 生成确定性 Parquet
  ├─ MinIO SDK: 上传输入、读取 Result、检查 Artifact、清理 prefix
  └─ Kubernetes SDK
       └─ one-shot Job Pod
            └─ spark-submit --master local[2]
                 ├─ BackfillApp
                 └─ PySpark Read Probe
```

固定 Spark 镜像：

```text
apache/spark:4.0.1-scala2.13-java21-python3-ubuntu@sha256:fb5c5e61e7bb1be94b7f3a31afe1f73c5b4d20b6008f4ffa7278fc085da08a9e
```

Job 固定为 Linux AMD64、`restartPolicy: Never`、`backoffLimit: 0`、2 CPU / 8 GiB，默认超时 30 分钟。

远程入口固定添加 `--packages org.apache.hadoop:hadoop-aws:3.4.1`。Nightly namespace 需要能够访问 Maven/Ivy 仓库；如果集群禁止公网 egress，应在 Spark 镜像中预热 Ivy cache 或预置兼容的 Hadoop AWS/AWS SDK JAR，并在接入 Jenkins 前验证解析过程。

## Connector bundle 契约

`--spark-connector-url` 必须是公开可访问的 HTTPS `tar.gz`，并通过 `--spark-connector-sha256` 固定归档内容。归档至少包含：

```text
manifest.json
connector-assembly.jar
lib/libmilvus-storage.so
lib/libmilvus-storage-jni.so
```

`manifest.json` 必须声明 Connector revision、文件 SHA256、Spark/Scala/Java、OS/架构、Assembly JAR 和 Backfill 主类。远程入口会在执行 Spark 前校验归档 SHA256、安全解压、文件 SHA256 和运行时兼容性。

## 凭证

若未指定 `--spark-storage-secret-name`，session fixture 从以下环境变量创建临时 K8s Secret：

```bash
export SPARK_BACKFILL_S3_ACCESS_KEY='...'
export SPARK_BACKFILL_S3_SECRET_KEY='...'
```

创建临时 Secret 时，Milvus token 来自现有 `--token`。凭证通过 Secret env 注入，不写入 Job manifest、pytest 证据或远程命令日志；远程入口还会对 Spark 输出中的实际凭证值做替换脱敏。

如果传入 `--spark-storage-secret-name`，该 Secret 应使用同样的 key：`s3-access-key`、`s3-secret-key`、`milvus-token`。S3 两个 key 可以在 IAM 模式下同时省略；启用 Milvus 鉴权时不能省略 `milvus-token`。

本地 MinIO SDK 当前需要静态 AK/SK。如果 Spark 侧使用 IAM，仍需为 pytest 的上传、Result 读取和清理提供可用的静态测试凭证，或后续扩展本地对象存储客户端认证方式。

## 最小 Kubernetes RBAC

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

## V3 手工运行

```bash
python3 -m pytest -p no:rerunfailures tests/python_client/spark_backfill \
  --run-spark-backfill \
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

每个 Job 在 `--spark-evidence-root` 下保存独立目录，默认：

```text
${CI_LOG_PATH:-/tmp/ci_logs}/spark_backfill/<job-name>/
```

包含：

- 脱敏 Job manifest；
- Pod 完整脱敏日志、退出码和失败原因；
- Snapshot 原始元数据；
- Backfill Result JSON；
- Result 同目录对象清单；
- Manifest/Column Group 文件路径和对象大小；
- Commit 响应和逐 Segment 状态；
- V2 提交前后持久 Segment 证据。

正常结束会删除 Collection、Snapshot、测试对象 prefix、ConfigMap、临时 Secret 和 Job。传 `--spark-keep-failed-job` 时保留失败 Job 供现场调试，但证据仍会先落盘。

## Nightly 接入

首期不修改 Jenkinsfile。后续创建独立 Spark Backfill Nightly Job：

1. Jenkins 提供 kubeconfig/ServiceAccount、Milvus/MinIO/Management 地址、Connector URL/SHA 和凭证。
2. V3 每晚执行完整 core + negative。
3. V2 环境准备完成后，顺序执行独立 V3 和 V2 部署，禁止在同一部署中混跑。
4. 不加入 PR CI、普通 E2E 或 Nightly2 现有矩阵的每一个单元。
5. Jenkins 归档 JUnit、整个 evidence root 和 pytest 控制台日志。

普通收集门禁可用下面的命令检查：

```bash
python3 -m pytest -p no:rerunfailures tests/python_client/spark_backfill --collect-only -q
```

显式选择该目录时因为 0 tests 可能返回 pytest exit code 5，这是 pytest 对“所选目录为空”的标准行为；在仓库常规测试收集中，本目录被忽略且其他测试仍正常收集。
