# Spark-Milvus Toolbox vcluster 部署报告

## 1. 报告信息

- 目标：在 vcluster 中部署一个可以连接现有 Milvus/MinIO 的 Spark 4.0.1 工具环境，并构建固定版本的 `zilliztech/spark-milvus` Connector。
- 状态截止：2026-07-23 23:00 CST。
- 报告实际落盘：2026-07-24 09:55 CST。23:00 后 vcluster 进入计划 sleep，后续集群状态未纳入本报告的完成判断。
- 23:00 截止结论：**当时未完成可用性验收**。Kubernetes 资源已部署，Connector 构建推进到 Arrow source 处理，但 Pod 尚未 Ready，Spark-Milvus Read/Backfill smoke test 尚未执行。
- 恢复后最终结论：**截至 2026-07-24 12:40 CST，手工基本功能闭环已通过**。后续恢复结果见第 12 节；该节取代 23:00 截止结论作为当前状态。

## 2. 环境与固定版本

| 项目 | 值 |
| --- | --- |
| kubeconfig | `/Users/zilliz/Desktop/kubecon/kubeconfig` |
| context | `my-vcluster` |
| namespace | `default` |
| Milvus Helm release | `eric-spark` |
| Milvus version | `2.6.18` |
| Milvus URI | `http://eric-spark-milvus:19530` |
| Management endpoint | `http://eric-spark-milvus:9091` |
| MinIO endpoint | `eric-spark-minio:9000` |
| MinIO bucket | `milvus-bucket` |
| MinIO root path | `file` |
| Spark image | `apache/spark:4.0.1-scala2.13-java21-python3-ubuntu@sha256:fb5c5e61e7bb1be94b7f3a31afe1f73c5b4d20b6008f4ffa7278fc085da08a9e` |
| Spark mode | `spark-submit --master local[2]` |
| Scala | `2.13.16` |
| Java | `21` |
| Connector commit | `dfcec3d564e78be771b6d41fc04632db77d8d507` |
| milvus-storage submodule | `84b433d498f0cc1449ad969e05f7ff0c7bf5950a` |
| Architecture | Linux AMD64 |

## 3. 已部署的 Kubernetes 资源

已创建：

```text
Secret:     spark-milvus-toolbox-credentials
ConfigMap:  spark-milvus-toolbox-scripts
Deployment: spark-milvus-toolbox
Pod:        spark-milvus-toolbox-795f57f78c-6kd8t
```

部署形态：

```text
Deployment / Pod
  ├─ init container: build-connector
  │    ├─ 安装 Conan / SDKMAN / Scala / SBT
  │    ├─ clone 固定 Connector commit
  │    ├─ 构建 milvus-storage JNI native libraries
  │    └─ sbt assembly
  └─ main container: spark-toolbox
       ├─ 长期运行
       └─ /usr/local/bin/spark-submit-milvus --master local[2]
```

这不是 Spark Master/Worker 集群，而是适合当前手工功能验证的一体化 Spark 工具 Pod。

## 4. 截止时的部署状态

最后确认的状态：

```text
Pod READY: 0/1
Init container restart count: 10
State: CrashLoopBackOff
Backoff: 5 minutes
```

最后一个已确认错误：

```text
ERROR: Revisions not enabled in the client, specify a reference without revision
```

错误发生在脚本尝试恢复固定 Arrow Conan recipe revision 时：

```text
arrow/17.0.0@milvus/dev-2.6#7af258a853e20887f9969f713110aac8
```

本地脚本随后增加了：

```bash
export CONAN_REVISIONS_ENABLED=1
```

但截止状态下，没有完成新一轮 Pod 日志验证，因此该修复只能标记为“已实现，未在集群确认”。

## 5. 已验证成功的工作

### 5.1 基础网络与依赖仓库

vcluster 已证明可访问：

- GitHub source/tag/release assets；
- Milvus JFrog Conan remote；
- Maven/Ivy；
- Ubuntu apt repositories；
- SDKMAN；
- pip package index。

`archive.apache.org` 在 vcluster 中不可达，这是本次多数 source download 问题的共同根因。

### 5.2 bzip2 source 修复

原 recipe URL：

```text
https://sourceware.org/pub/bzip2/bzip2-1.0.8.tar.gz
```

在 vcluster 返回 403。改为：

```text
https://fossies.org/linux/misc/bzip2-1.0.8.tar.gz
```

已真实构建通过，日志中确认 `BZIP2_BUILD_OK`。

### 5.3 Boost source 修复

JFrog 旧地址返回非归档内容，SourceForge fallback 不稳定。改为：

```text
https://archives.boost.io/release/1.83.0/source/boost_1_83_0.tar.bz2
```

Boost 已完成构建并进入 Conan cache。

### 5.4 Thrift source 修复

原地址：

```text
http://archive.apache.org/dist/thrift/0.17.0/thrift-0.17.0.tar.gz
```

vcluster 连接超时。本机下载了 Apache 原始 release tarball并验证：

```text
SHA256: b272c1788bb165d99521a2599b31b97fa69e5931d099015d91ae107a0b0cc58f
```

文件被放入 Pod 持久 `emptyDir` Conan cache：

```text
/root/.conan/source-cache/thrift-0.17.0.tar.gz
```

已验证结果：

```text
/root/.conan/source-cache/thrift-0.17.0.tar.gz: OK
thrift/0.17.0: CMake command: ...
thrift/0.17.0: Package ... built
thrift/0.17.0: Package ... created
```

因此 Thrift workaround 是已验证的。

### 5.5 Arrow 官方 release asset 校验

原 Conan recipe 的 Apache URL最终重定向到不可达的 `archive.apache.org`。

GitHub Releases 上存在官方 source asset：

```text
https://github.com/apache/arrow/releases/download/apache-arrow-17.0.0/apache-arrow-17.0.0.tar.gz
```

下载后 SHA256：

```text
9d280d8042e7cf526f8c28d170d93bfab65e50f94569f6a790982a878d8d898d
```

它与 pinned Conan recipe 的 checksum 完全一致，说明这是与 Apache archive 字节一致的官方 release 文件。

Conan user download cache key已按 Conan 1.61 实现验证：

```text
SHA256(original_url + expected_checksum)
= 80718851411e770d39c1871c3f87561896a45a3646a334b9bf43ce3355f568da
```

Pod 中对应文件也已验证：

```text
/root/.conan/download-cache/80718851411e770d39c1871c3f87561896a45a3646a334b9bf43ce3355f568da
SHA256: 9d280d8042e7cf526f8c28d170d93bfab65e50f94569f6a790982a878d8d898d
```

### 5.6 已完成并缓存的主要 native dependencies

日志确认已经完成的组件包括：

- protobuf 5.27.0；
- gRPC 1.67.1；
- Folly 2024.08.12.00；
- Thrift 0.17.0；
- Google Cloud C++ 2.28.0；
- AWS CRT C++ 0.35.2；
- AWS SDK C++ 1.11.692；
- libavrocpp、OpenSSL、Boost、glog、libevent 等。

这些产物保存在 Pod 的 Conan `emptyDir` cache 中。只要原 Pod 不被删除，init container 重启可以复用；删除或重建 Pod 会丢失这些缓存。

## 6. 根因与调试结论

### 6.1 根因一：Apache archive egress 不可达

以下地址在 vcluster 中连接超时：

```text
archive.apache.org:80
archive.apache.org:443
```

影响至少包括：

- Thrift 0.17.0；
- Arrow 17.0.0。

不能仅把 `http` 改成 `https`，两者都不可达。

### 6.2 根因二：`conan install --update` 会覆盖本地 recipe workaround

`milvus-storage/cpp/Makefile` 原始内容：

```make
CONAN_BASE = conan install .. --build=arrow --build=missing ... --update
```

即使提前修改 Conan cache 中的 URL，`--update` 仍可能从 remote 恢复旧 recipe。脚本已增加：

```bash
sed -i 's/ --update$//' milvus-storage/cpp/Makefile
```

### 6.3 根因三：Arrow requirement 固定了 recipe revision

`milvus-storage/cpp/conanfile.py`：

```python
self.requires("arrow/17.0.0@milvus/dev-2.6#7af258a853e20887f9969f713110aac8")
```

因此直接修改 Arrow recipe 会改变本地 revision，导致：

```text
local cache revision does not match requested revision
```

正确方向是：

1. 保留原 Arrow recipe 和 revision；
2. 使用原始 checksum 对应的 Conan download cache；
3. 显式启用 `CONAN_REVISIONS_ENABLED=1`；
4. 不用 `--update` 覆盖 vcluster-specific 的其他 recipe workaround。

### 6.4 Deployment progress deadline

首次源码构建超过了原来的 3600 秒。文件已调整为：

```yaml
progressDeadlineSeconds: 10800
```

`deploy.sh` rollout timeout 已调整为：

```text
180m
```

注意：修改 deadline 不会自动清除已有 Deployment 的 `ProgressDeadlineExceeded` condition；现有 Pod 应直接用 Pod Ready 状态观察。

## 7. 本地文件改动

目录：

```text
tests/python_client/spark_backfill/deploy/manual_toolbox/
```

文件：

```text
build-connector.sh
spark-submit-milvus.sh
deployment.yaml
deploy.sh
```

主要改动：

- 固定 Spark image digest 和 Connector commit；
- 构建/缓存 native Connector；
- bzip2、Boost、Thrift source workaround；
- Arrow 官方 release asset Conan download cache；
- Conan revisions 显式开启；
- 移除 submodule Makefile 中的 `--update`；
- 构建 deadline 从 1 小时增加到 3 小时；
- main container 提供 `/usr/local/bin/spark-submit-milvus`。

截至本报告，这些 `deploy/manual_toolbox` 文件仍为未提交文件。

## 8. 尚未完成的验收

以下项目均未通过，因此不能声称部署完成：

1. Pod `Ready=True`；
2. Spark 4.0.1 / Scala 2.13.16 / Java 21 runtime 检查；
3. Connector assembly JAR存在；
4. `libmilvus-storage.so` 和 `libmilvus-storage-jni.so` 存在；
5. `ldd` 无 `not found`；
6. Python包 `pymilvus`、`pyarrow`、`minio`、`numpy` 可导入；
7. Pod 到 Milvus 19530 和 MinIO 9000 的连通性；
8. 最小 Spark Pi/job；
9. 创建测试 Collection、insert/flush；
10. Spark Connector live client-mode Read；
11. Snapshot + Backfill + Commit；
12. pytest 对该手工 Toolbox 的接入。

## 9. 下一次恢复操作

### 9.1 先验证 ConfigMap 是否包含最终修复

```bash
kubectl \
  --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default \
  get configmap spark-milvus-toolbox-scripts \
  -o jsonpath='{.data.build-connector\.sh}' \
  | grep 'CONAN_REVISIONS_ENABLED=1'
```

若没有输出，重新应用 ConfigMap：

```bash
kubectl \
  --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default \
  create configmap spark-milvus-toolbox-scripts \
  --from-file=build-connector.sh=tests/python_client/spark_backfill/deploy/manual_toolbox/build-connector.sh \
  --from-file=spark-submit-milvus.sh=tests/python_client/spark_backfill/deploy/manual_toolbox/spark-submit-milvus.sh \
  --dry-run=client -o yaml \
  | kubectl \
      --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
      -n default apply -f -
```

### 9.2 检查原 Pod 是否还在

```bash
kubectl \
  --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default \
  get pod -l app=spark-milvus-toolbox -o wide
```

如果原 Pod 仍是：

```text
spark-milvus-toolbox-795f57f78c-6kd8t
```

优先保留它，因为其中包含已经构建的大量 Conan cache。

### 9.3 读取当前/上一轮日志

```bash
kubectl \
  --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default \
  logs spark-milvus-toolbox-795f57f78c-6kd8t \
  -c build-connector --tail=200

kubectl \
  --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default \
  logs spark-milvus-toolbox-795f57f78c-6kd8t \
  -c build-connector --previous --tail=300
```

预期下一步应看到：

```text
Arrow download cache file: OK
Downloading arrow recipe revision ...#7af258...
arrow/17.0.0 ... CMake command
```

不能再看到：

```text
Revisions not enabled in the client
archive.apache.org timed out
local cache revision does not match requested revision
```

## 10. 架构与后续建议

### 短期手工验证

继续使用当前长期 Toolbox Pod + `local[2]` 是合理的，避免额外部署 Spark Master/Worker。

### 中期可重复部署

不建议每次 Pod 启动都从源码构建完整 native dependency graph。首次构建已证明耗时长且受外部 source URL影响。更稳定的方式是：

1. 在可控 builder 环境构建 Connector JAR/native libs；
2. 制作固定 digest 的内部 Spark-Connector image；
3. 或把 artifacts 放入 PVC/对象存储；
4. Toolbox Pod 启动时只校验 revision/SHA，不重新编译。

### Nightly/pytest

在手工 Read + Backfill 闭环通过之前，不应开始 Jenkins/Nightly 接入。当前优先级仍应是：

```text
Pod Ready
  -> Spark minimal job
  -> Connector Read
  -> Snapshot
  -> Backfill
  -> Commit
  -> pytest 封装
```

## 11. 安全说明

- Milvus token 和 MinIO AK/SK通过 Kubernetes Secret/env 注入；
- 本报告未记录实际 MinIO凭证；
- Job/命令日志中未主动打印 Secret；
- 操作范围仅限测试 vcluster 的 `default` namespace 和本地 `spark_backfill` 工作区。

## 12. vcluster 恢复后结果

### 12.1 当前结论

恢复 vcluster 后，原 Pod 的构建 cache 和 artifacts 得以保留，最终完成以下闭环：

```text
Connector build
  -> Spark minimal job
  -> Connector Read
  -> Snapshot + Parquet Backfill
  -> Result JSON
  -> Milvus Commit
  -> 已加载 Collection 自动在线可见
```

当前 Pod：

```text
spark-milvus-toolbox-795f57f78c-qwfgw   Running   Ready=true   Restarts=0
```

### 12.2 Spark/Scala 版本结论

用户提示“SDKMAN 提供的 Spark 可能只有 Scala 2.12，需要手工安装 Scala 2.13 版本”是一个正确的通用兼容性提醒，但不适用于本次容器部署中的 Spark 安装路径：

- Spark 不通过 SDKMAN 安装；
- Spark 来自固定 digest 的 Apache 官方镜像；
- Pod 内实测为 Spark `4.0.1`、Scala `2.13.16`、Java `21.0.8`；
- SDKMAN 只用于安装构建 Connector 所需的 Scala `2.13.16` 和 SBT `1.11.1`。

因此本次不需要再下载 `spark-4.0.1-bin-hadoop3-scala2.13.tgz`。如果未来改成本机或自定义基础镜像安装 Spark，则必须选择该 Scala 2.13 distribution，不能使用 Scala 2.12 build。

### 12.3 Connector 构建结果

固定版本：

```text
Connector commit: dfcec3d564e78be771b6d41fc04632db77d8d507
Rust:             1.96.0
```

产物：

```text
spark-connector-assembly.jar
libmilvus-storage.so
libmilvus-storage-jni.so
完整 native shared-library closure（66 files）
```

SHA256：

```text
9218e0fe462e7f0b24d4579878d1b3171d60c64975bbc672777704a670c5461b  spark-connector-assembly.jar
1f69303f1dce46897cc5138781ade1663c03dee7019bcbdeec184766be237404  libmilvus-storage.so
8ac8dbab9d36635d546b138a4e75d0ce440109d52a1c9b847e1a4620db4017cf  libmilvus-storage-jni.so
```

`ldd` 未发现 missing library，Python 依赖导入、Milvus/MinIO 网络连接和 Spark Pi 均通过。

### 12.4 Connector Read smoke

Read smoke 返回码为 0，并验证：

- 行数为 10；
- PK 为 `0..9`；
- 字段裁剪只返回 `id`、`base_float`；
- Spark SQL 返回 `count=10`、`avg(base_float)=4.5`；
- TopK 返回 3 行。

将 executor classloader 改为 parent-first 后重新运行，Read 仍返回 0，证明 Backfill 修复没有破坏基础 Read。

待跟踪问题：TopK 结果中的 `id` 是大整数而不是原始 PK `0/1/2`，但普通扫描的 PK 正确。当前 pytest 只断言 TopK 数量，没有验证 TopK PK 内容；应单独调查 Connector vector search 的 ID 映射，并在确认语义后加强断言。

### 12.5 Backfill 首次失败与根因

Backfill 已经能够读取 Snapshot、Parquet 并开始处理 Segment，但最初在 `MilvusBackfill.scala:1057` 失败：

```text
BackfillConfig cannot be cast to BackfillConfig
```

错误中的两个同名 class 分别来自不同的 Spark `ChildFirstURLClassLoader`。移除重复 `--jars` 后问题仍存在，最终通过单变量实验确认根因是：

```text
spark.executor.userClassPathFirst=true
```

Backfill driver 创建的 `BackfillConfig` 被发送到 local executor 后，executor child-first loader 又加载了一份 Connector class，导致 JVM 认为它们是两个不同类型。

验证过的配置为：

```text
spark.driver.userClassPathFirst=true
spark.executor.userClassPathFirst=false
```

这使 executor 使用 parent-first loader，避免同一个 Connector class 被重复定义。该配置已固化到：

```text
tests/python_client/spark_backfill/deploy/manual_toolbox/spark-submit-milvus.sh
tests/python_client/spark_backfill/remote_entrypoint.py
```

ConfigMap `spark-milvus-toolbox-scripts` 也已更新。现有 Pod 的 ConfigMap subPath mount 不会热更新，因此当前 Pod 手工验证使用 `/workspace/spark-submit-milvus`；新 Pod 会直接获得修复后的 `/usr/local/bin/spark-submit-milvus`。

### 12.6 Backfill、Commit 和在线可见性证据

测试资源：

```text
Collection: spark_backfill_smoke_6b4e0b40
Snapshot:   spark_backfill_snapshot_6b4e0b40
Segment:    467889851153279299
Storage:    V3
Mode:       coalesce
```

Backfill 返回码为 0，Summary：

```text
Status: SUCCESS
Segments Processed: 1
Total Source Rows: 10
Total Backfill Data File Rows: 10
Total Matched Rows: 10
Total Rows Written: 10
New Fields: bf_score, bf_label
StorageV2 Segments: 0 / 1
```

Result JSON 中 `success=true`、`schemaVersion=0`、Segment version 从 Snapshot 版本前进到 `2`。

Commit：

```json
{
  "committed_segments": 1,
  "failed_segments": 0,
  "msg": "OK",
  "segment_statuses": [
    {"segment_id": 467889851153279299, "ok": true, "kind": "v3"}
  ],
  "total_segments": 1
}
```

Commit 前，已加载 Collection 中 `bf_score`、`bf_label` 均为 null。Commit 后没有主动 reload，第一次轮询即看到 10 行全部更新：

```text
id=0 -> bf_score=0.5, bf_label=backfill-0
...
id=9 -> bf_score=9.5, bf_label=backfill-9
```

因此已经验证 V3 的最小功能链路和自动元数据传播，而不只是 Spark 作业返回成功。

完成证据记录后已清理 smoke 资源：Snapshot 和 Collection 均已删除，对象存储测试 prefix 下剩余对象数为 0。Spark Toolbox Deployment 保留，便于继续手工测试。

### 12.7 测试代码修复与验证

同时修复了手工验证暴露出的测试兼容性问题：

- Snapshot 返回相对对象 key 时补全为 `s3a://<bucket>/...`；
- MinIO/S3A 显式使用 path-style addressing；
- PySpark 4 同时兼容 `schema.fieldNames` 属性和 callable 形式；
- JVM `--class` 应用不再重复把 Connector JAR 传入 `--jars`；
- executor 使用 parent-first classloader。

新增单元断言确保 pytest 远程入口和手工 wrapper 都保持：

```text
spark.driver.userClassPathFirst=true
spark.executor.userClassPathFirst=false
```

### 12.8 当前边界

本次完成的是“手工部署额外 Spark Toolbox，连接已有 Milvus/MinIO，验证基础 Read 与单次 V3 Backfill 闭环”。尚未声称完成：

- 全部 V3 core/negative pytest；
- V2 独立环境；
- Connector bundle 发布和一次性 K8s Job 端到端；
- Jenkins/Nightly 接入；
- TopK ID 映射问题。

这些工作应在当前手工功能基线稳定后分阶段推进。
