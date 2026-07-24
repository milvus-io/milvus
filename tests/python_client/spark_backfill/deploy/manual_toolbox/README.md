# Spark-Milvus Toolbox 可重复部署 Runbook

本文记录已经在 vcluster 中真实验证成功的 Spark-Milvus Toolbox 部署流程。目标是在已有 Milvus 和 MinIO 旁边部署一台临时 Spark 工作机，用它手工验证 Spark Connector Read 和 Storage V3 Backfill。

本文分成两个层次：

1. **当前已验证方案**：Pod 启动时在 init container 中从源码构建 Connector，适合功能开发和临时测试。
2. **长期稳定方案**：预先构建包含 Connector 的镜像，运行时不再现场编译。需要频繁重建、Nightly 或 CI 时应采用这个方案。

## 1. 当前方案解决什么问题

已有环境：

```text
Milvus:            eric-spark-milvus:19530
Management API:    eric-spark-milvus:9091
MinIO:             eric-spark-minio:9000
Namespace:         default
```

新增一个 Kubernetes Deployment：

```text
spark-milvus-toolbox
```

Pod 内包含：

```text
init container: build-connector
  └─ 下载并编译 spark-milvus、milvus-storage、JNI 和 native dependencies

main container: spark-toolbox
  └─ 保存 Spark、Connector 和测试依赖，等待手工执行 spark-submit
```

Spark 不使用 Master/Worker 集群。任务通过下面的模式在单个 Pod 内运行：

```text
spark-submit --master local[2]
```

## 2. 已验证版本

不要随意升级其中任意一项。升级后必须重新执行本文的完整验收。

| 项目 | 固定版本 |
| --- | --- |
| Spark image | `apache/spark:4.0.1-scala2.13-java21-python3-ubuntu@sha256:fb5c5e61e7bb1be94b7f3a31afe1f73c5b4d20b6008f4ffa7278fc085da08a9e` |
| Spark | `4.0.1` |
| Scala | `2.13.16` |
| Java | `21.0.8` |
| Connector commit | `dfcec3d564e78be771b6d41fc04632db77d8d507` |
| Rust | `1.96.0` |
| Hadoop AWS package | `org.apache.hadoop:hadoop-aws:3.4.1` |
| OS/architecture | Linux AMD64 |

Spark 来自 Apache 官方 Scala 2.13 镜像，不通过 SDKMAN 安装。SDKMAN 只安装构建 Connector 所需的 Scala `2.13.16` 和 SBT `1.11.1`。

已验证产物 SHA256：

```text
9218e0fe462e7f0b24d4579878d1b3171d60c64975bbc672777704a670c5461b  spark-connector-assembly.jar
1f69303f1dce46897cc5138781ade1663c03dee7019bcbdeec184766be237404  libmilvus-storage.so
8ac8dbab9d36635d546b138a4e75d0ce440109d52a1c9b847e1a4620db4017cf  libmilvus-storage-jni.so
```

## 3. 文件职责

| 文件 | 职责 |
| --- | --- |
| `deployment.yaml` | 定义 Toolbox Deployment、init container、主容器、资源和 Volume |
| `deploy.sh` | 创建 Secret、ConfigMap，应用 Deployment 并等待 Ready |
| `build-connector.sh` | 安装工具、下载源码、构建 Connector/JNI/native runtime |
| `spark-submit-milvus.sh` | 使用正确的 JAR、native library、S3A 和 classloader 参数启动 Spark |

所有命令都从 Milvus 仓库根目录执行。

## 4. 部署前必须确认的输入

### 4.1 kubeconfig 和 namespace

当前已验证：

```text
Kubeconfig: /Users/zilliz/Desktop/kubecon/kubeconfig
Namespace:  default
```

检查连接：

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig get nodes

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default get pods
```

所有节点必须是 `Ready`。

### 4.2 Milvus Service

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default get svc eric-spark-milvus
```

必须存在：

```text
19530/TCP  Milvus Client API
9091/TCP   Management API
```

如果 Milvus release 不是 `eric-spark`，必须修改 `deployment.yaml` 中的：

```yaml
MILVUS_URI: http://<release>-milvus:19530
```

### 4.3 MinIO Service 和 Secret

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default get svc eric-spark-minio

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default get secret eric-spark-minio
```

当前配置要求 Secret 中存在：

```text
accesskey
secretkey
```

当前对象存储配置：

```text
Endpoint:  eric-spark-minio:9000
Bucket:    milvus-bucket
Root path: file
SSL:       false
```

如果 Service、Secret、bucket 或 root path 不同，必须先修改 `deployment.yaml`，不能直接照搬。

### 4.4 节点资源

构建 init container：

```text
request: 4 CPU / 12 GiB
limit:   6 CPU / 16 GiB
```

Spark 主容器：

```text
request: 2 CPU / 8 GiB
limit:   2 CPU / 8 GiB
```

Namespace/集群至少要有可以容纳 init container request 的 AMD64 节点。

### 4.5 外网访问

源码构建期间需要访问：

- Ubuntu apt repositories；
- GitHub；
- Milvus JFrog Conan remote；
- SDKMAN；
- Maven/Ivy；
- pip package index；
- Rust toolchain 和 crate registry。

当前脚本已经包含 bzip2、Boost、Thrift 和 Arrow source 下载兼容处理。不要轻易移除这些 workaround。

## 5. 执行部署

在仓库根目录运行：

```bash
bash tests/python_client/spark_backfill/deploy/manual_toolbox/deploy.sh \
  /Users/zilliz/Desktop/kubecon/kubeconfig \
  default
```

脚本会创建或更新：

```text
Secret:     spark-milvus-toolbox-credentials
ConfigMap:  spark-milvus-toolbox-scripts
Deployment: spark-milvus-toolbox
```

然后等待 Deployment Ready，最长等待 180 分钟。

## 6. 如何观察部署进度

先查 Pod 名称：

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default get pod -l app=spark-milvus-toolbox -o wide
```

如果显示：

```text
Init:0/1
```

说明 Pod 已经启动，但 Connector 仍在构建。查看实时日志：

```bash
POD=$(kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default get pod -l app=spark-milvus-toolbox \
  -o jsonpath='{.items[0].metadata.name}')

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default logs -f "$POD" -c build-connector
```

一次全新无缓存构建实测约 56 分钟：

```text
started:  2026-07-24 13:44:25 CST
finished: 2026-07-24 14:40:33 CST
exit:     0
```

构建期间看到 `gcc`、`g++`、`rustc`、`cmake` 或 `sbt` 占用 CPU，通常表示仍在正常工作。

最终成功状态必须是：

```text
READY   STATUS    RESTARTS
1/1     Running   0
```

## 7. 部署成功后的强制验收

不能只看 Pod 是 Running。下面的检查全部通过才能认为 Toolbox 可用。

### 7.1 Spark/Scala/Java 版本

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default exec "$POD" -- /opt/spark/bin/spark-submit --version
```

必须看到：

```text
Spark 4.0.1
Scala 2.13.16
Java 21
```

### 7.2 Connector 产物

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

输出应与第 2 节记录的 SHA256 一致。

### 7.4 Native library closure

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default exec "$POD" -- bash -lc '
    ldd /opt/spark-milvus/native/libmilvus-storage.so
    ldd /opt/spark-milvus/native/libmilvus-storage-jni.so
  '
```

输出中不能出现：

```text
not found
```

### 7.5 Milvus 和 MinIO 网络

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

### 7.6 最小 Spark Job

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default exec "$POD" -- \
  /usr/local/bin/spark-submit-milvus \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples_2.13-4.0.1.jar \
  10
```

命令退出码必须为 0，并输出 Pi 计算结果。

## 8. 已验证的 Spark 启动配置

所有 Read 和 Backfill 应通过下面的 wrapper 启动：

```text
/usr/local/bin/spark-submit-milvus
```

不要绕过 wrapper 手工拼接不同参数。已验证的关键配置是：

```text
--master local[2]
--packages org.apache.hadoop:hadoop-aws:3.4.1
--exclude-packages software.amazon.awssdk:bundle

spark.driver.userClassPathFirst=true
spark.executor.userClassPathFirst=false
```

`spark.executor.userClassPathFirst=false` 不能改回 `true`。Backfill 曾经因为 Driver 和 Executor 分别加载两份 `BackfillConfig` 而失败：

```text
BackfillConfig cannot be cast to BackfillConfig
```

JVM `--class` 应用把 Connector JAR作为主应用 JAR，不要再同时通过 `--jars` 添加同一 JAR。

MinIO/S3A 必须使用：

```text
fs.use_ssl=false
fs.use_virtual_host=false
fs.region=us-east-1
```

Snapshot API 如果返回相对对象 key，执行 Backfill 前必须补成：

```text
s3a://<bucket>/<snapshot-key>
```

## 9. 为什么当前方案还不够稳定

当前下面这些目录都是 `emptyDir`：

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

`emptyDir` 与 Pod 同生命周期。发生以下任一情况都会丢失所有构建结果：

- 手工删除 Pod；
- Deployment rollout；
- 节点 scale down；
- vcluster sleep/wake 后 Pod 被重建；
- 修改 Pod template；
- 节点故障或驱逐。

Pod 重建后会重新执行大约一小时的完整源码构建。这不是 Kubernetes 卡住，而是当前架构的预期行为。

构建期间不要删除 Pod，也不要修改 Deployment。如果节点 autoscaler 可能缩容，应至少给 Pod template 增加：

```yaml
metadata:
  annotations:
    cluster-autoscaler.kubernetes.io/safe-to-evict: "false"
```

这个 annotation 只能降低构建期间被 autoscaler 主动驱逐的概率，不能防止 vcluster 整体 sleep 或节点故障。

## 10. 真正稳定的推荐方案

### 方案 A：继续使用当前源码构建 Pod

适用：一次性开发验证。

优点：

- 不需要镜像仓库；
- 修改 Connector 后可以直接重建。

缺点：

- 每个新 Pod 约一小时；
- 高度依赖公网和第三方下载地址；
- Pod 重建丢失所有 cache。

### 方案 B：用 PVC 保存 cache 和 artifacts

适用：短期反复开发。

至少持久化：

```text
/root/.conan
/root/.cache
/root/.sbt
/root/.ivy2
/artifacts
```

优点：Pod 重建可以复用大部分依赖。

缺点：仍然需要 init container 检查和构建，PVC 还会引入访问模式、节点挂载和旧 cache 一致性问题。

### 方案 C：预构建 Connector Spark 镜像（推荐）

构建一次镜像，把以下内容直接放进去：

```text
/opt/spark-milvus/jars/spark-connector-assembly.jar
/opt/spark-milvus/native/*.so
/opt/spark-milvus/python/*
预热后的 Hadoop AWS/Ivy dependencies
```

镜像使用 digest 固定。运行 Pod 不再包含 `build-connector` init container，启动后直接做 SHA256 和 `ldd` readiness check。

优点：

- Pod 通常分钟级甚至秒级 Ready；
- 不依赖运行时公网下载和源码仓库；
- 每次运行使用完全相同的 Connector 产物；
- 最适合 CI、Nightly 和多人复用。

这是“稳定重复部署不出错”的最终方向。

## 11. 常见状态和处理方式

| 状态 | 含义 | 检查方式 |
| --- | --- | --- |
| `Pending` 且没有 Node | 调度失败，通常是 CPU/内存不足或 nodeSelector 不匹配 | `kubectl describe pod` 查看 `FailedScheduling` |
| `Init:0/1` 且 init Running | Connector 正在构建 | 查看 `build-connector` 日志和 CPU 进程 |
| `Init:CrashLoopBackOff` | 构建脚本失败 | 查看当前日志和 `--previous` 日志 |
| `PodInitializing` | init 已结束或正在切换主容器 | 等待数秒并检查 Events |
| `Running 0/1` | 主容器启动但产物/readiness 不满足 | 检查 `/opt/spark-milvus` 文件和 readiness probe |
| `Running 1/1` | Kubernetes 层 Ready | 继续执行第 7 节功能验收 |

构建失败日志：

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default logs "$POD" -c build-connector --tail=300

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default logs "$POD" -c build-connector --previous --tail=300
```

Pod Events：

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default describe pod "$POD"
```

## 12. ConfigMap 更新注意事项

`spark-submit-milvus.sh` 通过 ConfigMap `subPath` 挂载到：

```text
/usr/local/bin/spark-submit-milvus
```

更新 ConfigMap 后，已运行 Pod 中的 subPath 文件不会自动刷新。要让正式路径获得新版本，必须重建 Pod，但重建会丢失 `emptyDir` 并重新编译。

调试期间可以把新 wrapper 临时复制到：

```text
/workspace/spark-submit-milvus
```

并从该路径执行。这个做法只适合调试，不能当作可重复部署流程。

## 13. 清理

只删除 Spark Toolbox，不会删除 Milvus release：

```bash
kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default delete deployment spark-milvus-toolbox

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default delete configmap spark-milvus-toolbox-scripts

kubectl --kubeconfig /Users/zilliz/Desktop/kubecon/kubeconfig \
  -n default delete secret spark-milvus-toolbox-credentials
```

注意：删除 Deployment/Pod 会永久丢失当前 `emptyDir` 中的 Connector 产物和编译 cache。

## 14. 每次部署的最终检查清单

部署前：

- [ ] kubeconfig 可用，节点 Ready；
- [ ] namespace 正确；
- [ ] Milvus 19530/9091 Service 名称正确；
- [ ] MinIO 9000 Service 名称正确；
- [ ] MinIO Secret 和 key 名称正确；
- [ ] bucket/root path 正确；
- [ ] AMD64 节点至少能提供 4 CPU / 12 GiB；
- [ ] 构建期间不会触发 vcluster sleep；
- [ ] 外网依赖可访问。

部署后：

- [ ] init container exit code 为 0；
- [ ] Pod `Running 1/1`；
- [ ] Spark/Scala/Java 版本正确；
- [ ] Connector revision 正确；
- [ ] 三个主要 Artifact SHA256 正确；
- [ ] `ldd` 没有 `not found`；
- [ ] Milvus、Management API、MinIO 网络正常；
- [ ] Spark Pi 退出码为 0；
- [ ] Connector Read smoke 通过；
- [ ] Backfill Result、Commit segment status 和在线可见性均通过。

只要其中任意一项没有验证，就不能认为部署已经稳定可用。
