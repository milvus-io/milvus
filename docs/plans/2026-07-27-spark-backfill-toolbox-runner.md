# Spark Backfill Toolbox Runner 实现计划

**目标：** 让现有 Spark Backfill pytest 可以直接在一个已经构建好 Connector 的 Toolbox Pod 中执行 Read 和 Backfill，同时保留原有一次性 Kubernetes Job 模式。

**架构：** 新增 `ToolboxSparkRunner`，通过 Kubernetes Pod exec 在 Toolbox 中调用 `/usr/local/bin/spark-submit-milvus`。pytest 仍在本地管理 Collection、Snapshot、Parquet、Result、Commit、证据和清理。Toolbox 生命周期不由测试管理；未来 Nightly 可以先启动一个现场编译 Connector 的 Builder/Toolbox Pod，等待 Ready 后再运行同一套 pytest。

**技术栈：** pytest、Kubernetes Python SDK、PyMilvus、MinIO SDK、PyArrow、Spark 4.0.1 local mode。

---

## 方案选择

### 方案 A：继续只使用一次性 Job + HTTPS bundle

优点是执行隔离完整，缺点是依赖 Connector Release 或额外的 bundle 发布系统，不能方便验证 spark-milvus 最新提交。

### 方案 B：pytest 直接 exec 已有 Toolbox Pod（本次实现）

优点是复用现场编译产物，不需要 bundle URL，最适合当前手工功能验证。测试保持串行，通过唯一 evidence run id 隔离日志。缺点是多个并发测试不能共享同一个 Spark local runtime，因此仍强制 `-n 0`。

### 方案 C：每次 pytest session 自动创建 Builder Pod/PVC

适合最终 Nightly，但会把长时间编译、缓存、Pod 生命周期和测试执行同时引入首期变更。首期只让 Toolbox Runner 能按 Pod 名或 label 连接；Builder Pod 的创建与等待后续作为独立 fixture/Pipeline 阶段实现。

## 关键约束

- Toolbox Pod 必须已经 `Running` 且目标容器 Ready。
- Pod 内必须存在 `/usr/local/bin/spark-submit-milvus`、Connector JAR 和 native libraries。
- Backfill 凭证从 Toolbox Pod 自己的 `S3_ACCESS_KEY`/`S3_SECRET_KEY` 环境变量展开，不写入本地命令或证据。
- Read Probe 源码通过 base64 注入 Toolbox 的 `/workspace/spark-backfill-pytest`，不要求重建 Pod 或修改 ConfigMap mount。
- 远端命令使用唯一退出码标记；Runner 不依赖 websocket client 的 return code 实现细节。
- 超时由 Pod 内 GNU `timeout` 控制，超时退出码为 124。
- Toolbox 模式不创建 ConfigMap、Secret 或 Job；最低 RBAC 为 get/list pods 和 get pods/exec。
- Job 模式继续要求 HTTPS bundle URL/SHA，并保持原行为。

### 任务 1：配置和 CLI

**文件：**

- 修改：`tests/python_client/conftest.py`
- 修改：`tests/python_client/spark_backfill/config.py`
- 测试：`tests/python_client/spark_backfill/test_config_unit.py`

**步骤 1：写失败测试**

- Toolbox 模式允许 Connector URL/SHA 为空。
- Job 模式仍拒绝空 URL/SHA。
- Toolbox 模式要求 Pod 名或非空 label。
- 新参数规范化为 `runner_mode`、`toolbox_pod`、`toolbox_label`、`toolbox_container`、`toolbox_wrapper`、`toolbox_workspace`。

**步骤 2：运行测试验证失败**

```bash
python3 -m pytest tests/python_client/spark_backfill/test_config_unit.py -q
```

**步骤 3：实现最小配置代码**

增加：

```text
--spark-runner-mode {job,toolbox}
--spark-toolbox-pod
--spark-toolbox-label
--spark-toolbox-container
--spark-toolbox-wrapper
--spark-toolbox-workspace
```

**步骤 4：运行测试验证通过。**

### 任务 2：Toolbox RBAC

**文件：**

- 修改：`tests/python_client/spark_backfill/k8s_resources.py`
- 测试：`tests/python_client/spark_backfill/test_k8s_resources_unit.py`

**步骤 1：写失败测试**

Toolbox 权限必须只包含：

```text
get pods
list pods
get pods/exec
```

**步骤 2：运行失败测试。**

**步骤 3：增加按 runner mode 生成和校验 RBAC 的函数。**

**步骤 4：运行测试验证通过。**

### 任务 3：ToolboxSparkRunner

**文件：**

- 创建：`tests/python_client/spark_backfill/toolbox_runner.py`
- 创建：`tests/python_client/spark_backfill/test_toolbox_runner_unit.py`

**步骤 1：写失败测试**

覆盖：

- 显式 Pod 名解析；
- 按 label 选择唯一 Ready Pod；
- 0 个或多个 Ready Pod 时 fail-fast；
- Read Probe/contract 文件 base64 注入；
- Backfill 命令使用 Pod 内 S3 环境变量；
- Read 命令使用 Pod 内 token/S3 环境变量；
- 不把真实凭证写入命令或证据；
- 成功、普通失败和 timeout 退出码解析；
- pod.log、command.json、result.json 保存。

**步骤 2：运行测试验证 ImportError/缺少实现。**

**步骤 3：实现最小 Runner。**

Runner 复用现有：

```python
SparkJobRequest
SparkJobResult
```

通过可注入 `exec_stream` 函数进行单元测试，默认使用 `kubernetes.stream.stream`。

**步骤 4：运行测试验证通过。**

### 任务 4：pytest fixture 路由

**文件：**

- 修改：`tests/python_client/spark_backfill/conftest.py`
- 修改：`tests/python_client/spark_backfill/k8s_resources.py`
- 测试：相关 config/RBAC/runner 单元测试

**步骤 1：为 fixture 选择行为写失败测试或拆出纯构造函数测试。**

**步骤 2：Toolbox 模式：**

- 不创建支持 ConfigMap；
- 不创建临时 Secret；
- 检查 pods/exec RBAC；
- 读取本地 `contracts.py` 和 `read_probe.py` 内容交给 Runner；
- 构造 `ToolboxSparkRunner`。

**步骤 3：Job 模式保持现有资源和 Runner。**

**步骤 4：运行全部非 E2E 单元测试。**

### 任务 5：文档

**文件：**

- 修改：`tests/python_client/spark_backfill/README.md`
- 修改：`tests/python_client/spark_backfill/deploy/manual_toolbox/README.md`

增加：

- 当前 Toolbox 手工运行命令；
- 按 label 自动发现 Pod 的推荐方式；
- 指定 Pod 名的调试方式；
- 本机 Milvus/MinIO port-forward；
- Nightly Builder/Toolbox Pod 的未来数据流；
- Job 模式和 Toolbox 模式差异。

### 任务 6：验证

**步骤 1：运行全部非 E2E 单元测试。**

```bash
python3 -m pytest tests/python_client/spark_backfill \
  --run-spark-backfill -m 'not spark_e2e' -n 0 -q
```

**步骤 2：静态检查。**

```bash
python3 -m py_compile tests/python_client/spark_backfill/*.py
bash -n tests/python_client/spark_backfill/deploy/manual_toolbox/*.sh
git diff --check
```

**步骤 3：当前 Pod 预检。**

确认 `spark-milvus-toolbox-795f57f78c-zmlvr` Ready，wrapper/JAR/native libraries 存在。

**步骤 4：运行单个 V3 coalesce E2E。**

通过本机 port-forward 连接 Milvus/MinIO/Management API，Runner 通过 K8s exec 使用 Toolbox Pod。

**步骤 5：检查证据和资源清理。**

确认 Result、Commit、在线可见性通过，测试 Collection/Snapshot/prefix 被清理，Toolbox Pod 保留。

### 任务 7：提交

仅暂存本次 Spark Backfill 相关文件，保留其他 untracked 文件不动。提交信息：

```text
test: add Spark backfill toolbox runner
```
