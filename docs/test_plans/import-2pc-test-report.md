# Import 2PC E2E 测试报告

## 基本信息

| 项 | 内容 |
| --- | --- |
| 测试对象 | Import 两阶段提交（2PC）与 `commit_timestamp` |
| 目标版本 | Milvus 2.6.x / master 验证实例 |
| 报告日期 | 2026-06-11 |
| 测试目录 | `tests/restful_client_v2` |
| 测试计划 | `docs/test_plans/import-2pc-test-plan.md` |
| 执行记录 | `docs/test_plans/import-2pc-execution.md` |
| 测试代码提交 | `1bd82bfc4a test: add import 2pc infra coverage` |

## 测试结论

Import 2PC 的核心写一致性链路在真实实例上已完成较全面 E2E 覆盖：手动 import 在 `Uncommitted` 前不可见，`CommitImport` 后最终可见；主备 CDC 场景下 primary/secondary 最终 PK 集合一致；delete 与 TTL 的关键 `commit_timestamp` 语义通过；DataCoord/MixCoord/DataNode/StreamingNode 重启、GC、quota、max job 等 infra-dependent 场景已通过专项验证。

当前不建议把该特性直接按“完全无风险”放行。仍有 4 个明确产品缺陷和 1 个 compaction 风险以 `xfail` 挂在自动化中，另有若干需要独立环境继续覆盖的场景。最高优先级是 RBAC：未授权用户当前可以通过 REST `commit`/`abort` 操作他人 import job。

## 测试范围

已覆盖范围：

- REST import API：`create`、`describe`、`get_progress`、`list`、`commit`、`abort`。
- 生命周期：`auto_commit=true`、`auto_commit=false`、`Uncommitted` 停留、显式 commit、abort、invalid state、空文件、多 job。
- `commit_timestamp` 语义：commit 前不可见、commit 后可见、delete 前后边界、混合 delete、TTL 从 commit 时间起算。
- DML 交织：insert、upsert、delete、flush、manual compact during Uncommitted。
- 数据矩阵：标量、JSON、Array、nullable、default、dynamic field、auto_id、Float/Binary/Sparse/Half/Int8/ArrayOfVector、Geometry/Timestamptz。
- 索引矩阵：HNSW、IVF、DISKANN、FLAT/SCANN、BIN_FLAT/BIN_IVF、SPARSE、标量索引。
- 文件格式：Parquet、JSON、JSONL、CSV、NumPy、多文件、非法后缀、非法 CSV sep。
- 分区与布局：指定 partition、partition key、clustering key、多 vector、多 vchannel。
- Function：BM25 input-only、BM25 output guardrail。
- 复制与故障：CDC happy path、CDC delete boundary、CDC abort、CDC DML interleaving、CDC commit idempotency、DataCoord/MixCoord/DataNode/StreamingNode restart。
- Infra 边界：`maxImportJobNum`、disk quota、短 GC 周期后再 commit。
- Backup/L0：真实源实例生成 binlog/delta log 后复制到目标实例验证，覆盖 V1/V2 backup、V1/V2/V3 L0；V3 backup 通过 fail-closed guardrail 覆盖 REST prefix reader 边界。

未完全覆盖或需要独立套件的范围：

- CDC 网络分区 / secondary missed `CommitImportMessage` 后重放恢复。
- secondary import 尚未 ready 时 primary commit 的阻塞与最终完成。
- REST 层 time-travel 查询 `commit_ts` 前快照不可见：当前尝试的低层 REST `/query` 路径返回 404，已移出自动化。
- StorageV3 backup manifest happy path：当前 REST `backup=true` prefix reader 不能诚实验证 manifest restore，需要 backup-tool/copy-segment 级别套件。
- 加密 backup `ezk` happy path、`start_ts/end_ts` 对真实 backup insert/delta 的窗口过滤 happy path。
- 长稳 30min+ 和吞吐性能基线。

## 自动化资产

| 文件 | 覆盖 |
| --- | --- |
| `tests/restful_client_v2/testcases/test_import_2pc_operation.py` | 常规 REST E2E，144 个测试函数，143 个 L0 marker，5 个 xfail |
| `tests/restful_client_v2/testcases/test_import_2pc_infra_dependent.py` | L3 专项/infra-dependent，11 个测试函数，参数化后 14 条 pytest case |
| `tests/restful_client_v2/pytest.ini` | 新增 `L3` marker，用于避免专项 case 默认进入 CI |
| `docs/test_plans/deployments/import-2pc-chaos-and-cdc.yaml` | cluster/CDC 专用实例定义，Woodpecker、16 DML channels、authorization enabled |

收集结果：

```text
158 tests collected in 3.29s
```

执行记录是按单 case 和 batch 增量真实执行，不是一次单命令全量绿跑。报告中的结论按最终重跑结果归一化，已剔除测试代码修正前的中间失败。

## 测试环境

| 环境 | 用途 | 关键信息 |
| --- | --- | --- |
| `import-2pc-playground` | 单集群常规 REST 验证 | `http://10.100.36.203:19530`，image `harbor.milvus.io/manta/milvus:master-20260609-385caab`，Python 3.12.12 |
| `import-2pc-chaos-cluster` | 故障恢复、GC、quota、max job | cluster mode，`http://10.100.36.215:19530`，MinIO `10.100.36.214:9000`，root `files` |
| `import-2pc-cdc-primary` / `import-2pc-cdc-secondary` | 主备复制 | primary `http://10.100.36.217:19530`，secondary `http://10.100.36.216:19530`，16 pchannels |
| `issue-49468-master-0611` | storage fixture source/target 验证 | `http://10.100.36.229:19530`，MinIO `10.100.36.228:9000` |
| `import-2pc-fixture-v1` | StorageV1 源数据生成 | `milvusdb/milvus:v2.5.27` 同步到 Harbor 后部署，用于生成真实 V1 binlog |
| `import-2pc-fixture-v3` | StorageV3 源数据生成 | master image，`common.storage.useLoonFFI=true` |

## 关键通过项

| 分类 | 结果 |
| --- | --- |
| 生命周期 | `auto_commit=true` 自动完成；`auto_commit=false` 停在 `Uncommitted`；显式 commit 到 `Completed`；重复 commit 不重复生效；abort 后 commit 被拒绝；空文件、多次连续 import 正常 |
| 可见性 | `Uncommitted` 期间 count/query/search 不可见；commit 后轮询可见；release/reload 后仍可见 |
| Delete/MVCC | commit 前 delete 对 import rows 无效；commit 后 delete 生效；混合 delete 边界正确；auto_id + scalar delete 正确 |
| TTL/GC | TTL 从 commit 时间起算；短 GC 周期内 Uncommitted segment 不被 GC 误删，后续 commit 后数据正常可见 |
| CDC | primary/secondary happy path 一致；delete boundary 一致；abort cleanup 一致；DML interleaving 一致；重复 commit 和多 job 无重复/串扰 |
| DML 交织 | import 与 insert/upsert/delete/flush/compact during Uncommitted 互不破坏可见性 |
| 数据类型/索引/格式 | 主流标量、向量、JSON、Array、nullable/default/dynamic、BM25、常见索引和 Parquet/JSON/CSV/NumPy 均通过 |
| Infra | DataCoord/MixCoord/DataNode/StreamingNode restart 后任务能恢复；`maxImportJobNum=1` 能拒绝额外 job；1 MiB disk quota 能触发 import quota failure 且无脏数据 |
| Backup/L0 | 真实源 binlog/delta log 复制导入路径可运行；L0 删除在 commit 前不生效、commit 后生效；V3 backup REST prefix 不会误读 manifest 数据 |

## 缺陷与风险

### HIGH: REST commit/abort 缺少 import 权限校验

- Case: `IMP-SEC-101`
- 自动化: `test_import_2pc_user_without_import_privilege_cannot_operate_jobs`
- Issue: `milvus-io/milvus#50458`
- 现象: 无 Import 权限用户调用 `/create`、`/get_progress`、`/describe` 被拒绝，但 `/commit` 和 `/abort` 返回 `code=0`，可以推进或终止 root 用户创建的 job。
- 影响: 授权开启的多租户/企业环境存在越权修改 import job 状态的风险。
- 建议: 发布前修复或明确限制 REST import commit/abort 的暴露范围；修复后去掉 strict xfail 并重跑。

### HIGH: FloatVector NaN/Inf 被 import 接受

- Case: `IMP-TYP-209`
- 自动化: `test_import_2pc_vector_nan_inf_fails_with_reason_and_no_visible_rows`
- Issue: `milvus-io/milvus#50459`
- 现象: Parquet 中包含 `NaN` / `Inf` 的 FloatVector 行，import job 进入 `Uncommitted`，`importedRows=3`。
- 影响: 如果后续 commit，可能引入非法向量数据，影响检索正确性或下游索引稳定性。
- 建议: DataNode import reader 或校验阶段拒绝非有限浮点；保留 no-visible-dirty-data 断言。

### MEDIUM: committed import segment 参与 manual compaction 后任务长时间 Executing

- Case: `IMP-CMP-101`
- 自动化: `test_import_2pc_manual_compaction_after_commit_preserves_rows`
- Issue: `milvus-io/milvus#50464`
- 现象: commit 后数据已可见且 count 正确，触发 manual compaction 后 compaction plan 600s 内保持 `Executing`。
- 影响: 未证明 import segment 与普通 segment compaction 完成后的查询正确性；更像 compaction 子系统风险，不是 2PC 可见性 bug。
- 建议: 修复 compaction 卡住问题或提供可稳定调度/完成的测试环境；该 case 当前 `xfail(strict=False)`。

### MEDIUM: `options.auto_commit=null` 被 REST create 接受

- Case: `IMP-REST-133`
- 自动化: `test_import_2pc_create_rejects_null_auto_commit_option`
- Issue: `milvus-io/milvus#50460`
- 现象: REST option contract 期望 option values 为 string；boolean/array 均被拒绝，但 `null` 被接受并创建 job。
- 影响: API contract 不一致，客户端错误请求可能被当作默认 auto commit 处理。
- 建议: 统一 REST JSON option value type 校验。

### MEDIUM: AbortImport 对 Failed job 重试不幂等

- Case: `IMP-ABT-003`
- 自动化: `test_import_2pc_abort_is_idempotent_for_failed_job`
- Issue: `milvus-io/milvus#50483`
- 现象: 第一次 `/abort` 成功并把 job 转到 `Failed` 后，第二次 `/abort` 返回 `code=2100`，message 为 `job ... is in terminal/committed state Failed, abort not allowed`。
- 影响: 客户端或管控面重试 abort 时无法安全做到 at-least-once 调用，可能把成功清理误判为失败。
- 建议: `AbortImport` 对已由 abort/cleanup 进入 `Failed` 的同一 job 返回成功，并保持 job 状态和不可见性不变；当前自动化已改为 strict xfail 记录该缺口。

### CONFIRMED: mixed file groups 是支持能力

- Case: `IMP-FMT-205`
- 自动化: `test_import_2pc_mixed_file_formats_across_file_groups_commits_all_rows`
- 结论: parquet 与 CSV 放在不同 file group 中的同一个 import request 是支持场景。测试计划已更新为正向验证 mixed file groups，同时保留非法后缀和非法 CSV separator 的负向验证。

## 未覆盖项与后续计划

| 优先级 | 项目 | 当前状态 | 后续动作 |
| --- | --- | --- | --- |
| P0/P1 | CDC secondary not ready 时 commit | 只有环境门禁，不是完整行为验证 | 构造 secondary import 延迟或暂停 DataNode/StreamingNode 的 CDC 专项套件 |
| P1 | CDC 网络分区后 CommitImport replay | 只有环境门禁 | 用 kubectl/网络策略暂停 secondary 消费，再恢复验证 WAL replay |
| P1 | time-travel snapshot before commit_ts | REST 路径未打通 | 使用正确 REST endpoint 或 SDK/Go e2e 补历史快照断言 |
| P1 | StorageV3 backup happy path | REST prefix reader 只能 fail-closed | 用 backup tool / copy segment manifest 元数据套件验证 |
| P1 | real backup `start_ts/end_ts` window | 当前覆盖 parse/guardrail，不覆盖真实窗口过滤 | 用真实 V1/V2/V3 insert/delta fixture 补窗口内外数据断言 |
| P2 | encrypted backup `ezk` | 仅覆盖非法 option type | 准备加密 fixture 后验证解密导入 |
| P2 | 长稳与性能 | 未执行 30min+ / perf baseline | 单独压测套件记录吞吐、索引耗时、CDC lag |

## 回归建议

发布前建议至少完成：

1. 修复并重跑 `#50458`、`#50459`、`#50460`、`#50483`。其中 `#50458` 属于权限边界问题，应作为 release gate。
2. 对 `#50464` 给出归属判断：如果 compaction 卡住与 import segment metadata 有关，应纳入 release blocker；如果是环境/compaction 调度问题，保留 xfail 并在 release note 中说明测试限制。
3. mixed-format import 契约已明确为支持 mixed file group；继续保留非法格式/非法 sep 的负向回归。
4. 补齐 CDC 网络分区、secondary not-ready、StorageV3 backup manifest happy path 的独立套件或签署延期风险。

建议回归命令：

```bash
cd tests/restful_client_v2
../../.venv/bin/python -m pytest --collect-only -q \
  testcases/test_import_2pc_operation.py \
  testcases/test_import_2pc_infra_dependent.py

../../.venv/bin/python -m pytest -q -rs \
  testcases/test_import_2pc_operation.py \
  --endpoint <milvus-http> \
  --token root:Milvus \
  --minio_host <minio-host> \
  --bucket_name <bucket> \
  --root_path <root-path>

../../.venv/bin/python -m pytest -q -rs -m L3 \
  testcases/test_import_2pc_infra_dependent.py \
  --endpoint <cluster-http> \
  --token root:Milvus \
  --minio_host <cluster-minio-host> \
  --bucket_name <bucket> \
  --root_path <root-path> \
  --release_name <milvus-cr-name>
```

## 签核判断

| 条件 | 当前判断 |
| --- | --- |
| 单集群核心 2PC 生命周期 | 通过 |
| `commit_timestamp` delete / TTL / GC 核心语义 | 通过 |
| CDC happy path 与常规一致性 | 通过 |
| REST API contract | 部分通过，`auto_commit=null` 有缺陷 |
| RBAC | 不通过，commit/abort 越权 |
| Backup/L0 | 部分通过，V3 backup happy path 需独立套件 |
| Release 建议 | 修复 RBAC 后可进入下一轮 release candidate 验证；当前不建议直接最终放行 |
