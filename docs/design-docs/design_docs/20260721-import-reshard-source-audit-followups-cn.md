# Milvus Import Re-shard 源码审计差异与后续工作

- **状态：** Audit Follow-up
- **审计日期：** 2026-07-31
- **更新日期：** 2026-08-04
- **原始材料：** `/home/zilliz/Downloads/2026-07-21-milvus-import-reshard-design-dark.html`
- **Milvus 基线：** `upstream/master@4d7d00e7f5ae9158ea79dbcfd7756ff288784dcc`（2026-08-04 复核）
- **AutoID 方案基线：** [milvus-io/milvus#51825](https://github.com/milvus-io/milvus/pull/51825)（2026-08-04 复核 head `cfbc0d30dc8b5e28f921d6065ef1026a3a3a9ac1`）
- **Cursor MergeSort 基线：** [milvus-io/milvus#51998](https://github.com/milvus-io/milvus/pull/51998)（2026-08-04 复核 head `983118dc71f512f2d2d67fae75afba1af7469ed0`）
- **milvus-storage 基线：** 仓库固定版本 `63c29c674bf8c75a84c49cca2c8ab088e771e57e`

## 1. 文档目的

本文只记录原始 HTML、会议结论、当前主设计与源码之间无法直接匹配的内容，以及正式设计落地前仍需完成的工作。Storage、资源模型和 TEXT 临时编码分别以主设计对应章节为准；未验证的端到端/生产性能收益、fan-in 和资源系数仍作为 follow-up。

审计遵循以下原则：

1. 当前 `upstream/master` 源码是 Milvus 行为的权威依据。
2. PR #51825 尚未合入，因此其代码按指定 head 单独审计，并明确记录依赖和未闭环项。
3. milvus-storage 只依据 Milvus 仓库声明的固定 revision 及该 revision 的上游源码；`build/`、`cmake_build/` 等本地构建目录完全排除在证据之外。
4. 所有不确定项均保留为 follow-up，不用推测填补源码缺口。

## 2. 结论摘要

| ID   | 优先级 | 不匹配或未闭环项                                         | 处理状态                                                                                                                      |
| ---- | ------ | -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| F-01 | P0     | `shard` 被错误定义为 `(vchannel, partition)`             | 主设计必须改为 routing bucket                                                                                                 |
| F-02 | P0     | `V×P` 被写成 segment 理论下界                            | 主设计必须改正公式                                                                                                            |
| F-03 | P0     | last-writer-wins 会允许旧 attempt 覆盖新 attempt         | 已确定改为更大 attempt ID 胜出                                                                                                |
| F-04 | P0     | 当前 import task 没有持久化 attempt fencing              | 需要 proto、catalog、请求和回报改造                                                                                           |
| F-05 | P1     | Storage V3 manifest 本身不提供 attempt-ID winner 语义    | 处理见主设计 §5.2 与 §6                                                                                                       |
| F-06 | P1     | 原生 Storage V3 迁移仍缺少必要的 Go/FFI 能力             | 当前使用自定义 ReshardManifest；未来条件见主设计 §5.2–§5.3                                                                    |
| F-07 | P1     | Milvus Storage manifest 文档与固定源码版本不一致         | 单列文档修正 follow-up                                                                                                        |
| F-08 | P1     | “128 MiB”混合了逻辑大小和物理对象大小                    | 必须定义精确度量                                                                                                              |
| F-09 | P0     | HTML 的确定性 AutoID 描述与 #51825 实现细节不完全一致    | 主设计按 #51825 完整重写                                                                                                      |
| F-10 | P0     | #51825 当前仍有依赖和边界缺口                            | 合入前必须闭环                                                                                                                |
| F-11 | P0     | 已物化 AutoID PK 会被当前 import 路径重新分配            | 需要显式 materialized 模式                                                                                                    |
| F-12 | P0     | “提前排序即可跳过 sort compaction”不成立                 | 需要逐项证明语义等价                                                                                                          |
| F-13 | P1     | `128 MiB × routing bucket 数` 可形成 2 TiB task          | 处理见主设计 §4.2 与 §5.1                                                                                                     |
| F-14 | P1     | 当前 grouping 不是 best-fit，且派发前拿不到 decoded size | 需要新的 estimator 和稳定装箱                                                                                                 |
| F-15 | P0     | “调度器零侵入、请求逐字节稳定”与源码不符                 | 需要 plan version 和 capability gate                                                                                          |
| F-16 | P1     | import slot 为固定小值或 `O(1)` 没有源码依据             | 模型见主设计 §4.3；生产系数仍需线上校准                                                                                       |
| F-17 | P0     | 坏文件可能在大量临时数据上传后才被报告                   | 主设计必须明确承认                                                                                                            |
| F-18 | P1     | 总 I/O、对象数、etcd 大小和性能收益被过度承诺            | 只能作为待验证假设                                                                                                            |
| F-19 | P1     | backup、L0、TEXT/LOB 和混版升级与当前能力存在差异        | 范围已确认，仍需实现与验证                                                                                                    |
| F-20 | P1     | Streaming Import ResourceKey 文档已落后于源码            | 文档自 #47297 起失配                                                                                                          |
| F-21 | P0     | 当前 MergeSort 与目标 fragment 协议存在实现差距          | [#51998](https://github.com/milvus-io/milvus/pull/51998) 正在改为 cursor k-way merge；配置上限为 16，规划时再固定可执行的 `K` |

## 3. 详细审计发现

### F-01：Milvus shard 是 vchannel，不是 `(vchannel, partition)`

原始 HTML 把 `shard` 定义为 `(vchannel, partition)`，但 Streaming System 的 channel 模型把 VChannel 定义为 collection 的逻辑 shard。当前 import 路由确实同时按 vchannel 和 partition 分桶，但该二维落点不应重定义 Milvus 的 shard 术语。

正式设计应统一使用：

```text
shard = vchannel
routing bucket = (vchannel, partition)
B = V × P
```

源码依据：

- `docs/agent_guides/streaming-system/channel/channel.md`
- `internal/datanode/importv2/hash.go:28-77`

### F-02：最终 segment 数不总是 `V×P`

原始 HTML 多处声称 re-shard 后 segment 数等于 `V×P`，并称其为理论下界。源码中 `AssignSegments` 会对每个 routing bucket 按 `segmentMaxSize` 循环分配 segment，因此一个大 bucket 可以产生多个 segment。

更准确的容量表达是：

```text
segment_count ≈ Σ_bucket ceil(bucket_final_bytes / segment_target_bytes)
```

其中空 bucket 不创建 segment；每个非空 bucket 最多保留一个未达到目标大小的尾段。Re-shard 消除的是“同一 bucket 被多个文件任务再次切碎”的额外 task 倍数，不能消除拓扑和数据倾斜本身造成的尾段。

源码依据：`internal/datacoord/import_util.go:162-230`。

### F-03：last-writer-wins 不满足重试正确性

旧的低 attempt DataNode 可能在新 attempt 完成后恢复并迟到写入共享 manifest key。last-writer-wins 会让提交时间而不是调度代次决定权威结果，导致旧 writer 覆盖新结果。

已经确定的设计约束是：

1. `attempt_id` 单调递增。
2. 每个 attempt 使用不可变、彼此隔离的对象存储前缀。
3. DataNode 只提交本 attempt 的不可变完成标记或 manifest。
4. DataCoord 根据持久化 attempt 状态裁决；更小 attempt 的迟到结果只能成为 GC orphan。
5. 任何 Storage manifest version 都不能替代 attempt fencing。

### F-04：当前 import task 没有持久 attempt ID

当前 `PreImportTask` 和 `ImportTaskV2` proto 均没有 `attempt_id`、`committed_attempt_id`、manifest reference 或 digest。`preImportTask.retryTimes` / `importTask.retryTimes` 只是 DataCoord 进程内字段；恢复 task 时不会从 catalog 恢复，也没有随 `CreatePreImport` / `CreateImport` 请求传给 DataNode。

当前 catalog 更新还是完整 protobuf 的普通 Save，只有单个 DataCoord 进程内锁提供串行化。因此落地至少需要：

- 在现有 Import task catalog protobuf 中持久化 `current_attempt_id`；
- 在 dispatch 前先持久化 attempt，再发送请求；
- 请求、查询和完成结果都携带 attempt；
- 在 DataCoord task 锁内比较 attempt 后更新完成状态；
- 持久化 `committed_attempt_id`、精确 manifest reference 和 digest；
- 明确定义旧 attempt、未来 attempt、重复完成和 DataCoord failover 的处理。

建议优先把 attempt 状态放入现有 task meta，而不是创建一套独立、容易与 task 状态失配的 etcd key。

DataNode 侧也必须同时改造，不能只在 DataCoord catalog 中增加字段。当前 `TaskManager` 只以 `taskID` 为 key，具体存在以下 fencing 缺口：

1. `Add` 遇到重复 `taskID` 只记录 warning 并返回，而 `PreImport` / `ImportV2` RPC 仍返回 success；新的较大 attempt 可能根本没有启动。
2. `Update` 只按 `taskID` 修改 clone；旧 attempt 的异步 goroutine 可能在新 attempt 建立后写入 state、reason、file stats 或 segment info。
3. `QueryPreImport` / `QueryImport` 只按 `taskID` 查询，响应也不携带 attempt；DataCoord 无法证明查询结果属于当前 attempt。
4. `DropImport` 只携带 `taskID`；迟到的旧 Drop 可能取消较大的新 attempt。
5. `Remove` 无条件 cancel 并删除 task，缺少“仅删除匹配 attempt”的条件语义。

因此 Create/Add、Update、Query、Drop/Remove 都必须携带并比较 `(task_id, attempt_id)`。同一 attempt 的相同 digest 重复完成应幂等；同一 attempt 返回不同 digest 应报告数据完整性错误，而不是任选一个结果。

源码依据：

- `pkg/proto/data_coord.proto:1399-1424`
- `internal/datacoord/import_task_preimport.go:38-45,103-116`
- `internal/datacoord/import_task_import.go:45-54,84-85,132-145`
- `internal/datacoord/import_meta.go:259-285`
- `internal/metastore/kv/datacoord/kv_catalog.go:779-785,813-820`
- `internal/datanode/importv2/task_manager.go:26-96`
- `internal/datanode/services.go:373-518`

### F-05：Storage V3 不会自动实现“更大 attempt 胜出”

Milvus 源码把 `StorageV3` 定义为 Loon manifest 格式，而不是一种独立的物理临时文件编码。milvus-storage 的 transaction commit 总是基于当前最高 manifest version 写入 `latest_version + 1`；version 表示提交顺序，不表示 import attempt ID。

因此以下做法都不正确：

- 用最高 manifest version 直接代表最高 attempt；
- 让不同 attempt 共用一个 base path，再依赖 Overwrite resolver；
- 让 DataNode 根据对象存储中“最新”文件自行裁决 winner。

本设计使用 attempt 隔离前缀和自定义 immutable ReshardManifest；DataCoord task meta 持有权威 attempt 和精确 manifest reference。

源码依据：

- `internal/storage/rw.go:41-48`
- `internal/core/thirdparty/milvus-storage/CMakeLists.txt:17-18`
- milvus-storage `63c29c6`：`cpp/src/transaction/transaction.cpp:348-415`

### F-06：原生 Storage V3 迁移仍缺少必要的 Go/FFI 能力

固定 revision 的 `ColumnGroupFile` 已有通用 `properties` map，但当前 Milvus Go/FFI 封装仍缺少：

- 通用文件枚举、property setter 和 property reader；
- Go `Flush`、多 writer output 汇总和 FAIL commit API；
- 按 bucket 选择 ColumnGroups/fragments 的通用 reader；
- TEXT/LOB selected reader、checksum 与 StoragePluginContext/CMEK；
- manifest 文件数、整文件加载内存和最大文件数保护。

普通 packed writer 仍会拒绝 TEXT schema；`LobFileInfo` 也没有 checksum，且其中的 `file_size_bytes` 是累计 raw payload bytes，不是对象实际 stat。

当前实现不依赖这些能力。未来原生 V3 迁移必须先补齐稳定 API 并完成容量、TEXT/CMEK 和完整性验证。

源码依据：

- milvus-storage `63c29c6`：`cpp/include/milvus-storage/column_groups.h:27-65`
- `internal/storagev2/packed/packed_writer_ffi.go:217-294`
- `internal/storagev2/packed/manifest_ffi.go:312-380,543-711`
- `internal/storagev2/packed/manifest_commit.go:28-71`
- `internal/storagev2/packed/manifest_commit.go:87-129`
- `internal/storagev2/packed/packed_reader_ffi.go:116-165`
- `internal/storage/record_writer.go:308-343`
- `internal/storagev2/packed/segment_writer_ffi.go:40-75`
- milvus-storage `63c29c6`：`cpp/include/milvus-storage/manifest.h:43-64`
- milvus-storage `63c29c6`：`cpp/src/lob_column/lob_column_writer.cpp:246-301`

### F-07：仓库内 manifest 设计文档已落后于固定源码版本

`docs/design-docs/design_docs/20260226-manifest-format.md` 仍把当前 manifest 写成 version 2，并声称有 `FailResolver`、`MergeResolver`、`OverwriteResolver` 三种内置 resolver。

固定的 milvus-storage `63c29c6` 实际为 manifest version 5，内置 resolver 只有 Fail 和 Overwrite；同时 `ColumnGroupFile.metadata` 已在 version 4 改为 properties map，version 5 又加入 LOB metadata。

这应作为独立文档修正，不应把旧文档中的 Merge 能力带入 re-shard 设计。

证据：

- `docs/design-docs/design_docs/20260226-manifest-format.md:34-52,248-257`
- milvus-storage `63c29c6`：`cpp/include/milvus-storage/manifest.h:34-40`
- milvus-storage `63c29c6`：`cpp/include/milvus-storage/transaction/transaction.h:152-173`

### F-08：128 MiB 只能先定义为逻辑 rollover target

milvus-storage 的 `writer.file_rolling.size` 按未压缩的 record-batch memory size 累计，并在 flush 时判断是否滚动；源码明确说明对象存储中的实际文件可能因编码和压缩而更小。C FFI 有显式 flush，但当前 Go wrapper 没有暴露。

所以正式设计不能写“每个 S3 临时对象严格为 128 MiB”或“物理大小不超过 128 MiB”。需要至少区分：

- `source_physical_bytes`
- `estimated_logical_bytes`
- `logical_bytes`
- `physical_object_bytes`
- `estimated_final_bytes`

在当前 API 下，`128 MiB` 最适合作为未压缩逻辑数据的 writer rollover target；每个完成对象的实际物理大小必须单独记录。

证据：milvus-storage `63c29c6` `cpp/src/properties.cpp:513-520`、`cpp/src/format/column_group_writer.cpp:45-74`。

### F-09：#51825 的确定性 AutoID 必须在主设计中完整展开

确定采用 #51825 的机制，但不能只留下 PR 链接。主设计需要明确描述以下完整链路：

1. DataCoord 在广播 Import 消息前为每个源 `ImportFile` 计算行数上界。
2. Parquet 从 footer 取精确 row count；NumPy 从 header shape 取精确 row count；JSON/CSV 使用文件字节数除以可证明的单行最小字节数，得到保守上界。
3. DataCoord 从 primary namespace 分配 ID，并给每个文件绑定字面区间 `[pk_id_begin, pk_id_end)`。
4. 区间随 WAL Import 消息复制，再经 ack、job meta、pre-import/import request 传到执行 DataNode。
5. DataNode 为每个文件建立 cursor；文件内第 `r` 行使用 `PK = begin + r`，并令 `RowID = PK`。
6. 后续 regroup、task 重派和文件组顺序变化不会改变已经绑定在该 `ImportFile` 上的范围。
7. 实际行数超过范围时硬失败，不回退到本地 allocator。

同时必须写清限制：

- 保证的是 PK 和 RowID；timestamp 仍可能按集群分别生成。
- 相同行数并不能证明主备对象内容或行序相同；需要对象不可变前提，最好记录 version/etag/checksum。
- deterministic 保证要求两个集群及执行 DataNode 都支持该字段；混版会 fail open。
- backup import 保留原 PK/RowID，不使用该分配路径。

### F-10：#51825 当前仍是开放 PR，并有合入前依赖与边界 follow-up

审计时 #51825 仍为 OPEN，且依赖的 [milvus-io/milvus-proto#641](https://github.com/milvus-io/milvus-proto/pull/641) 也仍为 OPEN。当前 PR head 未把包含 `common.IDRange` 和 `msgpb.ImportFile.pk_id_range` 的 go-api 版本更新到 Milvus，因此当前 CI build 失败。

此外，指定 head 还需要在合入前闭环：

1. 单文件最多只能保留一个连续、且不超过 `MaxUint32` 的范围；正式契约应明确“实际行数达到或超过 `2^32` 时返回输入错误”。
2. 当前 exact-count headroom 逻辑会把大于 `MaxUint32` 的精确 row count 截成 `MaxUint32`，与注释中的“应提前拒绝”不一致，会把错误推迟到 pre-import 后。
3. JSON/CSV 的保守上界可能在实际行数远低于 `2^32` 时就超过 `MaxUint32`；必须明确这是可接受的保守拒绝，或改进 range 表示。
4. 当前缺 range 的 DataNode 会回退本地 allocator；如果设计要求 deterministic 是强保证，需要 capability gate 或 fail-closed mode。
5. PR 中针对 deterministic assemble error 的 fail-fast 判定仍需确认实际 error 分类能命中。

正式文档应按 #51825 描述该机制，不能写“已经合入 master”。

### F-11：已物化 PK 不能直接复用当前 import 读取流程

Re-shard 临时数据若已经包含确定性 AutoID PK，当前 import 路径仍会调用 `AppendSystemFieldsData`。对于普通 autoID schema，默认会重新生成并覆盖 PK，同时生成 RowID 和 timestamp。

新路径至少需要显式内部模式，例如：

- `pk_materialized=true`
- 校验并保留已有 PK
- 规定普通 import 的 `RowID = PK`
- 不重复执行 nullable/default/dynamic normalization
- 在最终写 segment 时补 timestamp
- backup import 保留其原 PK、RowID 和 timestamp

源码依据：

- `internal/datanode/importv2/task_import.go:215-260`
- `internal/datanode/importv2/util.go:385-422`

### F-12：跳过 sort compaction 需要语义等价证明

当前 sort compaction 不只做 PK 排序，还执行或产出：

- deltalog delete 过滤
- collection TTL 和 TTL field 过滤
- function/materializer 补列
- commit timestamp 覆盖
- stats、BM25 stats 和 V3 manifest
- TEXT/LOB compaction 处理
- `IsSorted` / `IsSortedByNamespace`
- 输出 segment 的 row/stat metadata

新 import 路径若要直接产生 sorted segment，必须逐项说明由谁、在何时、以什么输入完成上述语义，并用老路径做结果等价测试。只写“读取临时文件后按 PK 排序并 sync”不足以跳过 sort compaction。

主设计定义 re-shard 生成 sorted runs、final k-way merge，并把 delete/TTL、timestamp、functions、hidden-column removal、TEXT finalization 和 stats 放在归并流上。在逐项等价测试完成前不得移除独立 sort compaction。

源码依据：`internal/datanode/compactor/sort_compaction.go:163-405`。

### F-13：`128 MiB × routing bucket 数` 在高基数场景不可作为无界 task 目标

实际重排 bucket 数是 `B = V × P`。典型 `V=16`、`P=1024` 时：

```text
B = 16,384
128 MiB × B = 2 TiB
```

若要求一个 task 给每个 bucket 都攒出接近 128 MiB 的单-bucket 文件，则 task 需要处理约 2 TiB 数据，带来数小时运行时间、本地 spill I/O 与清理量放大、极大的重试爆炸半径，以及在末尾发现坏文件时接近 2 TiB 的废弃产物。

主设计使用有界 task 和 single-bucket sorted streams，并允许 task tail。对象数超过生产门槛时再评估 physical bundling；final merge fan-in 超阈值时执行 hierarchical merge；task hard cap 不放大。

### F-14：当前 grouping 不是 best-fit，且派发前没有 decoded size

当前流程先按固定文件数创建 pre-import task，默认每 task 两个文件。只有全部 pre-import 完成并得到 `TotalMemorySize` 后，DataCoord 才重新分组 import task；当前算法按大小升序后做单当前箱的 next-fit，不是 best-fit。

若要求在派发 reshard task 前完成 grouping，DataCoord 当时通常只有源对象物理大小，无法直接获得 normalize 后逻辑大小。需要定义并持久化 estimator version：

- JSON/CSV：HEAD size 加格式经验系数；
- Parquet：footer compressed/uncompressed column-chunk size；
- NumPy：header shape/dtype；
- backup/binlog：listing 中的对象信息。

稳定装箱应使用确定性 tie-break，例如 `estimated_size DESC, file_id ASC, canonical_path ASC`。

源码依据：

- `internal/datacoord/import_checker.go:257-265,289-343`
- `internal/datacoord/import_util.go:391-424`
- `pkg/util/paramtable/component_param.go:6549-6557,6569-6578`

### F-15：“调度器零侵入”和“请求逐字节稳定”不成立

新流程至少需要持久化 attempt、plan version、manifest reference/digest、分组估算版本和资源提示，因此不是仅在 payload 内部增加几个字段。

当前 `AssembleImportRequest` 在每次组装时还可能现场分配 timestamp 和 log ID range，请求本身并非逐字节稳定。更重要的是，旧 DataNode 忽略新字段后只会执行旧 pre-import 统计，不会生成 reshard 临时产物；protobuf 的字段兼容性不等于行为兼容性。

主设计需要：

- `reshard_plan_version`
- DataNode capability 汇报和 scheduler gate
- 新旧 job 的恢复分支
- CDC 两端 capability 校验
- all-active-DataNode capability gate，fail-closed

源码依据：`internal/datacoord/import_util.go:310-388`。

### F-16：当前 slot 模型不能直接沿用

当前 import slot 近似为：

```text
max(
  floor(file_count / 4),
  floor(16 MiB × V × P / 160 MiB)
)
```

它不覆盖 task 输入字节数、编码工作集、对象存储带宽、PUT 并发和 sort 的逐行索引内存。`storage.Sort` 会先保留整个 segment 的 records 和行索引，因此 import 内存是 `O(one segment + rows)`，而不是严格常数。新设计明确不把本地磁盘 free space 或 estimated spill bytes 纳入 slot。

另外，全局 scheduler 在 task slot 超过任何节点可用 slot 时仍会 best-effort 派发；DataNode import scheduler 会启动所有 pending task，当前没有对新模型所需内存和稳定 reader/sorter/uploader concurrency 的原子 reservation。`dataNode.slot.slotCap=16` 的说明也与实际 `QuerySlot` 通过 `CalculateNodeSlots()` 计算容量的路径不一致。

`QuerySlot` 当前主要返回瞬时 available slots，而新 strict-fit 还需要 total capacity 与 DataNode CreateTask 原子复检。固定 16-slot floor 也不能直接用于总容量只有 4 slots 的 standalone/small node；此时必须拆 task 或使用不超过 total capacity 的 exclusive admission。

主设计在 §4.3 使用 dominant-resource scalar slot，同时要求 strict-fit 和 DataNode 对 memory/稳定并发资源做原子 reservation。本地磁盘容量视为无限，不进入节点选择、hard cap 或 admission；只保留 spill 目录可写性、可选紧急 low-watermark、`ENOSPC`/I/O error、清理和 metrics 等运行时防御。预试验只提供首期参数的辅助证据，完整数据在独立 benchmark 文档；生产系数仍缺少多 worker、更多 schema、S3 和完整 task pipeline 验证。

源码依据：

- `internal/datacoord/import_util.go:831-869`
- `internal/datacoord/task/global_scheduler.go:208-247`
- `internal/datanode/importv2/scheduler.go:75-105`
- `internal/datanode/services.go:610-646`
- `internal/storage/sort.go:41-223`

### F-17：必须承认坏文件报告延迟和废弃临时对象

Re-shard 仍能在最终 segment 写入前完成全量解析校验，但 pre-import 不再只是统计：它会同时重排并上传临时对象。若坏记录位于输入末尾，用户可能在绝大多数源数据已经处理、临时对象已经上传后才收到失败。

正式设计必须明确：

- 即使内部进度接近 99%，整个 job 仍会报告 Failed，而不是部分成功；
- 当前用户可见进度在 Failed 状态会回到 0；
- 当前源码没有“逐临时文件 deprecated”状态；更准确的语义是整个 attempt/job prefix 失去引用并成为 GC eligible；
- 多个失败 attempt 可同时留下接近 task 数据量的临时副本，不能只按一倍空间估算 quota。

源码依据：

- `internal/datacoord/import_util.go:570-631`
- `internal/datacoord/import_inspector.go:130-149`

### F-18：容量和性能收益只能作为待验证假设

原始 HTML 中以下断言没有足够源码或 benchmark 支撑：

- 总 I/O 一定下降；
- 性能不会回退；
- 临时空间只增加一倍；
- PUT/DELETE 一定无压力；
- etcd task meta 一定小于 10 KiB；
- binlog 数严格等于 `segment × field`；
- segment 数一定等于拓扑下界。

原因包括：sort compaction 可配置关闭、retry/双写放大、Storage V2/V3 文件布局不同、TEXT/LOB 有额外对象、function output 改变最终大小、以及 topology-induced tail segments 仍存在。

主设计已把这些改为可测量指标和故障注入 gate；预试验的完整结果独立保存，不能扩展成总 I/O、对象数、S3 性能或完整集群性能结论。

### F-19：首期范围已确认，但 TEXT/LOB 与混版能力仍需实现

原始 HTML 直接把普通格式、backup/binlog import 和拓扑变化纳入同一路径，但当前行为存在明显差异：

- backup import 已携带 PK、RowID、timestamp，不能走普通 AutoID materialization；
- L0 import 当前强制 Storage V2，且只处理 delete 数据；
- TEXT collection 依赖 Storage V3 的 TEXT-aware segment writer 和 LOB 语义，普通 packed writer 会拒绝 TEXT；
- 混版 DataNode 忽略新字段不代表能生成相同产物。

当前已经确认：

- 首期只覆盖普通 import；backup/binlog import 和 L0 import 为 non-goal；
- `DataType_Text` 在 re-shard 临时层通过显式 temporary schema 保存 raw UTF8，不创建临时 LOB；最终 TEXT-aware segment writer 再生成正式 inline/LOB；
- Import 依赖 `SharedCluster` 与 topology change 互斥，不支持 job 执行期间改变 replication topology；
- mixed-version 采用 all-active-DataNode capability gate 和 fail-closed，不允许旧节点 silent fallback。

当前 TEXT-aware writer 的实际规则是：小于 64 KiB 的值以内联 `[flag + payload]` 保存，其他值写入 Vortex LOB file，并在主文件保存 24-byte reference；LOB file rollover 和 buffer flush 的默认目标分别为 64 MiB 与 16 MiB。`rewrite` 会把旧 reference 解码为原文后重新编码，只有 `preserve-ref` 才可能避免 payload 重写，但当前 explicit-fragment reader 不解析 LOB reference。

仍需实现和验证的差异包括 temporary TEXT schema adapter、descriptor encoding version、TEXT-heavy logical-size estimator、最终 LOB path/metadata/checksum、selected-fragment LOB reader、StoragePluginContext/CMEK，以及失败时不发布部分 LOB/reference。S3 只提供 filesystem transport，不会自动消除上述协议与生命周期工作。

### F-20：Streaming Import ResourceKey 文档从 PR #47297 起与源码失配

Streaming 指南由 PR [#47936](https://github.com/milvus-io/milvus/pull/47936) 于 2026-03-02 引入，其中 Import 行写为没有 ResourceKey。

PR [#47297](https://github.com/milvus-io/milvus/pull/47297) 于 2026-03-25 合入 master，commit `56437f7a6ef9d50cc2358da658438b3d60c471ae`，把 Import 广播迁入 DataCoord，并通过 `startBroadcastWithCollectionID` 获取：

```text
SharedDBName + ExclusiveCollectionName
```

Broadcaster 又会自动附加 `SharedCluster`。replication topology 变更使用 `ExclusiveCluster`，因此 Import 在持有 SharedCluster 期间与 topology 变更互斥。`ExclusiveRequired` 仍应保持 `No`，因为该列描述的是 WAL append 的 VChannel/PChannel exclusive lock，不是 ResourceKey 的读写模式。

所以该 Streaming 文档从 #47297 合入 master 的 2026-03-25 起不再与代码一致。2.6 backport 为 PR [#48438](https://github.com/milvus-io/milvus/pull/48438)。

待办：修正 `docs/agent_guides/streaming-system/message/message-semantic-collection.md` 的 Import 行，并补充 Import 与 replication topology 变更的互斥说明。

源码与历史依据：

- `internal/datacoord/ddl_callbacks.go:69-81`
- `internal/datacoord/ddl_callbacks_import.go:169-200`
- `internal/streamingcoord/server/broadcaster/broadcast_manager.go:111-135`
- `internal/streamingcoord/server/service/assignment.go:86-110`

### F-21：内部有序的 fragment + cursor merge 已验证本地内存趋势，但生产协议仍未闭环

`storage.Sort` 会读取并保留全部输入 records，并建立逐行索引和 sort workspace；因此峰值内存与 final segment decoded bytes 和 row count 同阶。预先生成内部有序的 fragments 后做 k-way merge，可将 final 阶段内存边界收敛到“各 reader 当前 batch + merge heap/cursors + output/materializer batch”。

但当前源码存在以下差距：

1. `storage.MergeSort` 只在所有输入已经按完全相同的 fields 排序时正确；原始 re-shard 设计明确不排序临时对象。
2. 当前 master 把每个 reader 当前 record batch 中的所有有效行都加入 heap，不是每个输入只保留一个 head；开放 PR [#51998](https://github.com/milvus-io/milvus/pull/51998) 正在改为标准 cursor k-way merge，主设计把其合入或等价实现作为 capability 前置条件。
3. Key 相同时当前 tie-break 使用 reader index 和 row index，不等价于 job 级确定性的源文件顺序。显式 PK 路径需要 temporary hidden `source_file_ordinal` 和 `source_row_offset`，并把它们写入实际 sort specification；AutoID PK 唯一时可只按 PK（或 namespace 下 partition key + PK）排序。
4. Namespace 开启时当前排序顺序是 partition key 后接 PK，不能只记录 `pk_sorted=true`。
5. 一个临时 fragment 对应一个 packed 对象，只要求内部单调；不同 fragments 的键范围可以重叠，Final 必须归并而不能直接拼接。
6. 高 bucket 场景可能让一个 final segment 合并数百或数千个 task tails。当前 compaction 在 sorted segment 数超过默认 30 时禁用 MergeSort，因此必须设置 fan-in gate，并在超限时执行 hierarchical merge。
7. Re-shard 端新增 external sort 会增加 CPU、本地临时 I/O、运行时磁盘故障处理、恢复和 GC 复杂度。Int64 当前 full sort 使用 radix sort，merge 路径总 CPU 可能更高；收益主要是内存和 OOM 风险，而不是总计算量必然下降。
8. v1 应让一个 Import task 为同一 bucket 发布多个有界、内部有序的 fragments，并让 DataCoord 按完整 fragment 规划。这样不需要范围切分协议；单个 fragment 超过 segment 目标时独占一个 segment。
9. 单调性必须在消费端逐行验证。只检查 min/max 或 batch boundary 不能发现 Record 内无序；当前实现还可能在旧 heap index 尚未消费时推进并替换 Record。
10. `RecordReader.Next()` 返回 borrowed Record，cursor 必须消费完当前 batch 后再推进，或显式 Retain/Release；错误、取消和 writer failure 路径均需释放资源。
11. 目标 heap 应为每路输入保存一个值类型 head，并缓存 typed key；当前逐行 `*index`、重复 `Column()`/类型断言会增加 allocation、GC 和 CPU。
12. Comparator 不能从 reader 0 的首 Record 推断类型；首 reader 为空或全部 reader 为空必须安全。连续全过滤 batches 也应迭代跳过，不能递归。
13. Reader/output batch 和 128 MiB rollover 是软阈值，内存预算要包含最大单行导致的 overshoot。
14. `storage.Sort` 的 `batchSize` 只限制输出 RecordBatch，不限制它读入并保留的排序输入；Re-shard 必须先按 attempt 内存预留切出当前 fragment，再把有界 reader 交给 sorter。
15. 一个反复关闭并追加的 Arrow IPC stream 不可直接作为 spill file：writer 关闭会写 EOS，普通 reader 会在第一个 EOS 停止。需要使用可重开的长度分帧格式，并限制同时打开的文件数。
16. 满 bucket 在取得排序字节额度前不能离开 bucket；否则 `reshardSortConcurrency=1` 仍可能让多个 128 MiB 批次在等待队列中占满内存。

现有生产测试本身不足以作为新协议的正确性证据：`TestMergeSort` 的 writer 只检查每个输出 Record 的第一行；64 MiB output batch 下通常无法逐行发现内部乱序。

进程级 packed-storage 预试验已覆盖 full sort、当前 MergeSort 与 cursor merge；完整环境、数据和结果只保留在 [独立 benchmark 文档](./20260721-import-reshard-sort-benchmark-cn.md)，不在本源码差异文档重复。

该结果只关闭了“指定本地 packed workload 下是否存在峰值内存收益”的问题。它仍未覆盖 predicate、duplicate PK、namespace、VarChar PK、TEXT/functions、wide vectors、S3、错误路径、高 fan-in、多 worker 并发和完整 Import task pipeline，不能替代生产协议测试，也不能直接给出 slot 系数或端到端性能结论。

待实现与验证项：

- 每个 routing bucket 生成一个或多个带完整 sort spec/version 的有序 fragments；
- Planner 只引用完整 fragment；
- 首期 temporary layout 使用单 column group；
- 内存桶或本地 spill file 在每次上传前取一批数据排序，并由 writer 写成一个 fragment；
- Final Import 使用 #51998 或经等价验证的 one-head-per-input merge，并逐行验证 input/output monotonicity；
- fan-in 超限时使用 hierarchical merge；
- `dataCoord.import.maxFinalMergeFanIn=16` 是配置上限；规划时按 DataNode 总 reader/内存容量固定实际 `K`；扩展验证范围与指标统一维护在独立 benchmark 文档。

源码依据：

- `internal/storage/sort.go:56-223`
- `internal/storage/sort.go:326-438`
- `internal/storage/record_writer.go:41-179`
- `internal/storagev2/packed/packed_writer.go:40-196`
- [milvus-io/milvus#51998](https://github.com/milvus-io/milvus/pull/51998)
- `internal/storage/record_reader.go:19-32`
- `internal/storage/sort_test.go:218-280,315-349`
- `internal/datanode/compactor/mix_compactor.go:469-487`
- `internal/datanode/compactor/merge_sort.go:100-144`
- `internal/datanode/services.go:268-300`
- `configs/milvus.yaml:919-920`

## 4. 建议的验证清单

### 4.1 正确性

- 对每种 PK、partition key 和稀疏/倾斜分布，逐行对照现有 `HashData` 路由结果。
- 同一 job 在 regroup、重试、DataCoord failover 和跨集群执行后，PK/RowID 完全一致。
- 旧 attempt 在新 attempt 后迟到提交，不能改变 task 的权威 manifest reference。
- manifest 损坏、缺文件、checksum 不匹配、对象被覆盖均按数据完整性错误失败。
- 新 import+sort 路径与旧 import+sort-compaction 对 delete、TTL、functions、timestamps、stats、sorted flags 和 TEXT/LOB 逐项等价。
- 每个 fragment 内逐行保持单调；duplicate PK 使用 source file ordinal/row offset 保持 retry 确定性。
- Namespace sort specification 为 partition key、PK 和稳定 source-order tie-break。
- Fan-in 超限时 hierarchical merge 与直接 merge 输出一致。

### 4.2 故障注入

- 上传中途 kill、manifest commit 前 kill、manifest commit 后回报前 kill。
- DataCoord 在 attempt 分配前后、完成结果持久化前后 failover。
- 旧 attempt zombie writer 晚于新 attempt 完成。
- S3 throttle、5xx、timeout、partial multipart、checksum mismatch。
- emergency low-watermark、`ENOSPC`、spill 目录只读/不可写、其他本地 I/O failure、进程重启、任务取消和 orphan sweep。
- 99% 处理后发现坏记录，确认失败原因、进度和 GC 行为。

### 4.3 容量与性能

- `V=16, P=1024` 的对象数、manifest 大小、规划延迟和 GC 时间。
- 小文件、单大文件、极端倾斜、空 bucket 和大量尾文件。
- 继续比较 current `storage.Sort`、current `storage.MergeSort`、cursor merge 和单路 Re-shard 有序输入的 CPU、PSS/RSS、cgroup anon/file、spill、读写字节、OSS 请求数和端到端延迟。
- 补齐 Int64/VarChar PK、namespace、fan-in `4/30/100/1000`，以及大量 1 MiB tails。
- Narrow rows、wide vectors、raw UTF8 TEXT 和 function output 对 merge memory/CPU 的影响。
- 单节点 1/2/4 worker 并发与 slot estimator 校准。
- retry 产生两份及以上 attempt 数据时的 quota 放大。

## 5. 后续文档工作

- 中文主设计已经生成；自定义 ReshardManifest、slot 模型、TEXT 临时编码和 fragment 排序/归并约束均已写入对应设计章节。
- Benchmark 数据与扩展矩阵统一维护在独立报告；后续结果只用于更新 batch size、spill telemetry、slot 系数和 legacy enablement gate。
- 更新 Streaming Import ResourceKey 行及历史说明。
- 更新 `20260226-manifest-format.md` 到固定 milvus-storage revision 的真实 version 和 resolver 列表。
- #51825 / milvus-proto#641 合入后重新核对代码、依赖、CI、混版行为和 `2^32` 边界。
