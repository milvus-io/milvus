# V3 segment columnSplit：重启恢复问题与修复

## 分支与结论

分析及实现基线：

| 分支 | 基线 | 修复分支 |
| --- | --- | --- |
| master | `7648dd9b1f` | `fix/column-split-recovery-master` |
| 3.0 | `ae6fb0128a` | `fix/column-split-recovery-3.0` |

V3 在 etcd 中省略 FieldBinlog 数组及其独立 KV，但仍持久化完整
`SegmentInfo.Stats` 和精确版本的 `ManifestPath`。因此，Coord 重启不会
丢失 Stats 中原有的逻辑 GroupID、字段集合；恢复代码未消费这些信息，
才会让运行时 currentSplit 为空，并按当前配置和新批次统计重新分组。

仅重启 Coord 不会清除现存 StreamingNode 的内存；后续 StreamingNode
重启、channel 重新恢复或旧 flusher 迁移时会暴露问题。

## 元数据定义与来源

定义见 `pkg/proto/data_coord.proto`：

```protobuf
message ColumnGroupStatistics {
  int64 group_id = 1;
  repeated int64 field_ids = 2;
  int64 memory_size = 3;
}
message LoadResourceStatistics {
  repeated ColumnGroupStatistics column_groups = 1;
}
```

访问方式：

```go
groups := info.GetStats().GetLoadResource().GetColumnGroups()
```

生成及持久化链路：

```text
ColumnGroup.GroupID / Fields
  -> writer 的 FieldBinlog.FieldID / ChildFields
  -> StatisticsCollector.Digest / mergeColumnGroup / Publish
  -> SaveBinlogPathsRequest.Stats
  -> DataCoord.UpdateSegmentStats（整体保存累计统计）
  -> catalog 序列化 SegmentInfo，保留 Stats
  -> Coord 重启加载 -> GetSegmentInfo 返回 Stats
```

`Publish` 对 FieldIds 排序，故它提供字段集合，不提供文件中列的物理顺序。
Stats.Formats 只是格式的去重集合，不能表达每组对应的 Format。
正常成功 sync 路径没有证据表明 Stats 的逻辑编号与字段集合会自行不一致。

## manifest 与文件编号

manifest 提供有序 column groups、组内字段顺序和每组物理格式。

3.0 固定依赖 milvus-storage `f38765d`。实际数据文件名形如
`_data/{group_id}_{uuid}.{format}`，但编号是本次 C++ writer 内的序号：
Go 的 SchemaBasedPattern 只传字段分组；C++ 用循环下标 i 创建 writer。
因此，Go GroupID=101 的向量组，其真实文件名前缀可以是 1。
Go record writer 中包含逻辑 GroupID 的路径被代码标为 mock path，
不能将它当作 manifest 中的实际文件路径。

本次修复不解析文件名前缀、不改变编号语义，也不修改 manifest schema。
逻辑 GroupID 来自 Stats；物理顺序和格式来自对应版本 manifest。

## 3.0 的故障入口与实现

```text
WALFlusher
  -> GetChannelRecoveryInfo：segment IDs / seek position
  -> NewStreamingNodeDataSyncService
  -> Broker.GetSegmentInfo(IncludeUnHealthy=true)
  -> initMetaCache
  -> metacache.NewSegmentInfo
  -> SyncTask / GrowingSourceSyncTask
```

原 NewSegmentInfo 只从 Binlogs 重建 currentSplit。V3 Coord 重载后，
Binlogs 可以为空，但 Stats.LoadResource.ColumnGroups 仍可用。

修复：

1. NewSegmentInfo 在缺少 Binlogs 时，从 LoadResource 恢复逻辑编号和
   字段集合，复制 Fields 切片，避免后续修改污染持久化统计。
2. initMetaCache 在恢复有持久化 insert 行的非 L0、未完成 V3 segment
   时读取 manifest，补齐完整布局；成功等待所有恢复读取后，将布局安装到
   MetaCache.currentSplit。V1/V2 原路径和 L0 删除段不增加 manifest 读取。
3. 新增 storagecommon.RestoreColumnGroupLayout：按完整字段集合匹配
   Stats 与 manifest，保留唯一有效的原 GroupID、manifest 的分组顺序、
   组内顺序和 Format。相同字段集合的历史 GroupID 别名按确定规则归并；拒绝重复编号、
   字段集合部分重叠、缺失/不匹配的分组及缺失格式。
4. packed.RestoreColumnGroupsFromManifest 负责读取精确 manifest revision
   并解析数值字段名。读取失败直接返回，不降级为重新 split。

恢复阶段不伪造 FieldBinlog 文件列表，不修改输入 Stats，也不增加新 RPC
字段。归一化后的统计随下一次普通 sync 发布；列下标仍由该次 schema 重建。

## master 的不同入口与实现

master 已使用 SegmentView，其正常快照保留完整 FieldBinlog 和 Format。
该路径继续优先复用已有布局，不增加正常快照恢复的 manifest 读取。
已有分组但缺失 Format 的快照仍沿用原有拒绝写入逻辑，本次只为
无法从快照恢复分组的情况补充 Stats + manifest 路径。

旧路径迁移时：

```text
GetSegmentInfo
  -> legacyPersistedStorage
       保存 Statistics / ManifestPath，但 Binlogs 可能为空
  -> currentSplitForGrowingPack
```

仅修复 NewSegmentInfo 不足以覆盖 master，因为其 writer 在创建临时
MetaCache 之前，已独立选择 split。

master 额外修改 currentSplitForGrowingPack：已有完整快照布局时沿用；
缺少布局且已有提交的 V3 manifest 时，以同一共享 helper 从 Statistics
及 manifest 恢复，再用选定 schema 检查、重建 Columns。新段或没有
insert 布局/统计的空 manifest 仍可首次计算 split。

## 失败处理与自查

- 元数据损坏、布局不匹配：ErrDataIntegrity，恢复失败，不重新采样分组。
- master 显式永久的原生读取失败：保留 ErrLoonPermanent，在恢复调用处
  标记 Unrecoverable。3.0 只有 ErrLoonTransient，沿用该分支现有分类；
  不引用 master 的 marker，也不声称 3.0 能识别原生永久错误。
- 原生瞬时读取失败：保留 ErrLoonTransient 及 cause；master scheduler
  和旧 pipeline 的重试可以继续处理。取消在读前、读后检查。
- packed 层只对没有 wire code 的原生错误 marker 添加 Storage 错误码，
  原有 typed error 只增加 merr.Wrap 上下文。
- 旧版本若未保存可匹配的 LoadResource，恢复将明确失败；本补丁不猜测
  原逻辑编号，也不提供缺失历史元数据的修复迁移。
- 本次没有调整 FFI 上游的分类规则；真实远端 S3 限流/网络故障没有
  在部署环境注入。marker 的保留和调用端重试分支已做故障注入测试。

## 验证结果

所有 Go 测试均使用 `-tags dynamic,test -gcflags="all=-N -l" -count=1`。

### master：通过

- `internal/storagecommon`：新增恢复匹配、顺序、编号、切片所有权、重复及
  不匹配元数据测试；同时执行该包原有列分组策略测试。
- `internal/storagev2/packed`：真实 writer 生成混合 parquet/vortex manifest；
  元数据无 Binlogs 序列化重载后，恢复非文件序号的逻辑编号；修改当前
  writer 默认格式仍保留原物理格式。
- 原生读取真实缺失/损坏的 manifest，均返回永久失败 marker；注入瞬时、
  永久读取错误，验证 marker 和 Storage cause 保留；取消检查通过。
- `internal/flushcommon/metacache`：完整包测试通过。
- `internal/flushcommon/pipeline`：真实 SyncTask 写入 -> 捕获实际发布的
  Stats/ManifestPath -> 按 V3 catalog 省略 Binlogs 的形状序列化重载 ->
  initMetaCache 恢复 -> 修改当前默认格式 -> 第二次真实 SyncTask 续写。
  原 GroupID=7、字段集合、vortex 格式、累计行数与累计分组统计均保留。
- `internal/streamingnode/server/wal/vchannel/segment`：真实首批写入后，以
  迁移后仅保留 Statistics/ManifestPath 的形状恢复，修改当前 split 配置
  和默认格式后续写成功；累计分组统计继续增加。原完整快照、schema
  检查测试及读取错误的 retry/terminal 分类回归通过。

执行的定向包测试包括：

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./internal/storagecommon ./internal/storagev2/packed \
  ./internal/flushcommon/metacache ./internal/flushcommon/pipeline \
  ./internal/streamingnode/server/wal/vchannel/segment \
  -run 'TestRestoreColumn|TestSegment|TestInitMetaCache|TestLogicalSplit|TestV3GrowingPackResumes|TestV3ColumnGroupRecovery|TestCurrentSplit|TestFlushInsertBufferRejects|TestGrowingColumnGroups|TestGetChannelWithTickler'
```

本机另设置 PKG_CONFIG_PATH，并通过 CGO_LDFLAGS 为现有 master 原生库
添加 macOS rpath。上述验证使用现有本地原生库，没有重新构建全部 C++。

### 3.0：纯 Go 通过，原生集成受环境阻挡

`go test ... ./internal/storagecommon` 全包通过，包含该分支新增恢复测试。

新增 pipeline / packed / MetaCache 回归代码已落在 3.0 分支；共同代码
也在 master 的真实恢复与续写测试中执行通过。但本机当前只安装 master
原生库，3.0 编译在原有依赖处失败，尚未执行其原生集成测试：

```text
C.LoonFilesystemMetricsList / loon_filesystem_get_metrics /
loon_filesystem_list_metrics / loon_filesystem_free_metrics_list 未定义
C.SetArrowIOThreadPoolCapacity 未定义
create_token_stream 参数及返回值签名不匹配
```

现有原生库只有 master 新版 metrics / transaction_open 接口；3.0 要求
旧版 metrics / transaction_begin 及相应 core 接口。没有通过修改产品
代码或添加假实现来绕开此 ABI 差异。需构建 3.0 匹配的 core 和固定的
milvus-storage 后重跑原生测试。

两条分支均通过 gofmt 和 git diff --check。未执行真实集群进程的 Coord /
StreamingNode 重启，也未执行整仓 make test-go；不能将以上回归表述为
完整的线上重启或版本升级验证。


## 2026-10-23：3 组变 4 组的定向复现

新增 `TestDefaultPoliciesCanDriftAcrossBatches`（纯 Go）与
`TestV3ColumnSplitDriftAfterRecovery`（真实 native 写入/提交/读回）。
schema 和配置始终相同：系统列 0、1，主键 100，VarChar 101，Int64 102，
二维 FloatVector 103；开启 system-column / average-size split，阈值 64。
首批字符串为 `short`，第二批长度 1024。两个批次的采样平均大小跨过阈值。

| 阶段 | GroupID → fields |
| --- | --- |
| 首批，3 组 | 0 → [0,1,100]；1 → [101,102]；103 → [103] |
| 丢失 split 后重新采样，4 组 | 0 → [0,1,100]；1 → [102]；101 → [101]；103 → [103] |
| 正确恢复后续写，仍为 3 组 | 0 → [0,1,100]；1 → [101,102]；103 → [103] |

原生回归执行以下流程：

1. 真正的 SyncTask 首次按 DefaultPolicies 写入并提交，不预设分组；捕获
   SaveBinlogPaths 的实际 Stats / ManifestPath / 行数，省略 Binlogs 后做
   protobuf 序列化、反序列化，模拟 V3 catalog 重载。
2. 调用实际 initMetaCache 恢复。负对照主动清空恢复布局，模拟旧恢复结果，
   再写入长字符串，真实提交返回：
   `Column group size mismatch: existing has 3 groups, but appended has 4 groups`。
   拒绝后不发布 SaveBinlogPaths，读回已提交 manifest 仍只有原始首行。
   这是丢失 split 的对照实验，未启动旧二进制或进行真实进程重启。
3. 正对照保留恢复布局，写入相同长字符串成功；manifest 从版本 1 变为 2，
   原 GroupID、字段集合、parquet 格式保留，各组累计 MemorySize 增加。
   通过 native reader 读回主键 [10,20] 和两批字符串内容，验证实际文件数据。
4. 测试限制为最多 2 次提交尝试，避免复现时无限重试。3.0 用例断言该错误
   为 ErrLoonTransient；master 断言 ErrLoonPermanent，分别遵守分支语义。

### 对日志中 retried=392 的源码追踪

3.0 固定的 milvus-storage f38765d 在
`cpp/src/transaction/transaction.cpp` 的追加分组检查中构造
`arrow::Status::Invalid`。该错误沿 CommitManifestUpdates →
HandleLoonFFIResult 进入 Go；3.0 后者把所有失败统一包装为
ErrLoonTransient，BulkPackWriterV3 的 classifyLoonErr 继续允许重试。
生产 write_buffer 创建 SyncTask 时设置了 AttemptAlways，因此不能将
classifyLoonErr 注释中的“bounded retry”当作线上有界重试的证据。
已生成的 4 组文件不会因重复提交自动变成 3 组。

3.0 全仓实际 SplitColumns 调用也已核对：SyncTask 与 GrowingSourceSyncTask
共用 resolveColumnGroups；其余 PackedRecordWriter / ManifestRecordWriter /
TextManifestRecordWriter 仅在传入布局为空时首次计算。修复恢复入口提供原布局，
并从 manifest 补全 Format，让恢复后的两条 sync 路径不再丢失原分组。
本次回归没有覆盖 QueryNode growing-source 的真实 RPC，也没有调整 FFI 分类。

### 本次执行结果与边界

- 3.0：完整 `internal/storagecommon` 通过，包含实际 DefaultPolicies 的
  3→4 条件和恢复匹配用例；移除了此前误引入的 master 专有
  ErrLoonPermanent，读取错误测试改为遵守 3.0 的 transient 语义。
- master：storagecommon、packed、metacache、pipeline 的定向恢复回归通过，
  包含上述 native 错误负对照、正确恢复续写和数据读回。
- 3.0：同一原生回归已添加，但本机现有 master 原生依赖在 metrics、
  SetArrowIOThreadPoolCapacity、tokenizer 的 C ABI 上均与 3.0 不匹配，
  因而仍未运行 3.0 原生集成。没有用 master 的通过替代 3.0 的通过。
- 需要在匹配 3.0 core 和 f38765d 的环境执行以下命令，才能关闭该验证缺口：

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./internal/storagecommon ./internal/storagev2/packed \
  ./internal/flushcommon/metacache ./internal/flushcommon/pipeline \
  -run 'TestV3ColumnSplitDriftAfterRecovery|TestDefaultPoliciesCanDriftAcrossBatches|TestRestoreColumn|TestInitMetaCache|TestLogicalSplit|TestSegment'
```

本修复作用于恢复时重新建立的 cache；已经生成错误 split、陷入重试的现存
SyncTask 需要用修复版本重新建立恢复流程，不能靠仅更换 Coord 内存来修改
该 task 持有的列组。相同字段集合的历史别名由下述兼容逻辑处理；其他不能
完整匹配的 Stats、损坏 manifest 仍明确失败，不猜测缺失信息。


## 历史 GroupID 别名兼容

旧版本可能为相同物理布局先后生成 G1{101}、G101{101}；原生 append
接受相同位置的字段集合和格式，累计 Stats 因而同时保留两个逻辑条目。
这些条目是不同批次的增量贡献，不能因为字段重复就判为物理数据损坏。

恢复按完整字段集合建立索引，只归并集合完全相同的别名，MemorySize
相加。Stats-only 恢复选择最小候选 ID，正常唯一 ID 不变；已有完整快照
时以实际选用布局的 ID 为准。manifest 的顺序、字段顺序和 Format 保留。
部分重叠、重复逻辑编号、负值及合并溢出仍报 DataIntegrity。

NormalizeColumnGroupStatistics 返回独立 proto，保留 LoadResource 以外的
累计值不变。旧 pipeline 同时安装恢复布局与归一化后的累计 collector；
master 的完整快照和 Stats-only 路径也在写入前对齐 collector 的编号。
下一次正常 sync 发布完整 Stats，使别名不再进入后续持久化统计。

新增回归覆盖：输入统计顺序无关、原 proto 不变、字段集合归并、格式与物理
顺序保留、二次恢复幂等、完整快照编号优先、合并溢出和部分重叠拒绝。
原生 pipeline 用例还注入一次元数据发布失败，验证重试发布相同累计值，
失败尝试不推进 cache；成功续写后再恢复，编号和字节计数保持一致。

临时 overlay 以真实 SyncTask 制造 G1→G101 历史别名，再用修复版恢复、
续写第三批并读回三批内容，最后再次恢复成功。master 的相关恢复测试和
新增改动的完整 golangci-lint 通过。3.0/hotfix 的 storagecommon 完整包、
格式和 depguard 通过；匹配 3.0 原生依赖的集成测试仍受 ABI 环境限制。
这些结果不等同于真实集群版本升级或进程重启验证。
