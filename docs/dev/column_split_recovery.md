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
   Stats 与 manifest，保留原 GroupID、manifest 的分组顺序、组内顺序和
   Format。拒绝重复编号、重复字段、缺失/不匹配的分组及缺失格式。
4. packed.RestoreColumnGroupsFromManifest 负责读取精确 manifest revision
   并解析数值字段名。读取失败直接返回，不降级为重新 split。

恢复阶段不伪造 FieldBinlog 文件列表，不修改持久化 Stats，不需要新 RPC
字段，也不需要重新把布局写入 etcd。下一次 sync 按其 schema 重建列下标。

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
- 显式永久的原生读取失败：保留 ErrLoonPermanent，在恢复调用处标记
  Unrecoverable。
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
```

现有原生库只有 master 新版 metrics / transaction_open 接口；3.0 要求
旧版 metrics / transaction_begin 及相应 core 接口。没有通过修改产品
代码或添加假实现来绕开此 ABI 差异。需构建 3.0 匹配的 core 和固定的
milvus-storage 后重跑原生测试。

两条分支均通过 gofmt 和 git diff --check。未执行真实集群进程的 Coord /
StreamingNode 重启，也未执行整仓 make test-go；不能将以上回归表述为
完整的线上重启或版本升级验证。
