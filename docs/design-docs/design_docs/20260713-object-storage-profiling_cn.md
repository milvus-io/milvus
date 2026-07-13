# MEP：对象存储指标与按需启用的请求/任务存储剖析

- **创建日期：** 2026-07-13
- **作者：** 待定
- **状态：** 草案
- **组件：** Proxy | QueryNode | DataNode | DataCoord | StreamingNode | Segcore | Storage | Metrics
- **相关 Issue：** 待定
- **发布日期：** 待定

## 摘要

本 MEP 为 Milvus 引入一个统一的对象存储可观测性框架，包含两项互补能力：

1. 始终启用、低基数的 Prometheus 指标，用于观测对象存储操作、延迟分布、字节数、重试、失败、工作负载归因和相关缓存行为。
2. 显式启用的 `storage_profile`，为单个请求或后台任务收集固定大小、可合并的摘要，包括操作数、字节数、平均/最小/最大延迟、近似 p50/p95/p99、缓存用量、工作负载阶段和存储角色。

初始实现在 Milvus 仓库拥有的逻辑存储操作边界进行 instrumentation，不修改 milvus-storage。在这一阶段，设计假定一个被观测到的逻辑存储操作足以代表一个底层对象存储操作。该模型不要求在面向用户的命名中严格区分逻辑操作和 provider 操作。

尽管如此，schema 和接口仍为未来的 provider 访问层预留了扩展能力，以便在 milvus-storage 内部进行更深层的 instrumentation，包括 provider 重试、内部 range request、首字节时间和 provider 传输字节数。

初始实现不会：

- 将 profile 发布到 ClickHouse、Kafka 或其他外部分析后端；
- 将 profile 数据或指标添加到 Proxy access log；
- 暴露公开的请求响应字段、gRPC trailer 或管理员 UI；
- 将请求 ID、任务 ID、用户 ID、Collection ID 或对象路径放入 Prometheus 标签。

带版本的 sink、展示和 access-log 扩展接口将被预留，使未来添加这些能力时无需更改核心采集模型。

## 1. 动机

Milvus 在许多彼此独立且高度并发的执行路径中使用对象存储：

- QueryNode 加载 sealed 数据、索引、manifest、统计信息和 LOB 数据。
- Search 和 Query 可能触发分层存储缓存单元的冷加载。
- StreamingNode 和 DataNode 将 growing 数据 flush 到 binlog、统计信息、manifest 和 delete log。
- Import 读取用户提供的源文件，并写入 Milvus 所有的持久化 segment 数据。
- Compaction 读取源 segment，并写入替换后的 segment。
- Index、Analyze 和 Stats 作业读取源数据并写入派生产物。
- Load 和 warmup 填充本地内存或磁盘缓存。
- Recovery 从持久化 checkpoint、manifest 和 segment 数据重建状态。
- Snapshot、restore、copy-segment、external refresh 和垃圾回收会发出 read、write、stat、list、copy 和 delete 操作。

现有的存储可观测性虽有价值，但较为分散：

- Go `RemoteChunkManager` 暴露聚合的操作、延迟和大小指标。
- milvus-storage 暴露文件系统全局累计计数和字节数。
- 分层存储缓存暴露缓存单元命中和未命中字节数。
- `OpContext.storage_usage` 可以暴露总扫描字节数和冷扫描字节数。
- QueryNode 记录部分 segment 访问、磁盘缓存加载和缓存等待指标。

现有信号无法一致地回答：

- 哪个请求或任务导致了一组对象存储操作？
- 按工作负载划分的存储延迟 avg、p50、p95 和 p99 分别是多少？
- Import 从源存储读取了多少数据，又向 Milvus 持久化存储写入了多少数据？
- Flush、Compaction、Index、Load、Recovery、Snapshot 或 GC 分别产生了多少存储工作？
- Search 是否因分层存储缓存未命中而等待？
- 重试和 throttling 是否造成了延迟长尾？
- 分布式 Search 能否合并来自多个 QueryNode 的存储用量？
- 报告的零值是真正的零，还是该路径尚未被 instrument？

Prometheus 无法安全存储逐请求或逐任务明细，因为 ID 是无界标签。反过来，请求/任务摘要也无法取代始终启用的聚合 Dashboard 和告警。Milvus 需要从同一套语义 instrumentation 模型中同时获得这两类输出。

## 2. 目标

1. 在 Go 和 Milvus 自有 C++ 代码中定义统一且稳定的操作、字节、延迟、重试、结果、工作负载、阶段、存储角色、缓存和覆盖范围模型。
2. 添加详细的 Prometheus Histogram 和 Counter，在不使用高基数标签的情况下支持 avg/p50/p95/p99 查询。
3. 为单个请求或任务添加按需启用的 `storage_profile`。
4. 使 profile 分布可以跨节点和任务 attempt 合并。
5. 覆盖 Go `ChunkManager` 操作，以及围绕 Storage V2/V3、segcore、load、compaction 和 index 工作流的 Milvus 自有 C++ 调用边界。
6. 初始实现中避免修改 milvus-storage。
7. 在初始产品语义中，将观测到的逻辑操作视为具有代表性的对象存储操作。
8. 为未来的 provider 访问扩展预留能力，而不在当前引入其复杂性。
9. 保持 Import 与 Flush 相互独立，并将 Import 源读取与持久化写入分开。
10. 覆盖直接相关的分层存储缓存用量和缓存等待信号，同时不重新设计缓存架构。
11. 保持逐作用域 profiling 显式启用，并限制其资源使用。
12. 为管理员 TTL 规则、调用者展示、任务状态展示、外部 sink 和 access-log 集成预留扩展能力。
13. 在滚动升级和部分 instrumentation 的情况下保持安全。

## 3. 非目标

1. 在初始实现中修改 milvus-storage。
2. 精确的云 provider 计费或原始 HTTP 请求核算。
3. 观测完全发生在 provider SDK 或 milvus-storage 内部的重试。
4. 当一个 Milvus 操作在内部展开时，精确统计 provider 级 GET/PUT/RangeGET 数量。
5. 初始阶段将摘要发布到 ClickHouse、Kafka、Pulsar 或 OTLP。
6. 初始阶段向 Proxy access log 添加字段或指标。
7. 初始阶段提供最终的公开 SDK API、admin API 或 WebUI。
8. 将请求、任务、trace、用户、collection、segment、bucket、endpoint 或对象路径作为标签添加到 Prometheus。
9. 使用统一缓存框架替换现有缓存实现。
10. 将 OS page cache 或 mmap page fault 归因到某个请求。
11. 将异步 Flush、Compaction 或 Index 工作的成本分摊给某个 Insert 请求。
12. 为了指标而更改缓存准入、淘汰、存储格式、重试、调度或 I/O 行为。

## 4. 核心设计决策

初始实现采用以下决策：

- 功能名称使用 `storage_profile`；避免使用 `analyze`，因为它会与现有 Analyze 任务和查询计划术语冲突。
- 聚合 Prometheus 指标始终启用，且独立于逐作用域 profiling。
- 请求/任务 profile 明细默认关闭，需要显式启用。
- 仅在 Milvus 仓库拥有的边界添加 instrumentation。
- milvus-storage 保持不变，并被视为不透明的实现依赖。
- 在初始 Dashboard 和 profile 中，将一个观测到的逻辑操作视为一个具有代表性的对象存储操作。
- 指标名称可以使用 `storage_operation`，无需处处暴露严格的 `logical` 限定词。
- 内部数据结构保留 access-layer/version 字段，以便未来添加 provider 级数据。
- Import 创建的 SyncTask 继承 Import 归因，不计为 Flush。
- Import 的源存储角色和持久化存储角色相互独立。
- `workload_kind` 是 Prometheus 标签；`workload_subtype` 和 `phase` 初始阶段仅保留在 profile 中。
- Proxy access log 保持不变；预留未来的 access-log adapter。
- 初始生产环境 `SummarySink` 为 noop。

## 5. 术语

| 术语 | 含义 |
|---|---|
| 存储操作 | 一次 Milvus 观测到的逻辑 read/write/stat/list/delete/copy 操作。初始阶段假定它能代表一个对象存储操作。 |
| Provider 操作 | 未来发生在 milvus-storage 或云 SDK 内的更低层调用。初始阶段不采集。 |
| 作用域 | 一个被显式 profile 的请求或任务。 |
| Contribution | 来自一个节点/组件的可合并 profile 片段。 |
| 工作负载类型 | 稳定的请求/任务族，例如 `search`、`import` 或 `compaction`。 |
| 工作负载子类型 | 更具体的执行类型，例如 `l0_ingest` 或 `clustering`。 |
| 阶段 | 有界的工作负载步骤，例如 `read_source`、`write_output` 或 `warmup`。 |
| 存储角色 | 存储的语义用途，例如 `source` 或 `persistent`。 |
| 后端类型 | 有界的实现族：`s3_compatible`、`azure`、`gcp`、`local` 或 `unknown`。 |
| 冷字节 | 需要加载、而不是由缓存立即提供的缓存单元字节。 |
| 累计延迟 | 操作延迟之和；不一定等于请求关键路径延迟。 |
| 缓存等待 | 因等待缓存加载/填充而被阻塞的时间。 |

## 6. 测量模型

### 6.1 访问层

序列化模型预留一个访问层：

```go
type AccessLayer int32

const (
    AccessLayerMilvus   AccessLayer = 1
    AccessLayerProvider AccessLayer = 2 // reserved, not produced initially
)
```

初始实现只生成 `AccessLayerMilvus`。`AccessLayerProvider` 预留给未来的 milvus-storage instrumentation。

尽管内部模型保留了这一区分，初始 Dashboard 和展示层仍可以将观测结果统称为“存储操作”。除非某条具体路径另有说明，否则实现假定具有代表性的 1:1 行为。

### 6.2 操作分类

```go
type StorageOperation int32

const (
    StorageOperationUnknown StorageOperation = iota
    StorageOperationRead
    StorageOperationRangeRead
    StorageOperationWrite
    StorageOperationStat
    StorageOperationList
    StorageOperationDelete
    StorageOperationCopy
    StorageOperationMultipartCreate
    StorageOperationMultipartWrite
    StorageOperationMultipartComplete
    StorageOperationMultipartAbort
    StorageOperationCount
)
```

初始映射：

| Milvus 边界 | 操作 |
|---|---|
| `ChunkManager.Read` / `Reader` | `read` |
| `ChunkManager.ReadAt` | `range_read` |
| `ChunkManager.Write` | `write` |
| `ChunkManager.Size` / `Exist` | `stat` |
| `ChunkManager.WalkWithPrefix` | `list` |
| `ChunkManager.Remove` | `delete` |
| `ChunkManager.Copy` | `copy` |
| Milvus C++ packed reader 调用 | 根据调用者意图映射为 `read` 或 `range_read` |
| Milvus C++ writer/flush 调用 | `write` |
| Milvus C++ metadata/file-info 调用 | `stat` |

该操作是一项语义观测，不代表对内部 provider RPC 精确数量的声明。

### 6.3 批量和嵌套调用

指标统计对象级语义操作，而不是额外统计外层批量操作。

- `MultiRead`、`MultiWrite` 和 `MultiRemove` 不得在逐对象操作之外再增加一个批量计数。
- 当前将批量操作展开为一元方法的实现，应当 instrument 这些一元操作。
- `Path -> Exist -> Stat` 和类似的嵌套调用需要使用抑制/嵌套 token 以避免重复统计。
- 当 wrapper 只能观测批量操作的总时长时，不得虚构逐对象延迟。
- 未来真正的 provider 批量 API 可以添加独立的 provider 层批量概念，而无需更改初始逻辑语义。

### 6.4 结果和错误类别

结果是有界集合：

```text
success
failure
canceled
timeout
```

错误类别是有界集合：

```text
none
not_found
throttled
permission_denied
invalid_credentials
bucket_not_found
invalid_argument
invalid_range
entity_too_large
unexpected_eof
timeout
canceled
io_failed
unknown
```

在 provider 错误已于现有源头完成映射后，Go 分类使用带类型的 `merr` 错误，不得使用错误消息字符串匹配。指标分类绝不能改变返回的错误、可重试性或 Input-vs-System 分类。

C++ 分类使用 Milvus 自有调用边界上可用的带类型 Milvus/segcore status。如果只能看到通用错误，profile 应记录 `io_failed` 或 `unknown`，而不是猜测内部 provider 原因。

### 6.5 字节语义

```go
type ByteStats struct {
    Requested uint64
    Completed uint64
}
```

- `Requested` 是在观测边界请求或计划处理的字节数。
- `Completed` 是成功读取、写入或复制的字节数。

对于 C++ 操作，字节数可以来自现有 buffer 大小、文件元数据、manifest 元数据、packed-reader 核算、输出产物大小或 `OpContext.storage_usage`。Instrumentation 不得仅为了获取指标而额外发起 Stat/HEAD。

如果无法确定字节数，操作计数和延迟仍然有效，同时将字节覆盖状态标记为不可用。

Provider 传输字节数预留给未来使用，不得从逻辑字节数推断。

### 6.6 延迟语义

初始设计记录：

| 指标 | 定义 |
|---|---|
| 操作时长 | Milvus 观测到的存储调用的端到端时长，包括可见的重试和等待。 |
| 首字节时间 | 对于可观测的 Milvus 自有流式 reader，从开始到第一次成功的非零读取所需的时间。 |
| 传输时长 | 从开始到 EOF、成功、失败、取消或关闭所需的时间。 |
| 缓存查询时长 | 在已有可观测能力的情况下，判断相关缓存命中/未命中所花费的时间。 |
| 缓存填充时长 | 在已有可观测能力的情况下，填充相关缓存所花费的时间。 |
| 缓存等待时长 | 调用者因等待缓存数据而被阻塞的时间。 |

对于并行操作，累计存储时长可能超过请求 wall time。Profile 字段和未来展示必须区分：

- 操作数；
- 累计操作延迟；
- 最大操作延迟；
- 请求/任务 wall time；
- 缓存等待时间。

初始设计不声称能够精确计算存储对关键路径的贡献。

### 6.7 后端类型

```text
unknown
s3_compatible
azure
gcp
local
```

该 enum 可在不暴露 endpoint、bucket、凭证或对象名称的情况下区分远程行为与 standalone/本地行为。

## 7. 可合并分布契约

### 7.1 动机

一个分布式 Search 可以在多个 QueryNode 上运行。节点 p99 值不能取平均。每个 contribution 因此携带可合并的 bucket 计数、sum、count、min 和 max。Proxy 或任务 coordinator 在计算分位数之前先合并这些分布。

```go
const LatencyBucketSchemaV1 uint32 = 1

type LatencyDistribution struct {
    Count    uint64
    SumNanos uint64
    MinNanos uint64
    MaxNanos uint64
    Buckets  [LatencyBucketCount]uint64
}
```

内部 bucket 计数是非累计的。导出到 Prometheus 时，将其转换为累计的 classic Histogram bucket。

### 7.2 延迟 Bucket

单位为秒：

```text
0.00025
0.0005
0.001
0.002
0.005
0.010
0.025
0.050
0.100
0.250
0.500
1
2.5
5
10
30
60
120
300
+Inf
```

### 7.3 大小 Bucket

```text
1 KiB
4 KiB
16 KiB
64 KiB
256 KiB
1 MiB
4 MiB
16 MiB
64 MiB
256 MiB
1 GiB
4 GiB
+Inf
```

### 7.4 分位数语义

- `avg = sum/count`。
- `min` 和 `max` 在观测边界上是精确值。
- p50/p95/p99 是 bucket 分辨率内的近似值。
- 对于非常小的样本集，必须在分位数之外同时展示 count 和 max。
- 空分布产生不可用的分位数，而不是零值。

### 7.5 Schema 兼容性

每个序列化 profile 都包含 `schema_version` 和 `bucket_schema`。

接收方：

1. 合并匹配的已知 bucket schema。
2. 在兼容时合并标量 count/sum/bytes。
3. 如果任何必需的 contribution 使用未知 schema，则将分位数标记为不完整。
4. 绝不能重新解释未知的 bucket 边界。

## 8. 归因模型

```go
type Attribution struct {
    ScopeType ScopeType

    TenantID string
    UserID   string

    RequestID   string
    RequestType string
    TraceID     string

    TaskID      string
    TaskAttempt uint32

    Component    string
    NodeID       int64
    CollectionID int64

    WorkloadClass   WorkloadClass
    WorkloadKind    WorkloadKind
    WorkloadSubtype WorkloadSubtype
    Phase            WorkloadPhase
    StorageRole      StorageRole
    BackendKind      BackendKind
}
```

标识符仅作为 profile 字段。Prometheus 只接收有界 enum 维度。

### 8.1 Profile 禁用时的聚合归因

始终启用的指标仍需要工作负载归因。因此：

- 为普通请求/任务传播轻量的有界归因；
- 只有在启用 profiling 时才分配请求/任务 ID 和逐作用域分布；
- 禁用 profile 的路径使用 noop recorder，但保留 component/workload/storage-role/backend 维度；
- 即使 `storage.profile.enabled=false`，任务构造器也必须设置工作负载归因。

### 8.2 工作负载类别

```text
request_path
background
recovery
system
```

### 8.3 工作负载类型

请求类型：

```text
search
query
insert
delete
upsert
```

任务类型：

```text
flush
import
compaction
index
load
recovery
snapshot
gc
external_sync
replication   # reserved, not initially selected
```

### 8.4 工作负载子类型

Profile 中保留的示例：

- Flush：`auto`、`manual`、`streaming`。
- Import：`preimport`、`ingest`、`l0_preimport`、`l0_ingest`、`copy_segment`。
- Compaction：`mix`、`level0_delete`、`clustering`、`sort`、`partition_key_sort`、`bump_schema_version`、`single`、`minor`、`major`。
- Index：`build`、`analyze`、`stats_text`、`stats_bm25`、`stats_json_key`。
- Load：`segment_load`、`index_load`、`sync_warmup`、`async_warmup`、`cache_fill`。
- Snapshot：`create`、`restore`、`pin`、`unpin`、`copy`。

### 8.5 阶段

```text
read_source
read_metadata
write_output
write_metadata
copy_object
warmup
cache_lookup
cache_fill
cleanup
```

初始阶段中，`phase` 仅保留在 profile 中，以避免 Prometheus 时间序列成倍增加。

### 8.6 存储角色

```text
unknown
source
persistent
```

Import 和外部源工作流必须区分源存储与持久化存储。

## 9. Import 归因

即使 Import 复用了 `SyncTask`、SyncManager、pack writer 或相同的存储实现，它也不是 Flush。

### 9.1 PreImport

```text
workload_kind    = import
workload_subtype = preimport
phase            = read_source
storage_role     = source
```

PreImport 可以扫描大量源数据，而不产生持久化 segment 输出。

### 9.2 Ingest

源阶段：

```text
workload_kind    = import
workload_subtype = ingest
phase            = read_source
storage_role     = source
```

输出阶段：

```text
workload_kind    = import
workload_subtype = ingest
phase            = write_output
storage_role     = persistent
```

由 Import 创建的 SyncTask 必须继承父级归因。可复用的写入代码不得将 `workload_kind` 重置为 `flush`。

### 9.3 L0 Import

`l0_preimport` 和 `l0_ingest` 对 delete 数据使用相同的源/输出角色划分。

### 9.4 Copy Segment

```text
workload_kind    = import
workload_subtype = copy_segment
phase            = copy_object
storage_role     = persistent
operation        = copy
```

复制字节数在可用时使用现有元数据。不会为了指标而额外发起 Stat/HEAD。

### 9.5 重复统计规则

一次物理执行只更新一个活动的作用域 recorder。嵌套的可复用组件不会创建相互竞争的顶层作用域。因此，Import 输出只显示一次，即显示为 Import，而不是一次 Import 加一次 Flush。

## 10. 缓存覆盖范围

### 10.1 包含的信号

初始实现使用现有的 Milvus 可见缓存信号：

- `OpContext.storage_usage.scanned_total_bytes`；
- `OpContext.storage_usage.scanned_cold_bytes`；
- 现有分层缓存聚合命中/未命中字节数；
- QueryNode segment 缓存未命中指示；
- 缓存加载等待时长；
- 已暴露时的磁盘缓存加载时长和字节数；
- 同步和异步 warmup 工作负载归因。

### 10.2 分层存储用量跟踪

只有在现有分层存储用量跟踪路径提供数据时，逐请求的总扫描字节数/冷扫描字节数才可用。初始增强不会仅为了强制采集而修改外部缓存层。

如果跟踪被禁用或某条路径未暴露用量：

- 缓存覆盖状态为 `unavailable`；
- 不会将值报告为零；
- 已存在的聚合缓存指标仍然可用。

### 10.3 缓存比例

数据可用时：

```text
cached_bytes = scanned_total_bytes - scanned_cold_bytes
byte_hit_ratio = cached_bytes / scanned_total_bytes
```

该比例是缓存单元用量度量，不是精确的 provider 字节节省量。

### 10.4 Warmup

```text
workload_kind  = load
phase          = warmup
storage_role   = persistent
workload_class = background
```

异步 warmup 不继承触发它的用户请求 profile。当任务策略启用 profiling 时，它会创建一个后台任务作用域。

### 10.5 可选的低侵入信号

如果无需更改依赖即可获得，TEXT LOB reader-cache 的命中/未命中计数可以作为辅助元数据纳入。它们不计入主要字节命中率，因为复用 reader 并不能证明对象数据已被缓存。

DiskANN/index 本地可用性可以表示为 Load 子类型，但本地索引读取不计为远程对象操作。

### 10.6 排除的缓存

初始实现排除：

- expression result cache；
- index offset cache；
- index attribute cache；
- geometry cache；
- result/iterator cache；
- 文件系统 client-instance cache；
- OS page cache 和 mmap page fault。

这些缓存优化的是计算、元数据、本地文件或 client 构造，而不是直接暴露远程对象访问。

## 11. Profile 和覆盖范围数据模型

```go
type StorageProfile struct {
    SchemaVersion uint32
    BucketSchema  uint32

    Attribution Attribution

    Operations [StorageOperationCount]OperationStats
    Cache      CacheStats
    Coverage   ProfileCoverage

    StartedAtUnixNano  int64
    FinishedAtUnixNano int64
}

type OperationStats struct {
    Count     uint64
    Success   uint64
    Failed    uint64
    Canceled  uint64
    TimedOut  uint64
    Retried   uint64
    Throttled uint64

    BytesRequested uint64
    BytesCompleted uint64

    Duration LatencyDistribution
    TTFB     LatencyDistribution
    Size     SizeDistribution
}
```

实际实现应在热路径中使用固定数组，而不是 map。

### 11.1 覆盖范围

```go
type CoverageState int32

const (
    CoverageNotApplicable CoverageState = 0
    CoverageInstrumented  CoverageState = 1
    CoveragePartial       CoverageState = 2
    CoverageUnavailable   CoverageState = 3
)

type ProfileCoverage struct {
    GoStorageOperations  CoverageState
    CppStorageOperations CoverageState
    StorageBytes         CoverageState
    StreamingTTFB        CoverageState
    TieredCacheUsage     CoverageState
    CacheWait            CoverageState
    ProviderAccess       CoverageState // unavailable initially
}
```

覆盖状态可以防止不可用的 instrumentation 被显示为零值成功。

## 12. 核心接口

### 12.1 Profile 级别和决策

```go
type StorageProfileLevel int32

const (
    StorageProfileDisabled StorageProfileLevel = 0
    StorageProfileSummary  StorageProfileLevel = 1
    StorageProfileDetailed StorageProfileLevel = 2 // reserved
)

type ProfileDecision struct {
    Requested StorageProfileLevel
    Effective StorageProfileLevel
    Source    ProfileSource
    Reason    ProfileDecisionReason
}

type ProfileDecider interface {
    Decide(ctx context.Context, meta ScopeMeta) ProfileDecision
}
```

`Detailed` 预留给未来的慢操作样本或 provider 数据。

### 12.2 Recorder

```go
type Recorder interface {
    BeginOperation(meta OperationMeta) OperationRecorder
    ObserveCache(event CacheEvent)
    Snapshot() *StorageProfile
}

type OperationRecorder interface {
    AddCompletedBytes(n uint64)
    FirstByte()
    Finish(result OperationResult)
}
```

禁用的作用域使用一个单例 noop recorder。

### 12.3 Context 传播

```go
ctx = storageprofile.WithAttribution(ctx, boundedAttribution)
ctx = storageprofile.WithRecorder(ctx, recorder)
```

后台任务将不可变归因复制到任务元数据，并创建新的 context。它们不得保留已取消的请求 context。

### 12.4 Summary Sink

```go
type SummarySink interface {
    Publish(ctx context.Context, profile *StorageProfile) error
    Close(ctx context.Context) error
}
```

初始生产环境实现：`NoopSummarySink`。

未来实现：Kafka、OTLP、ClickHouse、有界 debug store 或其他分析后端。

Sink 发布不得因外部 I/O 而阻塞请求/任务完成。

### 12.5 展示和 Access-Log 扩展

```go
type ProfilePresenter interface {
    Present(ctx context.Context, profile *StorageProfile) error
}

type AccessLogProfileAdapter interface {
    Fields(profile *StorageProfile) []AccessLogField
}
```

两个接口都仅作预留。初始实现不注册 presenter，也不注册 access-log adapter。Proxy access-log 的格式、字段、大小和性能保持不变。

未来的 access-log 设计必须单独处理：

- 是否只记录被显式 profile 的请求；
- 最大字段大小；
- 是否允许 Histogram 摘要；
- tenant/user 可见性；
- 脱敏和安全；
- 不存在 Proxy access log 的异步后台任务。

## 13. Go Instrumentation

### 13.1 ChunkManager 边界

Instrument 以下方法：

- `Path`
- `Size`
- `Write`
- `MultiWrite`
- `Exist`
- `Read`
- `Reader`
- `MultiRead`
- `WalkWithPrefix`
- `ReadAt`
- `Remove`
- `MultiRemove`
- `RemoveWithPrefix`
- `Copy`

`RootPath` 仅涉及元数据。

Instrumentation 可以通过 decorator 加定点一元方法 hook 来实现。它必须遵循第 6.3 节的批量/嵌套规则。

### 13.2 流式 FileReader

```go
type instrumentedFileReader struct {
    inner FileReader
    operation OperationRecorder

    firstByteOnce sync.Once
    finishOnce    sync.Once
    bytesRead     atomic.Uint64
}
```

必需行为：

- 在创建/打开 reader 时开始计时；
- 在第一次成功的非零 `Read` 或 `ReadAt` 时设置 TTFB；
- 对部分读取，精确累加返回的 `n`；
- 有效读取完成并到达 EOF 时，以成功结束；
- 对非 EOF 错误，以失败结束；
- 区分 timeout/cancellation；
- 在 `Close` 时终止尚未结束的操作；
- 避免在 EOF/Close/error 竞态下重复结束；
- 审计未关闭 reader 的调用方。

### 13.3 重试核算

只统计 Milvus 可见的重试。一个逻辑操作保留一份总时长和重试计数。实现不会尝试暴露 SDK 内部 attempt。

### 13.4 现有指标迁移

在迁移窗口期间，现有 `PersistentData*` 指标继续保持注册。新指标不得静默改变现有 Histogram 的单位或含义。Dashboard 需要显式迁移。

## 14. Milvus 自有 C++ 插桩

### 14.1 边界规则

初始实现只修改本 Milvus 仓库拥有的源代码。它不修改 milvus-storage 源代码、生成的 header、filesystem adapter 或 provider client。

在 Milvus 可见的调用周围插桩，例如：

- segcore Search/Query 的 storage phase；
- Milvus 调用的 Storage V2/V3 packed reader/writer 入口；
- segment 和 index Load 操作；
- compaction reader/writer；
- index/analyze/stats reader 和 output writer；
- Milvus 代码中可见的 manifest/statistics 操作；
- 现有 `OpContext.storage_usage` snapshot。

### 14.2 Scope Recorder

Milvus C++ 可以在 request/task 的 `OpContext` 或 operation 自有结构中维护轻量 recorder。该 recorder 在 Milvus 边界记录 duration/count/known bytes。

不要使用 thread-local 归因，因为工作会在线程池之间移动，并且一个请求会执行并发操作。

### 14.3 Go/C++ 交接

如果现有 Go-to-C++ 调用已经返回 storage usage 字段，则仅在不需要修改 milvus-storage 的前提下扩展现有 Milvus 自有的 result/FFI 结构。否则将 coverage 标记为 partial。

避免从 C++ 到 Go 的 per-operation callback。在 C++ request/task scope 内部完成聚合，并在结束时返回一个 snapshot。

### 14.4 Provider 扩展预留

未来的 provider 插桩可以添加：

- milvus-storage 内部的 scoped recorder；
- provider operation count；
- 内部 range-read/write count；
- provider TTFB 和 transfer duration；
- provider-transferred bytes；
- provider 可见的 retry 和 throttling；
- profile snapshot FFI。

这些扩展应使用预留的 access layer 和 coverage 字段，而不是替换初始 schema。

## 15. 分布式请求 Profiling

### 15.1 显式请求信号

预留的 metadata/header：

```text
x-milvus-storage-profile: summary
```

初始服务端可以先在内部传播该信号，之后再将其暴露给公共 SDK。服务端策略决定实际生效的 level。

### 15.2 传播

Proxy 创建请求归因信息，并通过内部 RPC 传递 profile level、request ID、trace ID、workload kind、tenant/user identity 和 schema version。

每个 QueryNode 创建一个 contribution，并将 recorder 附加到对应的 Search/Query execution。

### 15.3 合并

Proxy 按以下规则合并：

- count 和 byte 通过加法合并；
- 匹配的 histogram bucket 按索引合并；
- min 取最小值；
- max 取最大值；
- coverage 保守合并；
- 使用 contribution identity 去重。

### 15.4 Contribution Identity

```text
cluster_id / node_id / scope_id / task_attempt / execution_id
```

Merger 必须处理重复投递、RPC retry、被取消的 hedged execution、部分节点 coverage，以及不提供 contribution 的旧节点。

### 15.5 初始结果处理

合并后的 profile 被传递给 `NoopSummarySink`。它不会被添加到：

- 公共 API 响应；
- gRPC trailer；
- Proxy access log；
- 管理员 API。

这些都属于未来的 presenter 集成。

## 16. Task Profiling

### 16.1 初始 Task Family

```text
flush
import
compaction
index
load
recovery
snapshot
gc
external_sync
```

`replication` 继续作为保留类型。Secondary persistence 通常归因到 Flush，并通过 replication origin/subtype 标明来源。

### 16.2 Attempt

Task retry 保持相同的 `task_id`，并递增 `task_attempt`。不同 attempt 的 summary 保持分离。未来的展示可以同时显示最终 logical usage 和跨 attempt 的累计工作量。

### 16.3 禁止使用全局 Counter 差值

不能通过对 task 前后的 filesystem-global Counter 做减法来计算 task profile，因为并发 task 会共享 filesystem。Task profile 数据必须来自该 task 自有的 Milvus recorder，否则应标记为 unavailable。

## 17. Prometheus 指标

### 17.1 Label

有界 label：

```text
component
workload_class
workload_kind
operation
outcome
error_category
storage_role
backend_kind
cache_tier
cache_result
```

不得添加 ID、名称、对象路径、bucket、endpoint 或原始错误。

### 17.2 Operation 指标

```text
milvus_storage_operations_total{
  component, workload_class, workload_kind, operation, outcome,
  storage_role, backend_kind
}

milvus_storage_operation_duration_seconds{
  component, workload_class, workload_kind, operation, outcome,
  storage_role, backend_kind
}

milvus_storage_operation_size_bytes{
  component, workload_class, workload_kind, operation, outcome,
  storage_role, backend_kind
}

milvus_storage_bytes_total{
  component, workload_class, workload_kind, direction,
  storage_role, backend_kind
}

milvus_storage_retries_total{
  component, workload_class, workload_kind, operation, retry_reason,
  storage_role, backend_kind
}

milvus_storage_operations_inflight{
  component, workload_kind, operation
}
```

Metric help text 必须说明这些是 Milvus 观测到的 storage operation。可以将其描述为具有代表性的 object-storage operation，但不能描述为精确的 provider 计费请求。

### 17.3 Cache 指标

在数据已经可用的位置复用或扩展现有低 cardinality 指标：

```text
milvus_storage_cache_lookups_total{
  component, workload_kind, cache_tier, cache_result
}

milvus_storage_cache_bytes_total{
  component, workload_kind, cache_tier, cache_action
}

milvus_storage_cache_wait_duration_seconds{
  component, workload_kind, cache_tier, outcome
}

milvus_storage_cache_load_duration_seconds{
  component, workload_kind, cache_tier, outcome
}
```

Cache action 是有界值：`requested`、`served`、`cold`、`filled`、`evicted`。

### 17.4 Profile 控制指标

```text
milvus_storage_profile_decisions_total{
  source, requested_level, effective_level, reason
}

milvus_storage_profile_active_scopes{scope_type}

milvus_storage_profile_dropped_summaries_total{reason}

milvus_storage_profile_snapshot_duration_seconds{component}
```

### 17.5 PromQL

平均读取延迟：

```promql
sum(rate(milvus_storage_operation_duration_seconds_sum{
  operation="read", outcome="success"
}[5m]))
/
sum(rate(milvus_storage_operation_duration_seconds_count{
  operation="read", outcome="success"
}[5m]))
```

P95 读取延迟：

```promql
histogram_quantile(
  0.95,
  sum by (le, workload_kind) (
    rate(milvus_storage_operation_duration_seconds_bucket{
      operation="read", outcome="success"
    }[5m])
  )
)
```

P99 写入延迟：

```promql
histogram_quantile(
  0.99,
  sum by (le, workload_kind) (
    rate(milvus_storage_operation_duration_seconds_bucket{
      operation="write"
    }[5m])
  )
)
```

Storage 吞吐量：

```promql
sum by (workload_kind, direction) (
  rate(milvus_storage_bytes_total[5m])
)
```

Cache 字节命中率：

```promql
sum(rate(milvus_storage_cache_bytes_total{cache_action="served"}[5m]))
/
sum(rate(milvus_storage_cache_bytes_total{cache_action="requested"}[5m]))
```

### 17.6 Cardinality 预算

每个指标都必须提供最坏情况下的笛卡尔积估算。Enum 转换应拒绝任意字符串。新增 label 必须经过设计评审。初始阶段有意排除 `workload_subtype` 和 `phase`。

## 18. 配置与策略

### 18.1 初始配置

```yaml
storage:
  profile:
    enabled: false
    level: summary

    request:
      allowExplicit: false

    task:
      enabled: false
      types: []

    cache:
      enabled: true

    maxActiveScopes: 1024
    maxProfiledRequestsPerSecond: 10
    maxProfiledTasks: 128
```

ParamTable key：

```text
storage.profile.enabled
storage.profile.level
storage.profile.request.allowExplicit
storage.profile.task.enabled
storage.profile.task.types
storage.profile.cache.enabled
storage.profile.maxActiveScopes
storage.profile.maxProfiledRequestsPerSecond
storage.profile.maxProfiledTasks
```

聚合 Prometheus 指标与 profile 是否启用相互独立。

### 18.2 Task 选择

使用一个经过校验的列表，而不是为每种 task 分别设置 boolean：

```yaml
storage:
  profile:
    enabled: true
    task:
      enabled: true
      types:
        - flush
        - import
        - compaction
        - index
```

预留 subtype filter。

### 18.3 TTL Rule 扩展

```go
type ProfileRule struct {
    ID        string
    Level     StorageProfileLevel
    ExpiresAt time.Time
    Selector  ProfileSelector
}

type ProfileRuleProvider interface {
    Match(meta ScopeMeta) *ProfileRule
}
```

初始 provider 不返回任何 rule。未来由 etcd 支持的 admin service 可以匹配 tenant、collection、workload、subtype、task ID、request type 或 node。

### 18.4 决策顺序

```text
global feature enabled?
  → administrator TTL rule
  → long-lived task config
  → explicit invoker request
  → permissions
  → active/rate limits
  → effective level
```

## 19. 性能与安全

### 19.1 Disabled 路径

- 使用 noop scope recorder。
- 不分配 per-scope bucket。
- 不序列化 contribution。
- 继续使用有界归因记录聚合指标。
- 不创建额外的 C++/FFI dependency handle。

### 19.2 Enabled Summary 路径

- 使用固定数组，而不是 sample list。
- 在实际可行时，仅序列化非零 operation entry。
- 仅为可观测的流式读取维护 TTFB。
- 仅在 size 已知时维护 size distribution。
- Provider coverage 保持 unavailable。

### 19.3 限制

- 最大活跃 scope 数量；
- 最大被 profile 的 request 速率；
- 最大被 profile 的 task 数量；
- 最大 contribution 大小；
- 有界的 operation/phase entry；
- 未来的 sink queue 容量。

达到限制时，关闭或降级 profiling，但不使 request/task 失败。

### 19.4 安全与隐私

- 指标中不包含 credential、endpoint、bucket、path、expression、vector data 或 error message。
- 在 presentation 设计定义授权方式之前，profile identifier 保持内部可见。
- 未来面向调用方的展示只能暴露该调用方自己的请求。
- 未来面向管理员的展示必须使用现有 Milvus authorization。
- 未来的 access-log 集成需要单独进行脱敏和大小评审。

### 19.5 Benchmark

Benchmark 范围：

- 修改前的 baseline；
- profile 关闭时的聚合指标；
- profile summary 开启；
- 小型流式读取；
- 大规模顺序读取；
- 并发 range read；
- cache-hit 和 cache-miss Search；
- Go 和 C++ 的观测边界；
- 高并发 Flush/Compaction/Import。

性能预算必须基于实际测量的 baseline 制定。

## 20. 兼容性

### 20.1 配置

默认值保持当前行为。配置验证时拒绝未知 task type。

### 20.2 RPC

Contribution 字段是新的可选 protobuf 字段。旧节点不会提供这些字段。Receiver 应将字段缺失解释为 coverage unavailable，而不是 usage 为零。

### 20.3 Schema

Profile 携带 schema version 和 bucket version。未知 distribution 不得被静默合并。

### 20.4 指标

迁移期间保留现有 storage metric。不得直接修改现有指标的单位或含义。Dashboard 应显式迁移到新的指标集合。

### 20.5 milvus-storage

初始阶段不需要更新依赖。未来的 provider 插桩必须单独评审和版本化。

## 21. 详细实施计划

### Phase 0：语义契约

1. 合入本 MEP。
2. 固定 enum、bucket schema、workload kind、phase、storage role、outcome 和 error category。
3. 确定 Milvus 自有代码共享 Go/C++ 定义的来源。
4. 记录 legacy metric 的迁移方式。
5. 定义“代表性 operation”的表述，并明确声明其不具备计费准确性。

### Phase 1：公共 Profile Library

默认选择 `internal/storageprofile`，除非跨 module 使用要求将其放入 `pkg`。遵守独立的 `pkg` Go module 边界。

实现：

- enum 和验证；
- 有界 attribution；
- 固定 latency/size distribution；
- merge 和 quantile helper；
- coverage model；
- noop 和 active recorder；
- decision/policy 接口；
- noop sink；
- 预留的 presenter/access-log 接口；
- profile-control 指标；
- unit test 和 cardinality test。

### Phase 2：Prometheus 指标

1. 添加 operation Counter 和 Histogram。
2. Duration Histogram 统一使用 seconds，并以 `_seconds` 结尾。
3. 添加 byte Counter 和 size Histogram。
4. 添加 retry/error/outcome 维度。
5. 集成现有 cache signal，同时避免与旧指标重复。
6. 在各组件 registry 中注册指标。
7. 添加 PromQL 示例和 dashboard panel。
8. 在文档说明的迁移窗口内保留旧指标。

### Phase 3：Go Storage 插桩

主要区域：

```text
internal/storage/types.go
internal/storage/remote_chunk_manager.go
internal/storage/local_chunk_manager.go
internal/flushcommon/
internal/datanode/importv2/
internal/datanode/compactor/
internal/datacoord/
pkg/metrics/persistent_store_metrics.go
```

任务：

1. 添加 ChunkManager logical instrumentation。
2. 添加流式 FileReader wrapper。
3. 审计 batch/nested double counting。
4. 审计 Milvus retry 边界。
5. 附加有界 workload attribution。
6. 启用时附加 profile recorder。
7. 保留带类型的 error classification。
8. 覆盖 Import source/persistent phase。
9. 在适用情况下添加 Local/MinIO/Azure/GCP 行为测试，但不做 provider-layer instrumentation。

### Phase 4：Milvus 自有 C++ 插桩

主要区域：

```text
internal/core/src/segcore/
internal/core/src/storage/
internal/core/src/query/
internal/core/src/indexbuilder/
internal/storagev2/
internal/querynodev2/segments/
```

任务：

1. 识别每一个从 Milvus 自有代码进入 packed reader/writer/storage function 的调用。
2. 添加 operation-scope timer 和 known-byte accounting。
3. 在可行时，将 storage usage 附加到现有 request/task `OpContext`。
4. 使用现有 Milvus 自有 result structure 或最小化的 FFI 扩展返回一个 scope snapshot。
5. 不修改 milvus-storage API 或实现。
6. 如实将不可用的 bytes/TTFB/cache data 标记为 unavailable。
7. 验证共享线程池不会泄漏 attribution。

### Phase 5：Task 归因

为以下任务插桩：

1. Flush 和 streaming sync/flush。
2. PreImport、Import、L0 variant、CopySegment。
3. Compaction variant。
4. Index build、Analyze、Stats。
5. Load 和 warmup。
6. Recovery。
7. Snapshot/restore/pin/copy。
8. GC。
9. External refresh/backfill。

可复用 sub-task 必须继承 parent scope。

### Phase 6：分布式请求 Contribution

1. 在 Proxy 接收内部显式 profile decision。
2. 通过 Search/Query RPC 传播。
3. 创建 QueryNode contribution。
4. 在 Proxy 合并。
5. 对 retry/hedge 去重。
6. 将合并结果传递给 noop sink。
7. 不修改 Proxy access log 或公共响应。

### Phase 7：验证与发布

1. 比较新旧聚合指标。
2. 对 storage failure 执行 fault injection。
3. 验证 Search、Import、Flush、Compaction、Index、Load、Recovery、Snapshot 和 GC。
4. 测量 disabled/enabled 开销。
5. 测试滚动升级。
6. 添加 dashboard 文档。
7. 在通过验证 gate 之前，保持功能默认关闭。

## 22. 验证计划

### 22.1 源代码审计

审计所有 Milvus 自有 storage 调用位置：

- ChunkManager method；
- FileReader 使用方式及缺少 Close 的调用；
- retry wrapper；
- Storage V2/V3 调用边界；
- segcore load/search/query 调用；
- import reader 和 SyncTask；
- flush pack writer；
- compaction reader/writer；
- index/analyze/stats task；
- snapshot/copy/GC/external 路径；
- error 构造和重写。

每条路径都必须具有 coverage 或显式 exclusion。

### 22.2 失败矩阵

| 失败 | Outcome/category | 预期行为 |
|---|---|---|
| Key 不存在 | failure/not_found | 保留现有不可 retry 语义 |
| Milvus 可见的 Throttling | failure/throttled | 统计可见的 retry |
| Permission denied | failure/permission_denied | 不执行不恰当的 retry |
| Invalid credentials | failure/invalid_credentials | 不执行不恰当的 retry |
| Timeout | timeout/timeout | 保留现有策略 |
| Cancellation | canceled/canceled | 不分类为 failure |
| Unexpected EOF | failure/unexpected_eof | 在允许时统计可见的 retry |
| 通用 I/O | failure/io_failed | 保留带类型的错误 |

该设计不声称能够观测隐藏在 SDK/milvus-storage 内部的 retry。

### 22.3 端到端场景

1. 所有 cache 均命中的 Search。
2. 跨 QueryNode 存在部分 cache miss 的 Search。
3. 执行并行读取的 Search。
4. 流式读取期间被取消的 Search。
5. Query/retrieve 的冷 scalar access。
6. 同步和异步 warmup。
7. Auto 和 manual Flush。
8. PreImport source scan。
9. Import source read 加 persistent write。
10. L0 Import。
11. CopySegment。
12. Mix/L0/clustering/sort/schema compaction。
13. Index build、Analyze、Stats。
14. Segment/index Load。
15. Streaming recovery。
16. Snapshot create/restore/copy。
17. GC list/delete 和部分失败。
18. External refresh/backfill。
19. Contribution 缺失时的滚动升级。

### 22.4 并发

- 并发 task 不会接收到彼此的 profile 数据。
- 一个 scope 能够安全地记录并发 operation。
- C++ worker pool 不会泄漏 attribution。
- EOF/Close/error 竞争不会重复 finish。
- Async warmup 不继承用户 request identity。
- 重复 contribution 会被去重。

### 22.5 Distribution 测试

- Merge 满足结合律和交换律。
- 合并后的 bucket 与合并 sample 由单个 recorder 记录的结果一致。
- Average 使用合并后的 sum/count。
- Min/max 能正确合并。
- Quantile error 受 bucket width 限制。
- 空 distribution 和单 sample distribution 的行为有明确定义。
- 未知 bucket schema 会产生 incomplete coverage。

### 22.6 Cardinality

- 不接受 ID/name/path 作为 metric label。
- 每个 label value 都是有界 enum。
- 计算最坏情况下的时间序列数量。
- 新 label 没有预算时不能通过评审。

### 22.7 对抗性审查

评审前回答：

- 哪个 Milvus storage 边界仍未插桩？
- 哪个值是 unavailable，而不是零？
- Batch 或 nested call 是否可能重复计算？
- Import output 是否也可能被计为 Flush？
- 共享的全局 Counter 是否可能被误认为 task-local 数据？
- 哪个 retry 仍隐藏在 milvus-storage 或 SDK 中？
- 哪个 cache metric 只是近似值？
- Label 是否可能接收到任意字符串？
- 是否意外修改了任何 Proxy access-log 路径？

## 23. 风险与权衡

### 23.1 具有代表性的 1:1 假设

初始设计假设一个被观测到的 Milvus storage operation 足以代表一个底层 object operation。这可以保持命名和产品语义简单，但 milvus-storage 内部扩展可能使 count 只是近似值。文档必须避免计费方面的声明。

### 23.2 Provider 可见性不完整

Provider retry、TTFB、range-request count 和 transferred bytes 仍然 unavailable。如果优化需求证明 logical boundary 不够充分，预留的 provider layer 可以支持未来扩展。

### 23.3 C++ 粒度

一些 C++ operation 是更高层的 packed read/write。它们的 latency 对 workload 优化仍然有价值，但可能组合多个底层 operation。

### 23.4 Cache 解读

Scanned total/cold bytes 属于 cache-cell 核算，不能保证等同于网络 byte。当现有 tracking 关闭时，cache coverage 可能 unavailable。

### 23.5 不做持久化或展示

初始 noop sink 可以验证采集和 merge 行为，但不提供历史 request/task 视图。这是有意将 instrumentation 与 analytics storage 和产品 UI 分离。

### 23.6 Always-On 指标成本

Histogram 会增加每次 operation 的开销。必须进行 benchmark，并采用有界 label 集合。

## 24. 已考虑的替代方案

### 24.1 立即修改 milvus-storage

延期处理。它可以提供更高的准确性，但在核心模型得到验证之前，需要先完成跨仓库 API、FFI、依赖发布、provider adapter 和滚动兼容性工作。

### 24.2 在 Prometheus 中使用 Request/Task ID

拒绝该方案，因为它会造成无界的时间序列 cardinality。

### 24.3 Filesystem 全局 Counter 差值

拒绝该方案，因为并发 task 会共享 filesystem 状态。

### 24.4 Per-Operation 日志

拒绝将其作为主要机制，因为高频 operation 会产生过多的日志和采集成本。

### 24.5 现在向 Proxy Access Log 添加字段

延期处理。Access-log size、latency、privacy、公共语义和后台 task 都需要单独的展示决策。预留一个 adapter。

### 24.6 仅使用分布式追踪

拒绝该方案，因为 trace 会被采样，不能替代聚合指标或确定性的显式启用 summary。

### 24.7 Ring-Buffer Percentile

拒绝该方案，因为 sample 无法在节点之间确定性合并，而固定 bucket 可以做到。

### 24.8 现在构建中央 Aggregator

拒绝该方案，因为它会在数据契约得到验证前引入 HA、storage、backpressure、retry、deduplication、retention 和 authorization。

## 25. 未来扩展

- milvus-storage 中的 scoped provider instrumentation。
- Provider GET/PUT/RangeGET/retry/TTFB/bytes 指标。
- 存储在 etcd 中的管理员 TTL rule。
- 公共调用方 profile response/trailer。
- Admin profile API 和 UI。
- Task-status profile 展示。
- Kafka/OTLP/ClickHouse sink。
- 有界的 recent-profile debug store。
- 采样的详细慢操作记录。
- 链接到 trace ID 的 Prometheus exemplar。
- 完成 cardinality 测量后的可选 phase/subtype 聚合 label。
- 单独完成 access-log 设计评审后的 Proxy access-log profile adapter。

## 26. 开放实施问题

这些问题不会阻塞语义设计：

1. 考虑 module 边界后，最终 Go package 应位于 `internal` 还是 `pkg`。
2. 哪个内部 protobuf 持有 contribution message。
3. 哪些现有 C++ result structure 可以携带 snapshot，而不需要大范围修改 FFI。
4. 初始 task profile 是立即由 noop sink 丢弃，还是临时保留在测试/debug build 中。
5. 聚合模式和 summary 模式的实测开销预算。

## 27. 完成标准

只有满足以下条件后，该 enhancement 才能进入生产评审：

1. Go 和 C++ 的 Milvus 自有 operation 语义一致并经过测试。
2. 所有约定的 Milvus storage 边界都具有 coverage 或显式 unavailable 状态。
3. Prometheus dashboard 能正确计算 avg/p50/p95/p99。
4. Label 中不包含高 cardinality identifier。
5. 分布式 Search/Query contribution 能正确合并。
6. Import source/persistent role 明确区分，并且 Import output 不被计为 Flush。
7. 在数据可用的位置，cache total/cold/wait 语义已经记录并验证。
8. Retry、throttle、timeout、cancel、partial-read 和 EOF 行为已在可见边界完成端到端追踪。
9. Disabled 和 enabled 性能 benchmark 满足约定预算。
10. 滚动升级报告 partial/unavailable coverage，而不是错误的零值。
11. 初始版本不修改 milvus-storage。
12. 初始版本不修改 Proxy access log。
13. 生产环境的 SummarySink 为 noop，并且不引入外部依赖。
