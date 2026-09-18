# JSON Stats Build 任务内并发优化设计

- 日期：2026-07-29
- 状态：Draft
- 组件：DataNode / Index CGo / Segcore JSON Stats
- 相关文档：[JSON Storage Design Document](./20250308-json_storage.md)
- 主要代码：
  - internal/datanode/index/task_stats.go
  - internal/core/src/indexbuilder/index_c.cpp
  - internal/core/src/index/json_stats/JsonKeyStats.{h,cpp}
  - internal/core/src/index/json_stats/parquet_writer.{h,cpp}
  - internal/core/src/index/json_stats/bson_inverted.{h,cpp}
  - internal/core/unittest/test_json_stats/test_json_key_stats_perf.cpp

## 1. 摘要

当前 JSON Stats Build 对同一份 JSON 数据执行两遍串行扫描：

1. CollectKeyInfo：统计路径、类型和命中行数，用于确定 shredding/shared 布局。
2. BuildKeyStats：再次解析每行 JSON，生成 Arrow/Parquet 列、共享 BSON 和 BSON key 倒排记录。

在 300K rows、8 个 binlog、每行约 256B payload 的测试中：

- Local Build wall time：约 4.19s。
- MinIO Build wall time：约 4.25s。
- Build 平均只使用约 1.12 个 CPU 核。
- 两遍 JSON 处理占 Build 时间约 90.7%。
- S3/MinIO 不是当前主要瓶颈。

本设计选择以下方案：

- 将两遍 JSON 处理都按连续全局 row range 并发。
- worker 只操作 thread-local parser、统计表、Arrow builder 和 BSON 临时结果。
- Parquet writer 和最终 BSON postings 合并保持单线程、有序执行。
- 使用有界 sliding window 和 byte budget 控制在途结果，防止并发导致内存线性增长。
- 使用 DataNode 进程级共享 JSON Stats Build executor，而不是每个 Build 私有创建线程池。
- 实际并发度由节点 CPU 配额、活跃 Build 数量、当前可用 worker token、内存预算和 chunk 数共同决定。

“并发度 4”只作为首轮灰度的安全上限，不是写死的生产并发度。生产运行时的有效并发可以在 1 到该上限之间变化；后续 benchmark 证明安全后，可以提高上限或启用自动上限。

## 2. 当前执行路径

### 2.1 调用链

当前调用链如下：

~~~text
DataCoord 创建 StatsTask
        |
        v
DataNode statsTask.Execute()
        |
        v
createJSONKeyStats()
        |
        | 当前会对启用 JSON Stats 的多个字段执行 errgroup.Go
        v
indexcgowrapper.CreateJSONKeyStats()
        |
        v
C.BuildJsonKeyIndex()
        |
        v
JsonKeyStats::Build()
        |
        +-- CacheRawDataAndFillMissing()
        +-- CollectKeyInfo()
        +-- ClassifyJsonKeyLayoutType()
        +-- JsonStatsParquetWriter::Init()
        +-- BuildKeyStats()
        +-- JsonStatsParquetWriter::Close()
        +-- BsonInvertedIndex::BuildIndex()
        +-- WriteMetaFile()
        |
        v
JsonKeyStats::Upload()
~~~

### 2.2 当前并发特征

当前系统存在两种不同粒度的并发：

- 不同 Stats/Index 任务可以同时在 DataNode 上执行。
- 同一个 StatsTask 中，不同 JSON 字段通过 errgroup 并发 Build。

但是单个 JSON 字段内部的所有行仍然串行处理。

如果直接在每个字段内部再创建 N 个线程，会形成：

~~~text
节点任务并发 × JSON 字段并发 × 字段内部 row worker 并发
~~~

例如两个 StatsTask、每个任务四个 JSON 字段、每字段四个 worker，理论上会同时产生 32 个 row worker，并且每个字段都持有自己的 raw FieldData、Arrow builder、BSON postings 和 writer 状态。这会同时造成 CPU 和内存超卖。

因此，本设计不采用“每个字段私有线程池”的方式。

## 3. 基线测试与瓶颈分析

### 3.1 测试模型

性能测试模拟了完整 Build 流程：

- 生成大批量 JSON。
- 创建 FieldData 并序列化为 insert binlog。
- 将 binlog 上传到 Local 或真实 MinIO 文件系统。
- Build 时重新下载并反序列化 raw data。
- 执行两遍 JSON 处理。
- 构建 Parquet shredding 数据。
- 构建共享 BSON key 倒排索引。
- 上传 Build 输出。
- 统计 CPU time、wall time、RSS、吞吐和物理对象大小。

默认大数据测试为 disabled test，通过以下环境变量控制：

| 环境变量 | 说明 |
|---|---|
| JSON_STATS_PERF_ROWS | 总行数 |
| JSON_STATS_PERF_BINLOGS | 输入 binlog 数量 |
| JSON_STATS_PERF_PAYLOAD_BYTES | 每行附加 payload 大小 |
| JSON_STATS_PERF_WRITE_BATCH_SIZE | Parquet writer batch size |
| JSON_STATS_PERF_STORAGE | local 或 minio |
| JSON_STATS_PERF_KEEP_DATA | 是否保留测试数据 |
| JSON_STATS_PERF_CALLGRIND | 是否只对 Build 阶段开启 Callgrind |

### 3.2 300K rows 基线

测试机器为 i7-8700，6 个物理核、12 个逻辑线程。下表是两次运行的平均值。

| 指标 | Local | MinIO |
|---|---:|---:|
| Build wall | 4.19s | 4.25s |
| Build 平均 CPU 核数 | 1.12 | 1.12 |
| Raw load | 73.5ms | 88.9ms |
| CollectKeyInfo | 1.136s | 1.106s |
| Materialize rows | 2.653s | 2.676s |
| Parquet close | 234ms | 274ms |
| BSON BuildIndex | 82.5ms | 86.5ms |
| Build 后 Upload | 38ms | 60ms |
| 输入上传 | 11.9ms | 185ms |
| Peak process RSS | 710532KB | 762084KB |

Local Build 的主要阶段占比：

| 阶段 | 时间占比 |
|---|---:|
| CollectKeyInfo | 27.2% |
| BuildKeyStats / materialize | 63.5% |
| 两遍 JSON 处理合计 | 90.7% |
| Parquet close | 5.6% |
| Raw load | 1.8% |
| BSON BuildIndex | 2.0% |

MinIO 只使 Build 增加约 54ms。输入首次上传虽然增加到约 185ms，但不属于 Build 的主要 CPU 路径。

### 3.3 Callgrind 结果

由于测试环境 perf_event_paranoid=4，perf stat/record 无权限，因此使用 Callgrind 分析 Build。

主要 inclusive 热点如下。它们存在调用包含关系，不能直接相加：

| 热点 | Inclusive 指令占比 |
|---|---:|
| BuildKeyStats | 51.9% |
| CollectKeyInfo | 28.3% |
| jsmn_parse | 11.8% |
| getType | 11.7% |
| AddKeyStats | 10.7% |
| Parquet close | 10.7% |

同时可以观察到大量以下开销：

- JsonPointer 构造和转义。
- std::map/std::set 查找。
- std::string 创建和复制。
- malloc/free。
- istringstream、stoi/stoll/stof/stod 数值解析。
- 每行重新分配 jsmn token buffer 和 path buffer。
- 对同一 JSON 重复执行 strlen。

### 3.4 结论

当前优化优先级应为：

1. 并发两遍 JSON 解析和 materialize。
2. 限制并发产生的额外内存。
3. 减少 parser/path/map/string/数值转换的单线程开销。
4. 保持 Parquet 和 BSON 最终写入有序。

暂时不应优先优化：

- 输入 S3 下载。
- 输出 S3 上传。
- BSON Tantivy BuildIndex。
- Parquet Close。

实际远程输出约 27.25MB，而 IndexStats 报告约 115.6MB。后者包含逻辑或内存估算，不等于物理上传量。这个统计口径问题可以单独处理，不阻塞本并发方案。

## 4. 目标与非目标

### 4.1 目标

1. 提高单个大 JSON Stats Build 的 CPU 利用率和吞吐。
2. 单任务与多任务场景下都不产生无界线程和无界内存。
3. 根据 worker 当前资源状态决定有效并发，而不是写死为逻辑线程数。
4. 保持现有 row 顺序、row ID、JSON layout 分类和查询语义。
5. 保持现有 JSON Stats 数据格式，不要求升级 data format version。
6. 并发关闭时保留可比较的串行执行路径。
7. 对每个阶段提供可观测的时间、并发和内存指标。

### 4.2 非目标

1. 本阶段不把 raw binlog 下载改造成流式 download-and-parse。
2. 本阶段不并发操作同一个 Parquet writer。
3. 本阶段不并发写同一个 BsonInvertedIndex::inverted_index_map_。
4. 本阶段不修改 JSON Stats 查询端。
5. 本阶段不改变 shredding/shared 的分类算法。
6. 本阶段不解决 CGo context 对正在执行的 C++ Build 的强制取消。
7. 本阶段不保证并发输出文件与串行输出文件逐字节相同，只保证逻辑内容和加载查询结果相同。

## 5. 必须保持的正确性约束

### 5.1 全局行序

FieldData 的遍历顺序和每个 FieldData 内部的行顺序共同定义全局 row ID：

~~~text
global_row_id = 前序 FieldData 行数之和 + 当前 FieldData local row
~~~

所有并发 chunk 必须覆盖互不重叠、连续且完整的全局 row range。

### 5.2 每个输入行必须产生一个输出行

以下输入都必须在 Parquet 中占据一个 row position：

- 正常 JSON。
- nullable field 的 null 行。
- 空 JSON 字符串。
- 空对象。
- 空数组。

尤其不能因为 jsmn_parse 返回 0 或 JSON 字符串为空而跳过 row ID。

### 5.3 BSON posting 的 row ID 和 offset

BSON 倒排值编码为：

~~~text
encoded = row_id << 32 | bson_offset
~~~

worker 可以并发计算 offset，但最终合并必须按照 chunk sequence 顺序执行，使同一个 key 下的 posting 保持全局 row ID 递增。

### 5.4 Schema 和 column 顺序

ClassifyJsonKeyLayoutType 的输入统计必须与串行版本一致。

key_types、column_keys、shared_keys 和 Parquet schema 在进入第二遍 materialize 前完成构建，第二遍期间只读，不允许 worker 修改。

### 5.5 错误生命周期

任一 worker、writer 或 BSON merge 出错后：

1. 设置该 Build 的 cancellation flag。
2. 停止提交新 chunk。
3. 所有已提交 future 必须完成或被显式 join。
4. 释放所有 chunk memory reservation。
5. 最后在 Build 调用线程重新抛出第一个原始错误。

不能在 JsonKeyStats、FieldData 或 writer 已析构后仍有后台 worker 引用这些对象。

## 6. 总体架构

~~~mermaid
flowchart TD
    A[Cache raw FieldData] --> B[Create ordered row ranges]
    B --> C[Pass 1: parallel CollectKeyInfo]
    C --> D[Deterministic reduce]
    D --> E[Classify layout and build immutable plan]
    E --> F[Pass 2: parallel materialize chunks]
    F --> G[Ordered bounded sliding window]
    G --> H[Single-thread Parquet write]
    G --> I[Single-thread BSON posting merge]
    H --> J[Parquet close]
    I --> K[BSON BuildIndex]
    J --> L[Write metadata and upload]
    K --> L
~~~

核心原则是：

~~~text
并发纯计算，串行提交共享状态。
~~~

## 7. Row range 与 chunk 切分

### 7.1 RowRange

建议增加只读的 row range 描述：

~~~cpp
struct FieldDataSlice {
    FieldDataPtr data;
    uint32_t local_begin;
    uint32_t row_count;
    uint32_t global_begin;
};

struct JsonStatsRowRange {
    uint64_t sequence;
    uint32_t global_begin;
    uint32_t row_count;
    std::vector<FieldDataSlice> slices;
    int64_t input_bytes;
};
~~~

一个 range 可以包含多个相邻 FieldData slice，以避免大量小 binlog 产生大量小任务。FieldData 使用 shared_ptr 保证所有 worker 完成前输入不会释放。

### 7.2 双阈值切分

只按 row 数切分会使大 JSON 和小 JSON 的 chunk 成本差异过大。因此建议同时限制：

- chunkRows：最大行数，初始建议 16K。
- chunkInputBytes：最大输入 JSON 字节数，初始建议 8MiB。

达到任一阈值即结束当前 chunk，但绝不拆分单行 JSON。

初始值只是起点，必须通过不同 payload、深度和 key 数量的 benchmark 调整。

### 7.3 为什么不直接使用 81920 rows

当前 81920-row writer batch 的逻辑数据约 31MB。若六个 worker 各持有一个完整 batch，再叠加：

- std::string/std::map 节点开销。
- BSON DOM 和 posting 临时数据。
- Arrow buffer capacity。
- raw FieldData。

额外内存可能达到 300MB 到 500MB，整体峰值容易超过 1GB。

较小的 compute chunk 可以在不改变 writer 配置语义的前提下，更细粒度地控制并发和内存。

## 8. Pass 1：并发 CollectKeyInfo

### 8.1 Worker 输入和输出

每个 worker 只处理一个 JsonStatsRowRange，并维护本地状态：

~~~cpp
struct JsonParseScratch {
    std::vector<jsmntok_t> tokens;
    std::vector<std::string> path;
};

struct CollectChunkResult {
    uint64_t sequence;
    uint32_t row_count;
    std::map<JsonKey, KeyStatsInfo> infos;
    int64_t retained_bytes;
};
~~~

tokens 和 path 在 worker 内重复使用，不再每行从初始 capacity 16 重新分配。

### 8.2 Reduce

Build 调用线程按 sequence 获取结果，并执行：

~~~text
global_infos[key].hit_row_num += local_infos[key].hit_row_num
num_rows += chunk.row_count
~~~

当前 KeyStatsInfo 只有整数计数，因此合并满足结合律和交换律。为便于测试和未来加入 min/max，仍建议按 sequence 确定性合并。

num_rows 必须统计所有物理输入行，包括 null 行；null 行只是不增加任何 key hit。

### 8.3 预期收益

CollectKeyInfo 占当前 Build 约 27.2%。若只并发这一阶段，四 worker 的理想总耗时约为：

~~~text
4.19s - 1.136s + 1.136s / 4 ≈ 3.34s
~~~

即理想加速约 1.25 倍。考虑调度和 merge 开销，单独并发第一遍的收益有限，因此第二遍 materialize 必须同时优化。

## 9. Immutable BuildPlan

第一遍完成分类后，构建只读 BuildPlan：

~~~cpp
struct JsonKeyAction {
    JsonKey key;
    JsonKeyLayoutType layout;
    int32_t column_index;
    std::vector<std::string> decoded_bson_path;
};

struct JsonStatsBuildPlan {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<JsonKey> ordered_columns;
    std::map<JsonKey, JsonKeyAction> actions;
};
~~~

BuildPlan 的作用：

- 固定 schema 和列顺序。
- 预计算 JsonKey 到 column/shared action 的映射。
- 对 shared key 预计算 ParseJsonPointerPath，避免每行重复解析 pointer。
- 允许所有 materialize worker 无锁只读。

第一阶段实现可以继续保留每行 std::map<JsonKey, string>，先保证语义一致；后续再将 action lookup 和 hit bitmap 用于减少 map/set/string 开销。

## 10. Pass 2：并发 Materialize

### 10.1 Worker 不能直接写共享对象

以下对象当前都不是并发写安全的：

- JsonStatsParquetWriter 内部 Arrow builders。
- PackedRecordBatchWriter。
- BsonInvertedIndex::inverted_index_map_。
- JsonKeyStats 的 column/shared mutable state。

因此 worker 不调用：

- parquet_writer_->AppendValue。
- parquet_writer_->AppendSharedRow。
- parquet_writer_->AddCurrentRow。
- bson_inverted_index_->AddRecord。

### 10.2 Worker 输出

每个 worker 使用独立 Arrow builders 和 BSON 临时对象，输出：

~~~cpp
struct MaterializedChunk {
    uint64_t sequence;
    uint32_t global_begin;
    uint32_t row_count;
    std::shared_ptr<arrow::RecordBatch> record_batch;
    std::map<std::string, std::vector<int64_t>> bson_postings;
    int64_t retained_bytes;
};
~~~

其中：

- record_batch 内的 row 顺序与输入 range 完全一致。
- 每个输入 row 都向每个 shredding column append 一个 value 或 null。
- 最后一列为该 row 的 shared BSON 或 null。
- bson_postings 使用真实 global row ID。
- offset 是相对于该行 BSON document 的 offset，与 chunk 起点无关。

### 10.3 Null 和空字符串处理

worker 对 null/空字符串行执行与现有 BuildKeyStatsForNullRow 相同的逻辑：

- 所有 shredding column append null。
- shared BSON column append null。
- row_count 增加一。
- 不产生 BSON posting。

### 10.4 BSON posting 合并

Build 调用线程按 sequence 合并：

~~~cpp
for (auto& [key, postings] : chunk.bson_postings) {
    bson_inverted_index_->AddRecordsBatch(key, postings);
}
~~~

建议增加批量接口，避免每个 posting 都进行一次 map 查找和函数调用。

由于 chunk 按全局 row range 顺序合并，同一个 key 下的 posting 顺序与串行版本一致。

## 11. 有序 sliding window 与背压

### 11.1 为什么使用 sliding window

worker 完成顺序可能与 row range 顺序不同，但 writer 必须按照 row 顺序提交。

无需构建无界的 result queue。Build 调用线程可以维护按 sequence 排列的 future window：

~~~text
提交 sequence 0 ... W-1
等待 future[0]
写入 chunk 0 并释放内存
提交 sequence W
等待 future[1]
写入 chunk 1并释放内存
...
~~~

后面的 worker 可以在 writer 写当前 chunk 时继续计算，实现计算和单 writer I/O 重叠。

### 11.2 Window 上限

window size 由以下条件共同限制：

~~~text
window = min(
    per-build worker 上限 + 1,
    剩余 chunk 数,
    maxInflightChunks,
    内存预算允许的 chunk 数
)
~~~

初始建议：

- maxInflightChunks = effective workers + 1。
- chunkRows = 8K 到 16K。
- 必须同时配置 maxInflightBytes。

### 11.3 Byte reservation

只限制 chunk 数不足以处理超大 JSON。提交任务前应根据输入字节数和列数做保守 reservation：

~~~text
estimated_chunk_bytes =
    input_bytes × expansion_factor
    + row_count × fixed_row_overhead
    + column_count × builder_overhead
~~~

worker 完成后记录实际 Arrow buffer、BSON 和 posting capacity，并更新统计。

为了避免所有 worker 持有结果后阻塞在 byte semaphore 上，不允许 worker 在完成结果后等待额外内存 token。若实际值超过 reservation：

- 允许当前 chunk 临时超出预算。
- 暂停提交新 chunk。
- 等有序 writer 消费并释放旧 chunk 后再继续。

这样最大超出量被限制在有限 window 内，不会形成死锁。

## 12. Parquet writer 改造

### 12.1 接口拆分

当前 JsonStatsParquetWriter 同时负责：

- 创建 schema/builders。
- 接收逐值 append。
- 形成 RecordBatch。
- 调用 PackedRecordBatchWriter。

建议拆分为：

1. BuildPlan/BuilderFactory：创建每个 worker 的独立 builders。
2. ChunkMaterializer：生成 RecordBatch。
3. OrderedWriter：只按顺序接收 RecordBatch 并写入 PackedRecordBatchWriter。

新增接口示意：

~~~cpp
std::vector<std::shared_ptr<arrow::ArrayBuilder>>
CreateChunkBuilders(const JsonStatsBuildPlan& plan);

arrow::Status
WriteRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch);
~~~

### 12.2 Writer 仍保持单线程

当前 packed write 和 close 占比明显小于 JSON materialize。并发调用同一个 PackedRecordBatchWriter 会增加：

- 线程安全风险。
- 输出顺序控制。
- multipart upload 状态竞争。
- 错误回滚复杂度。

第一阶段不并发 writer。

### 12.3 Compute chunk 与 write batch

compute chunk 建议小于当前 81920-row write batch。实现需要评估两种策略：

1. 直接按 compute chunk 调用 PackedRecordBatchWriter::Write。
2. OrderedWriter 将多个连续 compute chunk 合并到接近 writeBatchSize 后再写。

MVP 优先采用直接写入，避免额外数组复制。必须通过 benchmark 检查：

- 输出文件大小。
- Parquet row group 数量。
- PackedRecordBatchWriter 调用开销。
- 上传请求数量。
- Load 和 query 性能。

若物理输出大小或 writer 开销回退超过验收阈值，再增加 batch coalescing。现有 jsonStatsWriteBatchSize 仍保留为 writer 层参数，不与 compute chunkRows 混为一个配置。

## 13. 节点级动态并发模型

### 13.1 不使用 hardware_concurrency 直接开满

std::thread::hardware_concurrency() 可能看到宿主机逻辑线程数，而不是容器 CPU quota。

Milvus 已通过 Go 的 hardware.GetCPUNum 和 automaxprocs 获取 cgroup-aware CPU 数，并调用 InitCpuNum 写入 C++ CPU_NUM。JSON Stats executor 应使用 CPU_NUM，不应重新读取 hardware_concurrency。

### 13.2 两级控制

并发控制分为两级：

1. 节点级 hard capacity：限制整个 DataNode 进程中 JSON Stats worker 总数。
2. Build 级 soft share：多个活跃字段/任务之间公平共享节点 worker。

定义：

~~~text
C_effective = cgroup-aware CPU_NUM
C_configured = 配置的 maxWorkers；0 表示 auto
C_json_max  = clamp(resolve(C_configured, CPU reserve), 1, C_effective)
B_active    = 当前活跃 JSON field Build 数
P_soft      = max(1, floor(C_json_max / B_active))
~~~

resolve 必须为 standalone/共享节点保留在线和其他离线任务所需 CPU，不能把 C_effective 全部分配给 JSON Stats。

单个 Build 的有效并发为：

~~~text
P_effective = min(
    perBuildMaxWorkers,
    当前可分配 worker token,
    P_soft + 可借用的空闲 token,
    内存预算允许的并发,
    chunk 数
)
~~~

当其他 Build 没有 pending chunk 时，当前 Build 可以借用空闲 token；有新 Build 加入时，不抢占已经执行的 chunk，只在后续 chunk dispatch 时重新计算 share。

### 13.3 “worker 当前状态”的含义

本设计中的当前状态包括：

- cgroup CPU quota。
- JSON Stats executor 当前 active worker 数。
- 当前活跃 JSON field Build 数。
- pending chunk 数。
- 全局和单 Build 已 reservation 的 inflight bytes。
- DataNode 对离线任务预留后的资源上限。

不建议按每个 chunk 采样瞬时 CPU usage 决定线程数。瞬时采样存在滞后和抖动，容易导致并发度频繁升降。token accounting 比 CPU 百分比采样更稳定、可测试。

### 13.4 并发 4 的定位

首轮灰度可以配置：

~~~text
C_json_max = 4
~~~

这表示节点级安全上限为 4，不表示每个 Build 永远使用 4 个 worker：

- 节点只有两个有效 CPU 时，最多使用两个。
- 两个大 Build 同时运行时，可以各获得约两个 soft worker。
- 内存不足时，单个 Build 可能只运行一个或两个 chunk。
- 小输入只有两个 chunk 时，不会创建四路并发。

在 6C/12T 测试机上验证 6 或 8 worker 的收益和内存后，可以提高 hard capacity；不应因为机器显示 12 个逻辑线程就直接创建 12 个私有 worker。

### 13.5 与现有 DataNode task slot 的关系

现有 DataNode slot 机制负责节点选择和粗粒度任务准入：

- CalculateNodeSlots 综合 CPU 和内存计算节点总 slot。
- QuerySlot 汇总 index/stats、compaction 和 import 已使用的 slot。
- StatsTask 的 taskSlot 主要按 segment size 估算。

taskSlot 不是 CPU core 数，也不是 row worker 数，并且 usingSlot 还可能包含已入队但尚未真正执行的任务。因此不能直接使用：

~~~text
row_workers = taskSlot
~~~

正确的职责划分是：

- DataNode slot：决定是否还适合向该节点放入新的大任务。
- JSON Stats executor token：决定已经进入节点的 Build 此刻能运行多少个 row worker。
- inflight byte budget：限制这些 worker 可以同时保留多少 materialized data。

后续可以把 active task/slot pressure 作为调整 C_json_max 的输入，但只能在任务或 chunk dispatch 边界平滑调整，不能把 slot 数机械映射成线程数。

## 14. Executor 放置与跨语言配置

### 14.1 选择独立的 JSON Stats Build executor

不建议复用 Search CPU executor 或 Load CPU executor：

- JSON Stats 是离线 CPU 密集任务。
- 复用在线 search/load pool 可能造成查询和加载饥饿。
- 独立 executor 更容易设置资源上限和指标。

不建议每个 JsonKeyStats 创建私有 executor：

- 多任务、多字段时线程数相乘。
- 线程频繁创建和销毁。
- 无法实现节点级公平和统一指标。

建议增加进程级 JsonStatsBuildExecutor/JsonStatsBuildResourceManager。

### 14.2 不新增任务 Proto 字段

并发度、chunk size 和 inflight memory 都是 worker 本地执行策略，不影响索引语义，不应由 DataCoord 固化在任务协议中。

建议：

- 在 dataNode.jsonStats.parallelBuild 下增加本地参数。
- DataNode 启动或参数刷新时，通过 C API 初始化/resize C++ executor。
- 不向 CreateStatsRequest 和 BuildIndexInfo 增加并发字段。
- 不改变 JSON Stats data format version。

C API 示例：

~~~cpp
void
SetJsonStatsBuildExecutorConfig(int32_t max_workers,
                                int32_t per_build_max_workers,
                                int32_t chunk_rows,
                                int64_t chunk_input_bytes,
                                int64_t max_inflight_bytes);
~~~

resize 只影响后续 chunk dispatch，不中断正在执行的 chunk。

### 14.3 建议参数

| 参数 | 首轮建议 | 说明 |
|---|---:|---|
| dataNode.jsonStats.parallelBuild.enabled | false | 灰度开关 |
| dataNode.jsonStats.parallelBuild.maxWorkers | 4 | 节点级 worker hard cap；验证后可支持 0=auto |
| dataNode.jsonStats.parallelBuild.perBuildMaxWorkers | 0 | 0 表示由公平 share 和资源预算决定 |
| dataNode.jsonStats.parallelBuild.fieldConcurrency | 1 | 同一 StatsTask 同时进入 CGo Build 的 JSON 字段数 |
| dataNode.jsonStats.parallelBuild.chunkRows | 16384 | compute chunk 最大行数 |
| dataNode.jsonStats.parallelBuild.chunkInputBytes | 8388608 | compute chunk 最大输入字节数 |
| dataNode.jsonStats.parallelBuild.maxInflightBytes | 536870912 | 节点级在途 materialized chunk 内存上限 |

这些默认值必须经过 1M rows、宽 JSON、深 JSON 和多任务压力测试后再定稿。

## 15. 多 JSON 字段并发

当前 createJSONKeyStats 对所有启用字段直接 errgroup.Go，没有并发上限。

引入 row-level 并发后必须增加 fieldConcurrency 限制，否则即使 C++ worker 总数受限，多个字段仍会同时持有：

- raw FieldData。
- 第一遍统计 map。
- 全量 BSON inverted_index_map。
- Parquet writer。
- materialized chunk window。

首轮建议 fieldConcurrency=1。大字段内部 row 并发已经可以使用多个 CPU，串行字段可以显著降低内存风险。

后续可以根据字段输入大小和节点内存预算动态提高到 2，但所有字段必须共享同一个 C++ executor 和 inflight byte budget。

## 16. 内存模型

### 16.1 当前常驻内存

即使串行 Build，也会同时保留：

- 已下载反序列化的 FieldData。
- key_types、column_keys 和路径字符串。
- Parquet builders 当前 batch。
- shared BSON postings 全量 map，直到 BuildIndex 完成。
- writer 和压缩/上传 buffer。

### 16.2 并发新增内存

并发主要增加：

- 每 worker parser tokens/path。
- 每 worker 第一遍 local stats map。
- 每个在途 chunk 的 Arrow arrays。
- 每个在途 chunk 的 BSON document 和 postings。
- future/result/reorder 元数据。

全量 BsonInvertedIndex postings 原本已经存在，不应因为 worker 数复制为多份。chunk posting 合并后必须立即释放本地 posting。

### 16.3 预算原则

内存预算必须同时满足：

~~~text
节点级所有 Build 的 inflight bytes <= global maxInflightBytes

单 Build 的 inflight bytes <= 该 Build 获得的 byte share

inflight chunk 数 <= workers + 1
~~~

如果无法获得至少一个 chunk 的 reservation，则该 Build 应等待资源，而不是绕过预算无限分配。

### 16.4 当前内存状态

DataNode 可以使用 cgroup-aware total memory 和当前 process used memory，在启动新字段 Build 前进行 admission：

~~~text
available_for_new_build =
    total_memory
    - current_used_memory
    - safety_reserve
    - already_reserved_build_memory
~~~

瞬时 available memory 只用于是否允许启动新的字段 Build 和设置初始 byte share。已经开始的 Build 通过 reservation 保证上限，不根据瞬时系统内存频繁缩小到低于已持有值。

## 17. 单线程热点优化

并发化之外，建议按风险从低到高执行以下优化。

### 17.1 低风险优化

1. 使用 Json::data().size()，避免每遍每行 strlen。
2. worker-local 复用 jsmn token vector 和 path vector。
3. 根据上一行最大 token 数保留 capacity。
4. 预计算 shared key 的 decoded BSON path。
5. 将 JsonKey 到 column action 的查找结果放入 immutable BuildPlan。
6. 将 BsonInvertedIndex::AddRecord 改为 batch merge。

### 17.2 数值解析

当前 getType 和 Arrow append 使用：

- istringstream。
- stoi/stoll。
- stof/stod。

建议评估 std::from_chars 或 fast_float：

- 整数优先使用 from_chars。
- 浮点使用支持完整 JSON number 语义的实现。
- 必须覆盖科学计数法、边界值、负数、溢出和非法输入。

数值解析改变容易影响类型判断和错误语义，应独立提交并做 differential test。

### 17.3 Path 和 row 临时 map

当前每行先构建 std::map<JsonKey, string> 和 std::set<JsonKey>。

后续可以使用：

- column ID。
- hit bitmap。
- string_view。
- 预分配 small vector。

但 JSON 重复 key、混合类型和 shared DOM 构建行为必须与当前实现保持一致，因此不放在并发 MVP 中。

## 18. 错误、取消和清理

### 18.1 Worker 错误

所有 worker 包装为：

~~~text
try:
    process chunk
catch:
    atomically store first exception
    set cancelled=true
    rethrow to future
~~~

Build 调用线程发现错误后：

- 不再提交新任务。
- 通知尚未开始的 worker 快速退出。
- 逐个 get/join 已提交 future。
- 清空 chunk result。
- 释放资源 lease。
- 重新抛出第一个错误。

### 18.2 Writer 错误

若 WriteRecordBatch 或 Close 失败：

- 设置同一个 cancellation flag。
- 等待所有 worker 结束。
- 不再合并任何后续 postings。
- 保持现有 Build 失败语义。

不能为了提前返回而遗留后台 worker。

### 18.3 Go context

当前 CreateJSONKeyStats 的 ctx 不会中断正在同步执行的 C.BuildJsonKeyIndex。

本并发设计至少要求内部错误能够取消尚未开始的 chunk。将 Go context 传播为 C++ cancellation token 可以作为后续设计，不应与本次性能改造混在同一阶段。

## 19. 可观测性

### 19.1 Build 阶段指标

建议记录以下 histogram：

- raw_data_load_duration。
- collect_key_info_duration。
- classify_layout_duration。
- materialize_duration。
- ordered_write_duration。
- parquet_close_duration。
- bson_build_duration。
- metadata_write_duration。
- upload_duration。

### 19.2 Executor 指标

建议记录：

- executor_capacity。
- active_workers。
- queued_chunks。
- active_builds。
- chunk_queue_wait_duration。
- effective_parallelism。
- worker_busy_duration。

### 19.3 内存和吞吐指标

建议记录：

- inflight_chunks。
- inflight_bytes。
- peak_inflight_bytes。
- chunk_input_bytes。
- chunk_output_bytes。
- rows_per_second。
- build_cpu_time / build_wall_time。
- reservation_underestimate_count。

指标 label 不包含 segment ID、field ID 等高基数字段。每个 Build 完成时输出一条汇总日志即可，不记录每行或每 chunk INFO 日志。

现有 JsonStatsBuildProfile 可以继续用于 disabled perf test；生产指标应使用 Prometheus histogram/gauge，而不是依赖测试 getter。

## 20. 测试设计

### 20.1 Row range 单元测试

覆盖：

- 单个 FieldData。
- 多个 FieldData。
- chunk 正好落在 binlog 边界。
- chunk 跨 binlog。
- 总行数小于 chunkRows。
- 最后一个不完整 chunk。
- 0、1、chunkRows-1、chunkRows、chunkRows+1 行。

验证所有 global row ID 恰好出现一次。

### 20.2 Pass 1 differential test

对同一输入分别运行串行和并发 CollectKeyInfo，比较：

- num_rows。
- 每个 JsonKey。
- JSONType。
- hit_row_num。
- 最终 layout type。
- column/shared key 集合和顺序。

### 20.3 Pass 2 differential test

比较串行和并发输出的逻辑内容：

- Parquet schema 和 metadata。
- 每列逐行 value/null。
- shared BSON bytes。
- 每个 BSON key 的 row ID 和 offset。
- JsonKeyStats Count。
- Load 后各种 JSON filter 结果。

不要求文件字节完全相同，因为 RecordBatch/row group 边界可能变化。

### 20.4 特殊数据

必须覆盖：

- null 和空字符串。
- 空对象、空数组。
- nested object/array。
- 同一路径混合类型。
- Unicode 和 escaped string。
- 路径包含 / 和 ~。
- 极深 JSON。
- 极宽 JSON。
- 大字符串和大数组。
- 数字边界、科学计数法和非法 JSON。

### 20.5 并发确定性

通过 barrier 人为制造：

- sequence 1 先于 sequence 0 完成。
- 中间 chunk 最慢。
- 最后 chunk 最先完成。

多次运行后比较逻辑输出，保证 writer 和 posting merge 始终按 sequence。

### 20.6 错误注入

注入：

- 某个 worker parse 失败。
- Arrow builder 失败。
- Packed writer Write 失败。
- Close 失败。
- BSON merge/BuildIndex 失败。

验证：

- 所有 future 已 join。
- active worker 回到 0。
- inflight byte reservation 回到 0。
- 无 use-after-free。
- Build 返回原始错误。

### 20.7 多任务压力测试

场景：

- 1、2、4 个 StatsTask 同时 Build。
- 每个任务 1、2、4 个 JSON 字段。
- 与 compaction/import/index build 同时运行。
- standalone 和独立 DataNode。

验证节点级 active JSON worker 不超过 hard capacity，RSS 不超过预算，在线操作没有明显长尾回退。

## 21. 性能测试矩阵

### 21.1 并发度

至少测试：

- 串行旧路径。
- 新路径 maxWorkers=1。
- 2 workers。
- 4 workers。
- 6 workers。
- 8 workers。

不默认测试 12 workers 为最佳值，应根据 CPU time、wall time、上下文切换和 RSS 判断拐点。

### 21.2 数据规模

至少测试：

- 100K、300K、1M rows。
- 64B、256B、2KiB payload。
- 窄 JSON、宽 JSON、深 JSON。
- 1、8、64 个 binlog。
- 1 和多个 JSON 字段。

### 21.3 存储

分别测试：

- Local。
- MinIO/S3。

MinIO 测试必须使用真实远程 Arrow filesystem 路径，避免只测试 mock 延迟。

### 21.4 建议验收线

以下是目标，不是当前已验证结果：

- maxWorkers=1 相比旧串行路径回退不超过 5%。
- 300K local、单 Build、4-worker cap 的 wall time 至少提升 2 倍。
- 2.5 到 3 倍作为 stretch goal。
- 平均 CPU 核数明显高于当前 1.12。
- peak RSS 增量受 maxInflightBytes 控制，不随总 chunk 数增长。
- 多任务总吞吐提高，且不会因每任务私有线程池导致 CPU 超卖。
- MinIO 下并发路径相对 Local 不出现明显额外回退。
- 输出物理大小回退不超过 5%；超过时启用 write batch coalescing。

根据 Amdahl 定律，两遍 JSON 处理占 90.7%。四 worker 的理论下限约为：

~~~text
serial_part + parallel_part / 4
≈ 0.39s + 3.80s / 4
≈ 1.34s
~~~

该值未包含调度、merge、内存分配和 writer 开销，不能作为承诺。实际目标应通过 benchmark 决定。

## 22. 实施阶段

### Phase 0：保留基线和 profiling

- 保留 disabled 端到端 perf test。
- 保留阶段计时和 CPU/RSS 统计。
- 固定一组 Local/MinIO 回归数据。

### Phase 1：结构拆分，仍以并发度 1 运行

- 引入 RowRange。
- 引入 immutable BuildPlan。
- 拆分 ChunkMaterializer 和 OrderedWriter。
- 增加 AddRecordsBatch。
- 新 pipeline 先使用一个 worker。
- 完成 serial-vs-new differential test。

这一阶段先证明结构拆分没有改变数据语义。

### Phase 2：节点级 executor 和 Pass 1 并发

- 增加 JsonStatsBuildExecutor。
- 增加节点级 worker token。
- 增加 fieldConcurrency 限制。
- 并发 CollectKeyInfo。
- 增加 executor 指标和错误 join 测试。

### Phase 3：Pass 2 并发和有界 writer pipeline

- worker-local Arrow/BSON materialize。
- ordered sliding window。
- global/per-build inflight byte budget。
- 单 writer 和批量 posting merge。
- 完成 Local/MinIO 性能矩阵。

### Phase 4：动态资源和调优

- 根据 active Build 数动态计算 soft share。
- 支持空闲 token 借用。
- 根据 DataNode 当前内存状态限制字段 admission。
- 调整默认 maxWorkers、chunkRows 和 maxInflightBytes。

### Phase 5：单线程微优化

- strlen 改为已知长度。
- parser/path buffer 复用。
- pointer/path 预计算。
- from_chars/fast_float。
- column ID 和 hit bitmap。

微优化建议独立提交，便于分别验证收益和正确性。

## 23. 风险与缓解

| 风险 | 缓解措施 |
|---|---|
| 多任务和多字段导致线程数相乘 | 进程级共享 executor；Go fieldConcurrency 限制 |
| 并发结果导致峰值内存过高 | 小 chunk、有界 window、global maxInflightBytes |
| worker 乱序导致 row ID 错位 | 连续 global row range；按 sequence 写入和合并 |
| Parquet writer 非线程安全 | writer 仅由 Build 调用线程访问 |
| BSON map 并发写竞争 | worker 返回 local postings，按 sequence 批量 merge |
| 错误提前返回造成后台 UAF | cancellation flag；停止提交；显式 join 所有 future |
| 小 RecordBatch 增加物理文件或压缩开销 | 输出大小验收；必要时 writer coalescing |
| 宽 JSON 使 local stats map 过大 | byte-aware chunk；Pass 1 结果及时 merge 和释放 |
| 瞬时 CPU usage 导致并发抖动 | 使用 token accounting，不按瞬时百分比频繁调节 |
| 复用在线线程池影响查询 | 独立 JSON Stats Build executor |
| 新参数扩大任务 Proto | 参数保持 DataNode 本地，通过 C API 初始化 executor |

## 24. 最终建议

推荐首先实现以下最小闭环：

1. 保留当前端到端性能测试作为基线。
2. 将字段 materialize 拆成 worker-local MaterializedChunk。
3. 使用进程级共享 executor。
4. 使用按 sequence 的有界 sliding window。
5. writer 和 BSON merge 保持单线程。
6. 首轮节点 hard cap 设为 4，fieldConcurrency 设为 1。
7. 使用 maxInflightBytes 控制额外内存。
8. 通过 1、2、4、6、8 worker benchmark 找到 CPU、吞吐和 RSS 的实际拐点。

最终生产并发度应由 worker 当时的资源状态决定。“4”只应作为安全灰度上限，而不是算法常量。
