# Growing index implementations

本目录实现 growing owner 及稳定 reader 版本发布。Text、R-Tree 与支持的 Knowhere
interim vector 已接入 Segment 生产 feed。公共接口和发布协议在
../contracts/growing/IGrowingIndex.h；不再有 family-local GrowingAppenderBase 或按 ISpatialReader 特设的接口。

## 接口与所有权

三个实现都继承 IGrowingIndex，并按物理输入实现 IAppendable<Batch>：

| 实现 | 输入能力 | 目标 reader 能力 |
|---|---|---|
| TantivyGrowingTextIndex | TextBatch | ITextMatchReader 及其他实际支持的能力 |
| RTreeGrowingSpatialIndex | ScalarBatch<string_view>（WKB） | ISpatialReader，候选结果需原值精确过滤 |
| KnowhereGrowingVectorIndex<T> | VectorBatch<GrowingVectorStorageType<T>> | IVectorReader |

Segment 唯一持有 IGrowingIndex。GrowingIndexSet 先调用 owner 的 CommitIfNeeded，使 interval writer 在查询到来时提交已到期或首个尚未发布的 accepted generation，再由 PinSnapshot 返回绑定一个 reader 记录与 CoveredRowEnd 的 GrowingIndexSnapshotPin；同步发布型实现的 CommitIfNeeded 可用默认 no-op，错误不会以旧 pin 掩盖。Load/Reopen 调用更强的 `Flush`：已构建 owner 成功返回必须已发布所有 accepted rows；未达到 family 构建阈值时可以保持空 pin/raw fallback，不为 Flush 强制提前构建。查询按所需能力转换 Reader 引用，向量路径每个 pin 只转换一次统一 `IVectorReader`，并保持 pin 存活。记录内部唯一持有 reader；pin 协议内部共享记录，不共享可写 reader 所有权。所有实现复用基类发布逻辑，不再提供 typed snapshot、erased snapshot 与独立 watermark 三套 getter。

## 必须保留的不变式

1. 引擎写入与 commit 由写侧串行化；发布锁只保护版本指针切换。构建、查询和旧版本析构不持有发布锁。
2. PublishSnapshot 必须原子固定 Reader::Count、CoveredRowEnd、validity/offset mapping 和依赖生命周期。Tantivy/R-Tree 的记录绑定不可变引擎视图；Knowhere 记录可以绑定同一单活 Add/Search 引擎，但 reader 必须遵守记录的固定物理前缀且引擎自身须保证并发安全。分配失败、空 reader、负边界、边界回退不修改当前记录；已发布记录不会因 IGrowingIndex 析构而使现有 pin 的依赖失效。
3. CoveredRowEnd 是连续完整的 Segment 行前缀。不能用累计 Append 数量或最大完成 offset 计算；前面有未完成/失败的范围时不能越过。null 行仍占行号。嵌套元素 Count 与行覆盖边界不同。
4. Append 部分失败可能已经污染 writer；不推进覆盖边界只是必要条件。Knowhere 首次 cold Build 失败只有在从未发布过 engine 且完整 raw 仍保留时才允许回到空 pin/raw fallback；已构建 engine 的 Add 失败会 terminal-poison owner 并传播，不能重放或重建。Add 已成功而 Publish 分配失败时只重试发布，不能再次 Add。
5. GrowingCommitPolicy 仅提供 steady-clock 提交节奏；NoteCommitted 在成功发布后调用。它不提供读侧 watermark，也不统计可用行前缀。
6. typed batch 借用输入；Append 返回前需完成消费或复制。构建阈值前无记录时返回空 pin/raw fallback；未覆盖尾部由 exec 决定如何补齐，查询自身的可见性过滤仍然必须应用。
7. 普通 Insert 可以只接受而暂不发布后续 Text generation；首次 accepted generation 在首次 pin 时强制发布。Load/Reopen 在行可见或 schema 发布前调用 Flush，不能把 cadence 到期误当成强制加载边界。

## 引擎接线状态

- Text：实现使用启用 background merge 的 long-lived writer，并以 SetBitsetGrowing 创建独立的 manual-reload reader/searcher generation；旧版本不共享可 reload 的 writer wrapper。每次 commit 冻结对应 count，保证 Count、bitmap 尺寸和覆盖边界描述同一批数据；部分 add/commit 失败通过 native rollback 回到上一提交点并只重放未提交窗口。Analyzer 配置和 commit interval 由构造参数注入，不读 Segment 全局配置。
- Vector：实现使用一个单活 Knowhere engine 继续 Add/Search，不要求旧 pin 的 ANN 命中不变。每个发布记录固定 Count、CoveredRowEnd、逻辑行前缀和依赖生命周期（nullable 映射由共享 engine 内的 knowhere IdMap 持有，见 #50524）；固定的是逻辑/物理前缀，不是共享 live engine 后续返回的 ANN hits。Search 与 iterator 在 candidate/top-k 前按 `min(query-visible row end, pin coverage)` 生成同代 physical prefix；value/raw refine 使用同一边界，deferred iterator 保留 pin。冷启动由 IGrowingIndex 直接建立引擎，初始 Build 与增量 Add 分开，不经过 Artifact，并保留原构建阈值与 native Build/Add 参数。未达阈值保持空 pin/raw fallback；首次 Build 失败也仅在完整 raw 仍在且无已发布 engine 时使用该安全 fallback。DataView 回调持有 typed chunk storage，不持有 Segment/VectorBase。二进制和 vector-array 仍未接入。本链路尚未完成全量构建与基础 e2e 验证。
- Spatial：实现把每个已接受窗口的 MBR/row offset 构造成不可变 RTreeQueryEngine shard，并通过按大小层级合并的 RTreeIndexState 发布现有 RTreeIndexReader；不共享活树，也不另造 growing 查询类。每层至多一个 shard，实际 fanout 由含可索引 geometry 的发布窗口数决定；小窗口不会破坏该约束。CommitIfNeeded/Flush 发布不足阈值的尾部。NULL offset 随代冻结；无效、不可解析或 empty geometry 保留既有跳过语义但仍推进连续行覆盖。空 shard 集合和无查询 MBR 都保守返回全部非 NULL 行，由消费者读取原始 geometry 做精确过滤。R-Tree owner 仅在 geometry field 已配置 index 且全局 interim-segment-index gate 开启时接入 Segment。

RTree 的 publication 成本不是简单的 O(log N)：每次发布都会复制累计 NULL offset；无 carry 时只构造新增 parsed Values 的 shard，有 carry 时复制并重建本次被合并的已占用层。当前代的 MemoryUsage 只计其引用的 shard 与 NULL state；新旧 pin 并存时 shard 可共享或被替换，实际 active/retired 资源必须按 owner 并集记账，不能把各代报告直接相加，也不能把该值冒充精确 heap 总量。

## 生产消费者接线门槛

- GrowingIndexSet 已接入 Text/R-Tree/Vector 的 Initialize、Append、Flush 与 snapshot pin。Segment 先保留 Insert/Load 的 typed input，再记录 raw-ready 连续前缀；所有 owner 接受同一固定 batch 后才推进 feed cursor。可失败的 raw 回收在主 row ACK 前完成，ACK 成功后才释放 pending input；load 要求的强制发布边界在失败后保留并由下一次 pump 重试。其 field map 仅由 schema reopen 批量加入；注册与 pin 通过 set mutex 协调，取得的 pin 可独立存活。
- 旧 FieldIndexing/IndexingRecord 生产 owner 已移除。SearchOnGrowing、SearchOnIndex 与 SegmentGrowingImpl 的 search/value/deferred iterator 已使用固定前缀 pin；这些接线仍需全量构建和 e2e 验证，不能从静态实现推断运行通过。
- Expr 的 raw-data chunk size 不等于 growing 索引 bitmap 的大小。启用 growing scalar 前必须修正边界，不能沿用 size_per_chunk_ 截取 segment-global bitmap。
- 当前 growing 资源记账可能使用未刷新的缓存 ByteSize。发布/旧 pin 共存的内存需完整记账，不能仅报告当前 reader。
- text match 可允许延迟；普通谓词默认列扫描补齐。Ngram/JsonFlat 的产品策略尚待确认，不能把该策略位放进 index capabilities。

当前尚未完成全量构建和基础 e2e；静态接线不等于向量前缀约束、Add/Search 并发安全、完整行覆盖或生产查询已经验证。
