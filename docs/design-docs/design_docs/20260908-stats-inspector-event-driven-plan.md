# StatsInspector 事件驱动改造计划

状态：已在隔离分支实现，默认仍为 poll；本地验证及限制见末尾实施记录。日期：2026-09-08。

以下第 1–10 节保留原计划的阶段要求；已实现内容、实测结果和未通过的验证见末尾“实施记录”，不将原计划中的验收目标视为已达成。

实施基于 `codex/load-1m-segments-pr-stack-rebased-qv-work`。初始实现与本地测试使用 `e99fe0e5e1f31fc8c84978fea98180b84a0b1f81`；提交前对齐远端 squash 后的 `7e32a5ca9b7e472f1563668e497595a1f57b0d7d`，两者的 DataCoord 源码一致。本次不包含部署或线上性能收益声明。

## 1. 结论和范围

建议先把 **StatsInspector 的任务发现改为事件驱动，保留低频、有界的补偿扫描**；IndexInspector 已有事件机制，作为后续独立改动补齐覆盖、去掉溢出时的同步全量扫描。

这里的事件驱动是：元数据发生相关变化时，记录“哪个 segment / collection 需要重新检查”；后台读取最新元数据，决定是否需要创建任务。事件只负责唤醒，不携带完整元数据，也不直接代表一条必须执行的任务。

第一阶段只处理 TextIndexJob 和 JsonKeyIndexJob 的发现、提交及补偿，不改变它们的业务资格条件、持久化任务格式和 worker 执行协议。

不在本次范围内：

- 将所有 inspector、CompactionTriggerManager、TTL/超时检查和任务清理统一改成事件驱动。时间到期本身仍需定时器。
- 改动 Load/Search、QueryView listener 或 `discoverableShards` 的语义。
- 新增 WAL 消息、持久化事件队列、逐 segment 的 etcd watch 或线上全量 etcd 扫描。
- 改动 DDL broadcast 的 ACK、回放、提交和重试语义。
- 改动 QuotaCenter、PChannel affinity、Proxy SetRates，以及 SegmentInfo 冷热分层或 tombstone 清理。
- 调整线上 GOGC、配置、实例规模或进行部署；本文只是修改计划。

对于“每次请求 load 新 shard 再 search”的模式，本改造仍可能减少后台固定开销，但 **Load/Search 本身不等于 stats 构建资格发生变化**。应监听 flush、sort/compaction、schema、manifest 等元数据变化，不能把每次 load 变成一次 stats 扫描。

## 2. 为什么优先改 StatsInspector

### 2.1 可复现的分配路径

旧发现循环按周期遍历 collection/schema/segment，即使没有相关元数据变化，也会创建 collection/candidate slice 并重复解析字段参数。旧字段 helper 为参数创建 map，且缺失 `enable_match` 时构造错误；这些都是本次可以直接从代码和微基准验证的分配来源。

本 PR 只公开可复现的本地 benchmark 和验证边界，不附带运行实例标识、运维采样数据或本机 pprof 文件。具体分配结果见末尾实施记录；字段 helper 去分配与事件发现的收益有重叠，需分别衡量。

分配字节下降不能直接换算成 GC CPU 或 search p99 降幅。线上验收仍需分别观察 allocation assist、dedicated/fractional mark 和 idle mark，并使用同负载 A/B。

### 2.2 当前代码行为

| 位置 | 已核实的现状 | 对计划的影响 |
|---|---|---|
| [stats_inspector.go](../../../internal/datacoord/stats_inspector.go) 的 `triggerStatsTaskLoop` | 按 `TaskCheckInterval` 周期调用任务发现，Text/JSON 轮换先后顺序 | 替换正常发现方式，保留两类任务的公平性 |
| 同文件的 Text/JSON trigger | 分别 `GetCollections()`，检查 schema，再按 collection `SelectSegments()` | 不应只把 ticker 换成事件后继续扫全库 |
| 同文件的 `SubmitStatsTask` | 已存在任务、外部 collection 不满足条件、提交限流等分支都可能返回 `nil` | `nil` 不能作为“事件已完成”的唯一判断 |
| [stats_task_meta.go](../../../internal/datacoord/stats_task_meta.go) | `HasStatsTask` 包含 Finished/Failed，直到持久化任务清理后才释放二级索引 | 必须覆盖任务清理后重新检查的触发点 |
| [index_inspector.go](../../../internal/datacoord/index_inspector.go) | 已有 segment 事件、collection 事件和 pending 去重；全量补偿周期为 `TaskCheckInterval × 10` | 不是从零重写；后续处理补偿成本与覆盖缺口 |
| [go_channel_singleton.go](../../../internal/datacoord/go_channel_singleton.go) | segment index 通知溢出会唤醒一次全量补偿 | 新 Stats 队列不能照搬“溢出立即全扫” |
| [compaction_trigger_v2.go](../../../internal/datacoord/compaction_trigger_v2.go) | 消费现有 `statsTaskCh`，触发的是 sort compaction | 不复用该 channel 作为 StatsInspector 的 Text/JSON 队列 |

当前参数定义中 `dataCoord.taskCheckInterval` 默认 60 秒，`dataCoord.statsTaskPendingLimit` 默认 100；这是代码默认值，不是本次线上有效值的声明。不要通过增大共享 `TaskCheckInterval` 一并改变其他调度循环。

## 3. 目标和必须保持的不变量

目标：稳定、无相关元数据变化时，不再高频反复遍历全部 collection/schema/segment；正常发现成本主要随实际变化量增长。全量补偿仍有总量成本，但摊到受控的低频扫描中。

必须保持：

1. 当前 Text/JSON 资格条件不变，包括 Flushing/Flushed、非 L0、排序/namespace 排序、external 例外、字段配置、JSON 格式版本，以及 external JSON 的 Storage V3/manifest 条件。
2. `common.enabledJSONShredding` 和旧 `dataCoord.jsonShreddingTriggerCount=0` 的禁用语义不变；BM25 当前未启用，不顺带开启。
3. 先成功持久化并发布元数据，再发本地通知；通知失败或进程退出不回滚已经提交的业务操作。
4. 事件允许重复、合并、乱序和进程内丢失；最终正确性来自最新元数据、已有持久化任务和补偿检查。
5. 同一个 `(segmentID, subJobType)` 不因并发发现产生重复有效任务；不得把队列去重误当成持久化层的唯一性保证。
6. 提交准入仍按 Stats 类型计数，包含现有 scheduler 的 backoff 任务；不扩大预算、不改变现有阈值比较语义。
7. worker 执行失败由现有 GlobalScheduler 重试；发现阶段的暂缓由新的 pending 机制处理，两者不重复创建任务。
8. 队列、重试状态、collection dirty 集合、schema 缓存都必须有上限；不能用无界 ID set 代替原来的短命 slice。
9. 保留现有元数据 revision/CAS/tombstone 保护；本地事件序号不是持久化 revision，更不是 WAL TimeTick。

## 4. 事件从哪里来

### 4.1 接入边界

优先在 DataCoord **成功发布元数据后的公共出口** 接入实例级通知接口，业务入口用于核对覆盖，不逐处复制完整资格判断。

- segment：审计 `meta.UpdateSegmentsInfo` 及其他直接提交并更新 segment cache 的路径。事务成功、该批 cache 发布完成后，通知可能影响资格的 segment ID。
- collection：在 DataCoord schema/cache 成功更新后标记 collection dirty。只读取最新 schema；不让 RootCoord 等待 stats 任务完成。
- task：持久化任务清理且二级索引移除后，通知对应 segment/subjob 重新检查。
- config：使用现有 ParamTable `Watch/Unwatch`，回调只更新本地规则 epoch 并标记补偿；不在配置回调中扫描全库。

不在 CAS 重试用的 mutation closure 内发事件，也不在通用 `Cache.Insert` 中嵌入 Stats 业务。恢复、过期 revision、批量发布和业务提交的语义不同，不能因底层 cache 写入就假定“新任务已经具备条件”。

通知只需固定大小的 ID、原因枚举/位图、可选版本。不要 clone SegmentInfo、schema、binlog 或 manifest 内容进队列。多次相同状态更新合并；不相关的 heartbeat/统计数更新不应制造反复检查。

### 4.2 生产者审计清单

下表列出已定位的代码入口和实施时必须补齐的链路；**不是声称所有资格修改点已完成端到端审计**。关闭高频旧扫描前，必须逐项追踪到真实持久化和 cache 发布点。

| 变化 | 已定位的入口 | 计划动作及核查点 |
|---|---|---|
| Flush 完成 | `server.go` 的 `flushFlushingSegment`、`postFlush` | 从真实状态提交处触发检查；不能仅依赖会分流到 sort compaction 的旧通知 |
| Sort/mix compaction 完成 | `compaction_task_mix.go`、`task_stats.go` | 新 segment 元数据可读后通知；同时覆盖旧 sort stats 路径和现行 sort compaction 路径 |
| Clustering/schema bump | `compaction_task_clustering.go`、`compaction_task_bump_schema_version.go` | 覆盖临时 segment、最终结果以及原 ID 原地更新；保持可见性和排序条件 |
| Import | `import_checker.go`、`import_util.go`、`ddl_callbacks_import.go` | 审计 segment 创建、排序完成、import commit/abort；不要凭通知名称猜测 eligibility，也不改变两阶段提交可见性 |
| Manifest/外部数据刷新 | `ddl_callbacks_batch_update_manifest.go`、`task_refresh_external_collection.go` | 在实际元数据更新后通知；重复 manifest 不应不断创建任务；新增、更新、删除均需覆盖 |
| Schema/collection cache 变化 | `services.go:BroadcastAlteredCollection`、`meta.go:AddCollection`、`server.go:loadCollectionFromRootCoord` | 新 schema 可见后标记 collection；RootCoord 先推 schema 再发布绑定 index 的既有顺序保持不变 |
| Schema bump 的延迟完成 | `meta.go` 的 bump-schema compaction mutation | schema 先更新但 segment 尚不满足条件时，后续 segment 变化必须再次唤醒 |
| 任务结束与清理 | `task_stats.go`、`stats_task_meta.go:DropStatsTask` | 结果写入后检查后续需求；Finished/Failed 仍占二级索引时不能新建，清理成功后再唤醒 |
| 功能开关、格式升级 | `pkg/util/paramtable/component_param.go`、`common.JSONStatsDataFormatVersion` | 热更新启用时启动分批补偿；编译期格式升级由启动补偿覆盖 |
| Drop/truncate/restore | `meta.go`、`handler.go:FinishDropChannel`、`snapshot_manager.go` 及相关 DDL 路径 | 删除后旧事件不复活对象；snapshot restore 的 `AddSegment` 路径不能遗漏；collection 缓存缺失不能一律当成永久删除 |
| 启动和 leader 切换 | `server.go`、`statsInspector.Start/reloadFromMeta` | 恢复已有任务并进行一次有界发现，修补上个进程未送达的通知 |

源头审计至少搜索：`SetSegment`、`AddSegment`、`UpdateSegmentsInfo`、状态/排序/schema/manifest/stats 更新、`AddCollection/DropCollection`、任务二级索引增删，而不只搜索旧的 `notifySegmentIndexBuild`。

## 5. 消费、去重、重试设计

### 5.1 实例级队列

新增 Stats 专用的 pending 管理器，由 DataCoord Server/StatsInspector 的生命周期持有；第一版不抽象为全系统 EventBus，不新增全局 singleton。

工作分两类：

- segment 工作：key 为 `(segmentID, subJobType)`；只保存 generation、最早 dirty 时间、原因、重试时间等轻量状态。
- collection 工作：保存 collection ID 和扫描进度，用于 schema 改动；按批枚举 segment，不能一次展开成全量 segment 事件。

待处理、处理中和延迟重试的 key 共享总容量预算，避免出队后重试容器无限增长。同 key 一次只允许一个消费者执行；大 collection 不能一次占满全部执行预算。Text 和 JSON 使用轮转/配额，至少保留现有交替机制的无饥饿性质。

唤醒 channel 可以只有一个合并信号：先记录 dirty，再尝试非阻塞唤醒。信号不是事件存储本身，合并唤醒不等于丢掉 dirty 状态。

### 5.2 防止“处理期间来的新事件被清掉”

处理 key 时记录 generation；执行完成后，只能确认这一代的检查。若处理中 generation 又增加，则保留 pending 再检查。不得 `pop` 后无条件删除同 key 的新状态。

所有资格判断读取当前元数据。旧事件可以触发多一次无害检查，但不能把旧 schema、旧 manifest 或已删除 segment 写回 cache。新旧 Inspector 切换时，旧实例的回调必须失效。

### 5.3 明确提交结果

从当前 `SubmitStatsTask` 抽取内部可区分结果的提交函数，兼容现有 `error` 接口；结果名称以下为拟定值：

| 结果 | 含义 | pending 处理 |
|---|---|---|
| `Submitted` | 任务元数据已持久化，已交给 scheduler | 确认当前 generation；并发新事件仍保留 |
| `Existing` | 同 segment/subjob 的任务已存在，包括待清理的终态任务 | 不创建副本；任务结果/清理通知负责后续检查，补偿兜底 |
| `NotNeeded` | 最新状态当前不需要构建，或对象已确认删除 | 确认当前 generation；未来资格变化重新唤醒 |
| `Deferred` | 提交容量不足、依赖暂未就绪、collection 信息暂不可用等 | 保留 key，延迟重试，不依赖另一次业务事件 |
| 返回错误 | ID 分配、文件资源查询、任务持久化等失败 | 按现有错误契约处理，必要时退避重试并记录原因 |

元数据 cache miss 与确认删除必须区分。文件资源仍按当前 schema 和 ref-mode 语义获取，不能因为改成单 segment 处理就省略资源依赖。

当前 `AddStatsTask` 的锁以 taskID 为 key，而重复判定使用 segment/subjob。不能仅凭该锁宣称并发唯一性已经保证。第一版发现提交保持串行，审计所有调用者；若引入多提交者，必须先补齐同 segment/subjob 的串行化，并覆盖“检查—分配 ID—持久化—发布”的整个区间。

### 5.4 限流与失败重试

- 准入不足时，以延迟队列重试已知 key；退避有上限和抖动，禁止立即 requeue + 立即唤醒形成忙循环。
- 第一版不为了唤醒加入新的 GlobalScheduler 接口；可用本地最早重试定时器。后续若增加容量释放通知，仍需保留定时重试以免丢唤醒。
- scheduler 已接管的任务不重新分配 taskID，不复制 worker 失败重试状态。
- schema 在旧任务执行期间变化时，不覆盖旧任务；保留新的检查需求，结果发布和旧任务清理后按最新资格判定是否补建。
- 禁用 JSON 后停止新增 JSON 任务；不顺带取消此前已接管的任务。再次启用必须主动补偿，不能等待下次 flush。

### 5.5 控制 schema 解析成本

保留当前未提交的无 map 字段检查优化，先按本次处理批次复用字段 ID 列表。暂不为所有 collection 新建一份常驻 schema 索引。

若 profile 仍显示事件批次内反复解析显著，再增加有界字段判定缓存。key 至少覆盖 schema 版本/规则 epoch；external、文件资源模式和 JSON 开关等依赖也需正确失效。缓存只存判定所需信息，不持有完整 schema；命中只是优化，不参与正确性保证。

## 6. 补偿扫描与生命周期

### 6.1 为什么不能完全删除扫描

进程可能在“元数据提交成功”和“记录本地事件”之间退出；通知有容量上限；历史数据和编译期 JSON 格式升级也没有新的业务事件。因此本方案保证的是 **事件优先 + 元数据最终收敛**，不是 exactly-once 事件投递。

三类补偿共用预算：启动补偿、溢出/规则变更触发的补偿、低频定时补偿。

### 6.2 扫描必须真正有界

当前 `GetCollections()` 会物化 collection slice；`CachedSegmentsInfo.GetSegmentsBySelector()` 会先生成 candidates，再生成结果；`ConcurrentMap` 基于 `sync.Map`，没有现成的稳定分页 cursor。

因此不能把全量 `GetSegments()/Values()/Keys()` 的结果每次取 100 条就称为有界扫描，也不能反复 `Range` 前 N 项冒充分页。

推荐先实现 **单次遍历、分批输出的流式扫描**：一个扫描者依次产出小批 ID，消费者预算不足时暂停推进；不持有业务元数据锁跨批等待，不在内存积累全量候选，不为本改造新增一套每 segment 的常驻索引。collection 扩展同样遵守该约束。

实现前必须验证所用遍历器的并发、取消和暂停行为；`sync.Map.Range` 不是一致性快照，不能宣称具有稳定分页或严格扫描耗时上界。新插入/删除由事件与下一轮补偿收敛；如现有遍历方式无法满足锁等待和内存预算，再单独评审更换迭代方式，不暗中引入全量快照缓存。

批大小、每秒扫描量和每次处理时间预算必须同时约束“检查过的条目”与“提交的任务”，不能只限制成功提交数量。扫描不因新的全局 dirty 标志不断从头重启；先完成当前轮，再处理新的补偿代数，防止后半部分长期饿死。

### 6.3 队列满

segment key 无法入队时，非阻塞地将 collection 标记为需补偿；collection dirty 集合也满时，提升为合并的全局补偿 generation。生产者不能等待慢扫描、ID 分配或任务提交。

补偿 generation 与普通 key 一样按代确认：本轮扫描期间再次溢出，完成时不能清掉新补偿需求。溢出只安排有界扫描，不在事件回调中同步全扫。

在元数据规模有限、扫描持续推进且任务依赖恢复的前提下，漏通知的合格任务应能最终被发现；下游持续饱和时不承诺固定完成时延。须通过扫描完成时间和 oldest-dirty-age 检测无法收敛的状态。

### 6.4 启停顺序

1. 元数据基础设施就绪后，安装实例级 dirty 收集器，再开放相关正常写入路径，避免扫描与事件订阅之间的空窗。
2. 恢复已持久化的 Init/Retry/InProgress 任务，仍由原 scheduler 接管。
3. 启动消费者并安排一轮有界补偿；启动补偿与正常事件合并去重，不同时启动多个全库扫描。
4. Stop 时禁用/解除回调，取消扫描、等待工作 goroutine 退出；持久化任务继续作为下次恢复依据。

配置监听使用独立 handler 标识并 `Unwatch`；不能通过覆盖 ParamItem 的单 callback 影响其他模块。测试须覆盖同进程多次创建/停止 Inspector，确保无旧回调和 goroutine 泄漏。

## 7. 分阶段修改与文件范围

| 阶段 | 主要工作与拟修改文件 | 进入下一阶段的条件 |
|---|---|---|
| P0：基线与源头审计 | 对 `stats_inspector.go`、`stats_task_meta.go`、`meta.go` 及第 4 节入口建立资格/通知矩阵；补行为测试和 benchmark | 所有资格构造/重写点分类；已有补丁与本改造基线分开；确定队列、扫描预算和发现时延目标 |
| P1：局部 reconcile 和有界基础设施 | 抽取单 segment 判定/提交结果；拟新增 `stats_reconcile_queue.go`；在 `meta.go` / `segment_info_cache.go` 附近提供有界遍历适配 | 去重、处理中再变更、准入不足、取消、扫描内存和公平性测试通过；旧发现模式仍为默认 |
| P2：事件接线与影子验证 | 元数据发布后、collection schema/cache、任务清理和配置变更接入通知；`server.go` 管理生命周期 | 第 4 节所有路径逐项端到端验证；重复/丢通知/重启/rollback 测试通过 |
| P3：切换正常发现路径 | `stats_inspector.go` 从高频全量发现切到事件/pending；增加独立补偿周期和观测；参数定义在 `pkg/util/paramtable/component_param.go`，配置说明在 `configs/milvus.yaml` | 正确性门禁通过，A/B 显示分配和非 idle GC 成本改善，无任务遗漏/饥饿/明显时延回退 |
| P4：IndexInspector 独立后续 | `index_inspector.go`、`go_channel_singleton.go`、`ddl_callbacks_create_index.go` 及必要元数据入口 | Stats 结果先验收；重新 profile 确认 Index 仍值得投入，再复用已验证的有界机制 |

上述新增文件名和配置名均为计划，不代表当前已经存在。各阶段单独评审；不把现有其他性能补丁顺手提交，不改公共 proto、生成文件或 CI 流程。

Index 后续的具体目标：补齐 collection CreateIndex 通知满时的补偿（当前 `default` 会丢通知）、把 overflow 和定时全量扫描改成有界过程、限制 `pendingIndexSegs` 及一次 pop 的规模。保留当前按 segment/collection 触发和任务语义，不另起一套执行 scheduler。

## 8. 测试与验收

### 8.1 正确性测试矩阵

| 场景 | 必须验证的结果 |
|---|---|
| 正常 flush → sort → stats | 每个阶段按最新资格决定，任务结果与旧发现逻辑一致 |
| 同 key 重复通知、乱序通知 | 不创建重复有效任务；旧状态不覆盖新状态 |
| 消费期间再次修改 schema/manifest | 新 generation 不被旧消费完成清掉；最终处理新需求 |
| segment 和 collection 队列同时满 | 生产者及时返回；补偿推进，且历史尾部条目不饿死 |
| Stats 提交队列满，此后无业务事件 | 容量恢复后仍提交；不能因为此前返回 `nil` 永久漏任务 |
| 文件资源/ID 分配/任务持久化失败 | 未持久化的任务不会被确认已提交；依赖恢复后按原错误契约重试 |
| worker 失败/backoff | 由现有 scheduler 重试同一任务，不创建任务风暴 |
| 旧任务 Finished/Failed 尚未清理 | 不重复提交；清理成功后可以发现仍缺少的 stats |
| JSON 关闭 → 开启、旧 kill switch、格式升级 | 关闭语义不变；开启/升级主动补偿全部适用历史数据 |
| external V2/V3、manifest 缺失或更新 | 当前支持范围不变；状态变为合格后能重新检查 |
| Import commit/abort、snapshot restore、schema bump | 覆盖真实元数据来源，且不改变现有可见性/资格规则 |
| Drop/truncate 与旧事件、旧 revision 并发 | 不复活已删除资源，不绕过 tombstone；临时 cache miss 不误判删除 |
| 提交成功后、发事件前 crash；任务持久化后、enqueue 前 crash | 重启通过元数据和任务恢复补齐，无永久遗漏 |
| 多 collection、Text/JSON 大小 backlog 混合 | 有界推进，不被单一 collection 或 subjob 长期独占 |
| 多次 Start/Stop、补偿中取消、配置回调与停止并发 | 无泄漏、无死锁、无旧实例继续提交 |

Go 行为测试按仓库要求执行，以下是实施阶段的命令，本轮未执行也不代表已有新测试：

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/datacoord/...
go test -race -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/datacoord/... -run 'Stats|Reconcile'
```

若触及共享 ParamTable、field helper、cache 或 scheduler，补跑对应包及相关依赖测试；`pkg` 有独立 go.mod，需要在该模块执行。涉及错误分类/封装时，实施前另读 error handling guide/casebook，不在本计划中顺带改变错误码。

### 8.2 性能验证

至少对比三组：部署旧逻辑、仅字段去分配补丁、字段补丁加事件发现。不能将前两组的差额算作事件改造收益。

固定数据规模、schema 分布、Go/构建方式、GOGC、CPU 配额和负载，覆盖无变化、多次 load+search、持续 flush/compaction、schema 批量变化、冷启动和容量饱和场景。性能测试使用一致的生产构建，不能把关闭优化的 mockey 测试构建与生产 profile 直接比较。

检查：

- 分配 bytes/s、objects/s，以及 Stats/Index 路径分配；重复窗口必须覆盖完整补偿周期。
- GC assist、dedicated/fractional mark、idle mark 分开看，并用 CPU 秒/墙钟秒比较绝对成本，不只看火焰图占比。
- 稳态和补偿中的 RSS/Go heap、pending 上限、schema 缓存占用及扫描临时内存。
- 从“元数据已可读且具备资格”到“任务持久化”的 discovery delay，以及任务最终完成时间；两者分开。
- 队列溢出、最老 dirty 年龄、完整补偿一轮的时长、补偿发现的遗漏任务、两类 subjob 和 collection 公平性。
- 在可比 load+search 压测中检查查询 p95/p99；不根据后台分配下降直接宣称搜索 latency 已改善。

硬性验收：静态数据稳定后，普通唤醒/重试不触发全库枚举；队列和缓存不随运行时间无界增长；故障恢复后无永久遗漏；补偿确实分批推进。分配降幅和发现时延的数值目标在 P0 依据补丁后基线与产品 SLO 确定，不能先承诺“GC 降低一半”。

### 8.3 最小观测集

复用 `milvus_datacoord_task_count`、`milvus_datacoord_task_num_in_scheduler` 观察执行任务。拟新增发现侧的 pending 数、oldest dirty age、处理结果计数、deferred/overflow 计数、补偿检查数和轮次耗时。

新增标签只使用有界的 subjob、原因、结果、扫描类型，不加 collectionID、segmentID、schema version 等无界标签；scrape 不触发全量扫描。日志使用 `mlog`、真实 context 和限频输出，不记录每个重复事件的完整元数据。

## 9. 灰度与回滚

建议拟新增启动级模式 `dataCoord.statsInspector.discoveryMode = poll | shadow | event`，默认 `poll`。补偿周期独立配置，不改变共享 `TaskCheckInterval`。具体容量/批量默认值在 P0 测量后确定，参数非法值在启动阶段拒绝。

- `poll`：保留现有正常发现；新机制不得增加后台全库扫描。
- `shadow`：旧路径提交任务，新路径只做只读资格检查/覆盖抽样，不分配任务 ID、不持久化和 enqueue。该模式有额外开销，只用于受控验证。
- `event`：新路径提交任务，关闭旧的高频全量发现，保留有界补偿。

影子差异应基于同一元数据版本或可重放测试快照比较。实时并发下，任务已被另一条路径提交、schema 更新等导致的瞬时差异不能直接判为遗漏；若无法取得一致输入，只能作为诊断证据，不能作为正确性证明。

先在测试环境做事件丢失/重启故障注入，再在授权的实例灰度。出现任务长期未发现、补偿无法收敛、队列/RSS 无界增长或时延回退即停止扩大范围，切回 `poll` 并重启组件。复用原任务格式，回滚从原元数据恢复；不需要删除任务、回退 schema 或重写 WAL。部署与重启需要另行授权。

## 10. 开始编码前的决策门槛

1. 完成第 4 节所有资格变化源头和提交出口审计，确认没有只发 index 通知而漏 stats 的路径。
2. 明确队列总容量、扫描吞吐预算、最大补偿延迟和发现时延 SLO；这些是负载相关参数，不凭默认值推断。
3. 验证有界流式遍历的真实内存/锁行为；若要新增常驻全量索引，必须另行评审内存代价。
4. 确认无新字段/功能被这次改造启用，schema/任务生命周期的资格判定与旧路径一致。

建议先实施 P0/P1，通过后再接入全部事件。未满足源头审计与故障路径验证前，不删除旧扫描，也不宣称改造已经解决 GC 或搜索延迟问题。

## 附录：证据与参考

- 可复现的分配基准：[Stats 发现 benchmark](../../../internal/datacoord/stats_discovery_benchmark_test.go)、[字段 helper benchmark](../../../pkg/util/typeutil/field_match_test.go)。
- 关键实现：[segment cache](../../../internal/datacoord/segment_info_cache.go)、[versioned cache](../../../internal/datacoord/write_through_cache.go)、[GlobalScheduler](../../../internal/datacoord/task/global_scheduler.go)、[字段 helper](../../../pkg/util/typeutil/field_schema.go)、[参数定义](../../../pkg/util/paramtable/component_param.go)。
- 约束参考：[Streaming System](../../../docs/agent_guides/streaming-system/streaming-system.md)、[Broadcaster](../../../docs/agent_guides/streaming-system/coordination/broadcaster.md)、[Collection 消息语义](../../../docs/agent_guides/streaming-system/message/message-semantic-collection.md)、[Observability](../../../docs/agent_guides/observability/README.md)。本文不改变这些文档定义的协议边界。

本地运维采样证据不随 PR 上传；本文的仓库链接均为相对路径。

## 实施记录（2026-09-08）

实施基线：`codex/load-1m-segments-pr-stack-rebased-qv-work`，`e99fe0e5e1f31fc8c84978fea98180b84a0b1f81`。
提交基线：`7e32a5ca9b7e472f1563668e497595a1f57b0d7d`。工作分支：`codex/stats-inspector-event-driven`。原工作目录未修改；其他已回退补丁未恢复。

### 实际结构

- `stats_reconcile_queue.go`：实例级有界 dirty 状态；Text/JSON 各一个最早重试堆，在两者都 ready 时轮换；同 key 去重，完成时按 generation 确认。
- `stats_reconcile.go`：单 segment 最新状态检查、明确提交结果、批内字段/文件资源复用、配置订阅、流式补偿。
- `stats_discovery_meta.go`：只对资格/依赖字段变化发通知，忽略行数等无关更新；不把完整元数据装入队列。
- 元数据通知均在成功提交并发布 cache 后发生；事件是提示，消费者重新读取 cache，沿用原有 revision/tombstone 保护。
- 单次持续遍历通过 `iter.Pull2` 暂停，不调用全量 `Values/GetSegments` 生成快照。全库扫描直接访问原 cache 的 entries，连 tombstone 都按条计入预算，避免在底层过滤中隐含无界遍历。没有新增全量 segment 索引。
- 元数据依赖 RPC 可以超过本地 10 ms 处理时间片，但有 context 取消/超时；不承诺单个时间片的严格墙钟上界。
- 只更改 Text/JSON 任务发现；已有 worker/scheduler 重试、任务回收、BM25 禁用、Sort compaction、Load/Search 协议保持原样。恢复时已有 Sort stats 任务仍由旧逻辑跳过，不新增 Sort stats 执行。

### 配置与内部预算

只新增两个公开、启动时生效的配置：

```yaml
dataCoord:
  statsInspector:
    discoveryMode: poll
    reconcileInterval: 600
```

`poll` 仍是默认：不创建事件队列/消费者/额外补扫；`shadow` 的新路径不分配任务 ID、不持久化、不 enqueue，旧路径继续提交；`event` 关闭原发现 ticker。
`reconcileInterval` 单位秒，范围 1–86400；模式非法值也在初始化时拒绝。

内部初始预算：4096 个 key（含处理中和延迟项），单 collection 最多占 1/8；128 个 collection 扫描范围加一个合并的全局范围；最多 4 个暂停的遍历器共享每 100 ms 128 条的扫描预算。每轮消费最多 64 个 key，10 ms 软时间片；发现重试为 1–30 秒有界退避和抖动。容量和批量是内部选项，不再暴露六个独立调参项。

这些是本地验证使用的保守预算，不是负载 SLO。扫描理想上限为 1280 条/s，百万条至少约 13 分钟，仍会受 tombstone、任务容量和依赖延迟影响。完整扫描不因周期到期而重启；扫描期间的实际变更保留下一代补偿需求。

### 通知源审计

| 原始入口/状态变化 | 实际发布出口 | 验证方式 |
|---|---|---|
| Flush 状态、manifest 回调、Import commit/abort 和结果、external refresh、stats 结果 | `SetState/UpdateSegment/UpdateSegmentsInfo` | 公共出口成功/失败/无关更新测试；审计业务调用链 |
| 导入新 segment、snapshot restore | `AddSegment` | 持久化失败不发通知、成功发通知；源码追踪 restore/import |
| Mix/Sort/Clustering/schema bump（含原 ID 原地更新） | `CompleteCompactionMutation` 公共包装层 | 四种 compaction 的真实内存持久化路径测试 |
| 删除、分区 drop、channel drop、truncate、批量 GC 删除 | 对应 `meta` 发布出口 | 删除后过期事件、不复活 tombstone、批量删除通知测试 |
| Schema/外部字段映射/文件资源 ID 变化 | `AddCollection`，仅 schema 有变化时安排 collection 补扫 | 新字段通知、资源查询期间 schema 替换测试 |
| 旧任务终态清理 | `DropStatsTask` 成功移除任务和二级索引之后 | 清理失败保留占位；成功后重新发现 |
| JSON 开关、旧 kill switch、文件资源模式 | ParamTable `Watch/Unwatch` | 开关主动补扫及旧实例回调解除测试 |
| 启动/重启/漏通知 | 初次有界补扫、原 `reloadFromMeta` | 漏通知、持久化任务恢复、不重新分配已有 taskID 测试 |

生产 `AddStatsTask` 调用者审计后只有 Inspector。提交 mutex 覆盖检查、ID 分配、持久化和 enqueue，不能把 taskID 锁误当作 segment/subjob 唯一性锁。
`CachedSegmentsInfo` 本地更新只涉及行数、position、allocation、compacting 等，不改变 Stats 资格；不接入这些高频更新。
collection cache miss 可重试，只有现有 handler 确认 CollectionNotFound 才结束检查。external 判定沿用 `typeutil.IsExternalCollection` 的外部字段映射语义，不能仅在测试中设置 ExternalSource 就当成 external collection。

### 已完成的本地验证与边界

- 新增队列、发现、资源失败、容量恢复、漏通知、schema/config、终态清理、取消、重复启停、并发提交、四类 compaction 测试；原 StatsInspector/StatsTaskMeta 一同验证。还覆盖任务已持久化但未 enqueue 就重启的恢复窗口，确认复用已有 taskID。
- 上述 focused 测试在提交基线 `7e32a5ca9` 上以 `-race -tags dynamic,test -gcflags='all=-N -l' -count=3` 连续通过，最后一轮耗时 16.039 秒。DataCoord 和涉及的 pkg 包增量 `golangci-lint --new-from-rev=7e32a5ca9` 均为 0 issues。
- 资格用同一份元数据对照旧 trigger：Flushing/Flushed、L0、sorted/namespace sorted、external V2/V3、当前 JSON 格式、Text/JSON，以及不新增 BM25/Sort。
- 配置默认值/非法值/启动级属性、字段 helper 等价性和零分配路径、指标注册测试。
- 全量 DataCoord 与共享包测试已尝试，并非全绿。未修改的基线复现了提交时间戳断言、DISKANN 大小断言、external refresh/schema bump 测试缺少持久化对象导致的 panic、Meta reload 缺少 mock 预期、QueryView 配置断言及 Proxy 指标旧标签数导致的 panic。未顺手修改这些不相关测试或业务逻辑。
- 单独的本地 etcd/MinIO 用于回归；不部署、不变更线上配置。提交 PR 不代表完成上线验收。
- 提交前在独立验证工作树执行 `make lint-fix`，gofumpt/gci 已运行，根模块 typecheck 停在 `internal/metastore/kv/querycoord/kv_catalog_test.go:372: undefined: mocks`。该文件与提交基线的 blob 完全相同；本次不修改，pkg/client 后续 lint 阶段未执行。固定版本 gci 会将标准库 `iter` 错分到第三方组，因此保留已通过当前增量 lint 的标准库分组；未纳入其他自动格式化改动。
- `run_clang_format.sh` 使用 clang-format 15.0.7 完成；本 PR 无 C/C++ 文件变更。`git diff --check` 通过。

下面的微基准使用同一 Go 1.26.5、darwin/arm64、`-tags dynamic,test -gcflags='all=-N -l'`，不是生产优化构建，也不是线上 A/B：

| 场景 | 1000 segment | 100000 segment |
|---|---:|---:|
| 旧周期发现（已使用无 map 字段检查） | 51,744 B/次 | 10,624,400 B/次 |
| 事件模式无待处理变化的一次消费 | 0 B/次 | 0 B/次 |
| 创建流式游标、读取第一项并停止 | 236 B/次，7 次分配 | 232 B/次，7 次分配 |

重复 key 通知：0 B/次；缺失 enable_match 字段判断：旧 helper 617 B/次、12 次分配，新直接判断 0 B/次。
这些只验证局部成本不再随存量扫描放大；事件补扫、实际新任务仍有成本，不能解释成整个进程零分配，不能换算为 GC CPU 或 search p99 降幅。

### 尚待环境验证

- 未执行完整的 `make test-go`（包含重新生成 proto、重建 C++ 等前置步骤）；已尝试的 DataCoord/共享包全量 Go 测试本身也未全绿，不能将 focused/race 通过称为完整 CI 成功。
- 生产构建和同负载 A/B，覆盖完整补偿周期、持续 flush/compaction、大 schema 变更、容量饱和、多 collection；确认扫描可收敛及业务发现时延 SLO。
- 部署、启用 event、线上 CPU/alloc pprof 和 load+search p95/p99 对比需要另行授权。默认 poll 保留用于回滚。
