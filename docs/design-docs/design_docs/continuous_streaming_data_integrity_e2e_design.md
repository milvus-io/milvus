# 持续流式数据完整性 E2E 测试设计

## 1. 背景与定位

Phase 1 并非面向通用用户场景的 DataIntegrity Test，而是专门验证 compaction 生命周期的数据完整性测试，即 Compaction-DataIntegrity Test。它采用“写入一批数据、等待 compaction 完成、再检查数据”的批处理模型，核心证明的是数据经过 compaction rewrite 和 serving handoff 后仍然完整正确，而不是持续 mutation 条件下的通用数据完整性。

真实用户通常不会按批次停止数据进入并手工触发 compaction，而是通过 Insert、Upsert、Delete 或连续 Import 等方式持续导入和修改数据，同时由 Milvus 在后台自动完成 sync、seal、compaction 和 serving handoff。

Phase 2 因此在 Phase 1 已建立的测试体系上增加持续流式 cases，用于验证后台生命周期变化与前台 mixed ingress 长时间并行时的数据正确性；Phase 1 cases 继续保留，不做修改或替代。

## 2. 核心问题

Phase 2 只回答一个核心问题：当用户通过不同 ingress 方式持续导入和修改数据、Milvus 同时自动重写 segment 并切换 serving 数据集时，所有已经明确成功的数据是否仍能被完整、唯一、准确地读取。

测试必须独立维护预期数据，不能用查询结果反过来证明查询结果正确；所有计划内 mutation batch 与 import job 都必须明确完整成功，任一请求发生 partial failure、timeout、cancel、断连、返回数量不符或最终结果无法确认，都可能已经产生部分写入，因此必须立即终止整个 case、将该 collection 标记为不可继续判定，并禁止基于后续操作继续 replay 或给出通过结论。

## 3. 测试模型

持续数据由多个 ingress lane 并发产生，其中 `MutationIngress` lane 持续执行 Insert、重复 PK、Upsert、Delete 和 reinsert，`ImportIngress` lane 则连续提交并完成 import jobs；各 lane 内保持明确顺序，不同 lane 之间并发推进。

“数据前缀”不是 PK 排序的前 N 行，也不是某一时刻碰巧查询到的数据，而是由测试侧逻辑序列定义的稳定数据范围。测试为每个 action 分配全局唯一且单调递增的 logical sequence；Insert、Upsert、Reinsert 和 Import 将该值持久化为 row 的 `test_sequence_id`，Delete 不产生新 row，因此只在 oracle 与审计日志中保存同一序列域内的 operation sequence。Sequence 数值允许存在空洞，检查点边界 X 只要求 logical sequence 不大于 X 的所有已分配 action 都已经取得明确结果。不同 lane 使用互不重叠的 PK 范围，因此这些边界可以无歧义地合并成一份全局预期数据。

```text
时间 ---------------------------------------------------------------------->

MutationIngress A   Epoch-1✓ │ Epoch-2✓ │ Epoch-3✓ │ Epoch-4✓ ──┐
MutationIngress B   Epoch-1✓ │ Epoch-2✓ │ Epoch-3✓ │ Epoch-4✓ ──┤ Mixed ingress
ImportIngress C       Job-1✓ │   Job-2✓ │   Job-3✓ │   Job-4✓ ──┘
                              │          │          │              │
                            Prefix-1   Prefix-2   Prefix-3       Pause
                              │          │          │              │
Validator              全量验证 P1  全量验证 P2  全量验证 P3   全量验证最终数据集

Milvus          sync / seal ──> 自动 compaction ──> serving handoff
```

图中的 `Job-1✓` 表示该 import job 不仅执行完成，而且其数据已经持久化并可见；Prefix-1 对应测试侧选定的 logical sequence boundary X1。测试以该 boundary 而非墙上时间定义检查点，只纳入 X1 以内已经明确成功的 mutation rows、Delete actions，以及已经 completed、持久化且可见的 import rows。

Prefix-2 和 Prefix-3 依次纳入更多已确认 ingress，并验证逐步扩大的稳定前缀。检查点不暂停 producer，也不设置阻断 ingress 的硬 epoch barrier；logical sequence 达到计划 boundary 后异步启动验证，所有 lane 立即在新的 PK epoch 上继续写入，并要求 validation interval 内出现高于该 boundary 的 ingress 进展。Prefix-3 之后还必须继续执行 Epoch-4，最后才暂停 producer、等待系统稳定并验证完整数据集。

独立预期模型从初始状态开始，按 logical sequence replay 检查点 boundary X 内的全部已确认 action：Insert、Upsert 和 Reinsert 将对应 PK 更新为携带 canonical row 的 Live 状态，Delete 将对应 PK 更新为 Deleted 状态，Import 则把已经 completed、持久化且可见的 rows 合入相同状态模型。Replay 完成后的结果数据集才是该检查点的 `expected_data_set`，其中只包含最终状态为 Live 的 PK；Deleted PK 从 Live map 中移除，但保留在 tombstone/audit state 中用于验证不会重新可见。每个同类型 mutation request 或 import job 只有在完整成功且返回数量与计划完全一致后才能原子提交到 oracle，任何非 all-success action 都直接使整个 case 失败。

`test_sequence_id <= X` 只定义当前 MVCC view 的 prefix 查询域，不提供 point-in-time 或 time-travel snapshot；如果 suffix 再次修改 prefix PK，当前查询只会看到最新存活版本，旧版本无法由 sequence filter 重建。因此冻结 prefix 后，MutationIngress 必须切换到新的 PK epoch，ImportIngress 也必须继续使用新的不重叠 PK 范围，任何 suffix 操作都不能再次修改 prefix PK。各 lane 在新 PK epoch 上继续产生数据，因此 validator 可以全量检查稳定 prefix，同时 Milvus 仍承受真实 mixed ingress 和后台生命周期变化。

为了控制首个 case 的不确定性，不同 ingress lane 使用互不重叠的 PK 范围；跨 lane 竞争同一个 PK 属于后续增强，不进入首轮设计。

## 4. 两类数据检查点

**流式前缀检查点。** Ingress 不能停止；测试选定 logical sequence boundary X，以 Strong consistency 查询 `test_sequence_id <= X` 的当前数据，对照 replay 结果精确验证完整 Live PK 集合以及全部字段和全部 cell，同时根据 tombstone/audit state 确认 Deleted PK 不可见。各 lane 继续在新的 PK epoch 上执行 mutation 或 import，且验证开始至结束期间必须记录高于 X 的 ingress 进展，从而证明检查发生在真实 mixed-ingress 负载中。

**最终静默检查点。** 流式阶段结束后停止所有 ingress lane，等待全部在途 mutation 请求与 import job 返回明确结果，并等待 compaction 与 serving 状态稳定，然后对完整预期数据集做最终全量验证。

两类检查点都不进行数据抽样；区别仅在于前者验证“系统仍在变化时的数据正确性”，后者验证“最终稳定状态的数据正确性”。

三个 ingress lane 始终并发运行，但三个流式前缀检查点共享一个 validator，并按 Prefix-1、Prefix-2、Prefix-3 串行执行；每个检查点都将三条 lane 在各自 boundary 内的 action 合并 replay 为一个全局 `expected_data_set`，不按 lane 分别判定，也不并发执行多个全量扫描，以免验证流量本身改变被测负载。

这里的“唯一”仅指 serving view 中每个 PK 只有一个逻辑可见结果；测试不声称枚举或排除底层被 MVCC/PK 去重隐藏的物理版本，但必须证明 Milvus 最终选出的 visible winner 与 oracle 中该 PK 最后一次成功 mutation 完全一致，并且其全部字段逐 cell 正确。

流式前缀检查点需要在数据验证前后采集 segment blood lineage 快照，但不等待 lineage 收敛，也不要求前后快照保持不变，因为验证期间自动 compaction 可能仍在进行；该检查点的通过条件只由冻结数据前缀的逻辑正确性决定。

正在进行但尚未闭环的 lineage transition 在这里是允许的，但血缘自引用、环路或其他不可能成立的拓扑矛盾应立即判定为生命周期失败。

## 5. Compaction 与 Serving 的正确性基础

测试不能仅凭 compaction task 显示 Completed 就认定 compaction 生效，因为 task 状态只能证明调度过程结束，不能证明新数据已经接管查询流量。

完整 lifecycle 闭环由独立的“活跃写入期间自动 Compaction 检查点”负责验证，不能附加到流式前缀检查点上，否则持续写入验证会退化为等待系统静默的批处理验证。

Phase 2A 必须分别建立 MixCompaction 与 L0Compaction 的闭环，因为两者的物理转换语义不同，不能使用同一套 source→new-target 断言。

一次有效的 MixCompaction 必须同时表现为：source 与新 target 之间存在明确 lineage、source 退出 active/serving 数据集、target 成为 Flushed active segment，并且 target 已进入 serving segment set。

一次有效的 L0Compaction 必须覆盖非平凡 Delete 路径：测试先预留并确认一组 PK 已位于 Flushed 且 serving 的 L1/L2 segment，再删除这组 PK、记录由这些 Delete 形成的 L0 source IDs，并等待消费这些 source 的 Level0DeleteCompaction 完成且 L0 sources 退出健康 segment 集合并进入 Dropped/Compacted 状态；随后 frozen-prefix 验证必须确认该 PK 集合全部不可见、其余 Live PK 与全部字段逐 cell 正确。

L0 通常不会替换 target segment ID，因此不强制要求 source→new-target lineage、target manifest 变化或 serving frontier 更换；L0 task completed、目标 L0 sources retirement、Delete 可见性与保留数据完整性共同构成在线闭环。

现有 segment 状态接口能够枚举 Flushing、Flushed、Sealed 状态的 L0 segment，并通过 `Level=L0` 识别；测试必须在 GC retention window 内缓存 L0 source ID，并显式查询 Dropped 状态来证明 source retirement，不能依赖终态快照永久保留历史 L0 metadata。

至少一次 MixCompaction 闭环和一次 L0Compaction source-retirement 闭环必须在 mixed ingress 活跃期间完成，而且闭环后各 ingress lane 仍继续取得进展，否则该 case 仍然只是“先持续导入、再停止验证”的批处理测试。

release/load 不加入三个流式前缀检查点，因为它会中断连续 serving 并改变被测负载；最终静默阶段只执行一次 release/load，随后再次验证完整 expected dataset 与预留 Delete PK 集合，以证明 Delete 结果已经持久生效，而不是仅由内存中的在线 delete forwarding 暂时保证查询正确。

测试不调用 `compact()`，因为目标是验证用户无法直接控制的自动生命周期，而不是再次验证 Phase 1 的手工 compaction 模型。

## 6. 测试边界与 Phase 1 集成

Phase 2A 从第一版开始就支持 Mixed-ingress，基础 workload 固定使用两个 MutationIngress lane 与一个 ImportIngress lane 在同一个 collection 上并发运行，三个 lane 使用互不重叠的 PK 范围。

Phase 2A 不是另起一套测试体系，而是在 Phase 1 cases 基础上增加持续 mixed-ingress case，并增强已有框架对持续负载的表达能力；数据生成、独立预期状态、全量数据比较、compaction lineage、serving checkpoint 和审计证据继续沿用 Phase 1 已建立的方法。

Phase 2A cases 与 Phase 1 cases 严格串行地集成到同一个 L3 CI workflow，复用相同的部署、Milvus instance 和执行入口，不需要新建独立 workflow，也不要求在两个阶段之间重启或重新部署 instance。

Phase 1 完成后，workflow 使用已有 etcd helper 写入 Phase 2 所需的 hot-effective overrides，读回对应 revision 并等待 10 秒，再创建 Phase 2 collection；`dataCoord.segment.maxSize` 只影响配置更新后新分配的 segments，因此 collection 和 G0 segments 均不得早于该配置步骤创建。

运行时只修改当前消费路径能够动态采用且 Phase 2 确有需要的五项配置：`dataNode.segment.syncPeriod=10`（秒）、`dataCoord.segment.maxSize=64`（MiB）、`dataCoord.segment.maxLife=60`（秒）、`streaming.flush.l0.maxLifetime=30s` 和 `dataCoord.compaction.levelzero.forceTrigger.deltalogMinNum=4`；其中部分参数属于当前实现中的 hot-effective 行为而非稳定公开配置契约，因此 etcd 写入、读回及 10 秒等待只能证明 desired value 已传播，不能单独证明运行时消费者已经采用新值。

配置生效必须由 bounded runtime behavior 验收：测试应在时限内观察自然 sync/seal、活跃写入期间的 Mix lineage 和 L0 source retirement，并记录从配置写入到各证据首次出现的耗时；缺少任一必要行为时报告 `coverage_not_reached`，不得无限等待，也不得仅凭 etcd value 将配置判定为已生效。

首个 Phase 2 case 固定使用 Storage V3、单 partition 和单 shard，以隔离 mixed ingress、自动 compaction 和 serving handoff 这三个核心变量；该 case 不运行 V2 副本，也不执行 storage-version transition，因为 Phase 1 已独立覆盖 V2、V3 及 V2→V3→V2 转换。

首个 case 固定使用 Phase 1 普通 Compaction-DataIntegrity case 已验证的 V3 全类型 schema profile，并增加测试控制字段 `test_sequence_id`：VARCHAR PK、`test_sequence_id`、scalar、nullable/default、VARCHAR、JSON、Array、dynamic field、StructArray 及其五种 nested vector、nullable top-level vectors、sparse vector 和 TEXT/LOB；BM25 function 与 DDL 专用 top-level-vector profile 不合并进本 case，避免把新的 schema 变量和索引成本带入持续写入模型。

MutationIngress 与 ImportIngress 使用 Phase 1 相同的确定性 row generator、canonical encoding 和独立 oracle，因此相同逻辑 row 无论通过 mutation 还是 import 进入系统，都必须具有完全相同的 expected bytes；ImportIngress 可以转换物理文件表示，但不得把 nullable StructArray 的 `None` 改写为 `[]`，也不得把 nullable sparse-vector 的 `None` 改写为非空 fingerprint。TEXT 每 1,000 rows 生成一条超过 inline threshold 的 LOB 数据，既保留 LOB 路径覆盖，也使平均 row size 保持有界。

每个 MutationIngress lane 连续执行四个 PK epoch，每个 epoch 包含 3,000 个 row-level operations：2,200 个首次 Insert、400 个 Upsert、200 个 Delete、100 个对已删除 PK 的 Reinsert，以及 100 个对仍为 Live 的 PK 执行的后续普通 Insert。Insert、Upsert 和 Delete 分别通过单一操作类型的 request batch 提交，不能混入同一个 mutation RPC；epoch 是这些同类型 request batches 的计划混合序列，oracle 按全局 logical sequence replay，并将最后一次成功 mutation 的 canonical row 或 Deleted 状态作为该 PK 的最终状态。

ImportIngress lane 在四个 epoch 中各完成一个包含 3,000 rows 的 import job，因此基础 workload 共包含 24,000 个 mutation operations 和 12,000 个 imported rows，总计 36,000 次 ingress；当计划内操作全部成功时，三个流式检查点分别验证 7,200、14,400 和 21,600 个 Live PK，最终 `expected_data_set` 包含 28,800 个 Live PK。

MutationIngress 以每个同类型 request batch 100 rows、每两秒一个 batch 的速率运行，即每个 lane 每个 epoch 恰好执行 30 个同类型 RPC batch、约 50 row operations/s，并在约一分钟内完成；100 rows 不是 Milvus 正确性条件，而是用于固定 request 数量、产生速率、receipt 与失败边界并消除无意义尾批次。Logical sequence 达到 Epoch-1、Epoch-2 和 Epoch-3 的计划 boundary 后依次提交三个流式前缀验证任务，单一 validator 串行执行这些任务，producer 不等待验证完成便进入新的 PK epoch，Epoch-4 及每次 validation interval 内更高 sequence 的进展共同证明 ingress 始终继续。

ImportIngress 只在前一个 job 已完成、持久化且可见后提交下一个 job；持续 ingress 目标时长约四至六分钟，停止 ingress 后预计再用二至四分钟完成 drain、自动 lifecycle handoff 和最终全量验证，因此正常目标时长为八至十二分钟，完整 case 硬上限为二十分钟。

Phase 2 仅通过上述 helper 覆盖五项配置，其余依赖沿用并记录实例默认值：`dataNode.segment.insertBufSize=16777216`（16 MiB）、`dataCoord.segment.sealProportion=0.12`、`dataCoord.segment.sealProportionJitter=0.1`、`dataCoord.compaction.mix.triggerInterval=60`（秒）、`dataCoord.compaction.levelzero.triggerInterval=10`（秒）、`dataCoord.compaction.levelzero.forceTrigger.minSize=8388608`（8 MiB）、`dataCoord.enableCompaction=true`、`dataCoord.compaction.enableAutoCompaction=true` 与 `dataCoord.compaction.twoTierCompaction=false`；特别是不再尝试把 Mix trigger interval 动态改为 10 秒，因为该 ticker 在 compaction manager 启动时固化，而默认 60 秒在四至六分钟 ingress 窗口内仍提供多次自然调度机会。

单个 deltalog file 没有固定大小，其大小由该 flush 周期内的 Delete PK、timestamp、编码和压缩结果决定；本 workload 的低速 Delete 数据预期主要依靠 deltalog 数量而非 8 MiB size threshold 触发 L0Compaction，因此初始设计使用 `deltalogMinNum=4`，目标是在约两分钟内形成一次非平凡 L0 plan，同时避免 `deltalogMinNum=2` 导致过于频繁的 L0 与 Mix/Sort 互斥调度。

Schema 与 write settings 必须作为一组配置共同评审：该 V3 全类型 schema 的 logical row size 决定 size-based sync 和 seal 的实际触发速度，64MiB × 0.12 再考虑 jitter 后形成约 6.9–7.7MiB 的 segment seal threshold，而 10 秒 sync 与 60 秒 maxLife 分别提供周期持久化和时间兜底；若 schema、vector dim、LOB 频率、batch size 或写入速率发生变化，必须重新校准整组参数，不能只改单项。

该组合的目标不是承诺精确物理 segment 数量，而是让约四至六分钟的 mixed ingress 至少跨越多个 sync、seal、MixCompaction 和 L0Compaction 调度周期，并在 producer 活跃期间形成可观察的 Mix lineage 与 L0 source retirement；最终有效性仍由真实 segment/task 快照、至少一次独立于 import SortCompaction 的 MixCompaction 闭环、至少一次 L0Compaction 闭环、active/serving handoff、release/load 后的 delete visibility 和全量数据结果判定。

上述参数是 draft 阶段的初始设计值而非永久常量；draft 实现完成后必须通过原型运行记录实际 deltalog count/size、Mix/L0 task 时长、互斥等待和各验证阶段耗时，再在不降低全量验证强度的前提下校准参数并确认正常八至十二分钟、硬上限二十分钟的时长目标可达。

测试全程不调用 `flush()` 或 `compact()`；Import job 自身完成持久化不视为测试侧手工 flush，最终静默阶段也只停止 ingress 并等待自动 sync、seal、compaction 和 serving handoff。

数据类型继续沿用 Phase 1 已证明有效的全字段数据安全模型；资源占用通过上述固定数据量、速率、schema 和 lifecycle 配置控制，而不通过抽样或减少字段覆盖来降低验证强度。

测试必须设置明确的总时长和资源上限；如果时限内没有同时形成有效 MixCompaction 与 L0Compaction 闭环，应报告“覆盖未达到”，而不是无限等待或报告测试通过。

Storage version 动态切换、多 shard、跨 ingress lane 重复 PK、故障注入和组件重启均作为后续独立增强，不与首个 mixed-ingress case 同时引入；同一个 MutationIngress lane 内跨 batch 的重复 PK 已属于 Phase 2A 基础 workload。

## 7. 关键审计日志

测试必须输出稳定、机器可解析的结构化证据日志，使人工或 AI 能在不重新运行 case 的情况下重建 ingress、checkpoint、compaction 和 serving handoff 的完整时间线；日志以事件为单位记录摘要与完整 ID，不逐 cell 打印大体量数据。

关键观察节点包括：配置阶段记录五项 override 的 original/desired value、etcd revision、读回结果与 10 秒等待区间，并记录首次自然 sync/seal、Mix lineage 和 L0 source retirement 的时间；case 启动时记录 Storage V3、schema fingerprint、workload 参数、随机种子和各 lane PK 范围；每个 mutation request 与 import job 明确成功时记录 logical sequence 范围、job ID、row count 和 oracle commit；每次 prefix 冻结时记录 checkpoint boundary X、Live/Deleted 数量及 expected PK digest；每次全量验证完成时记录 validation start/end sequence、期间高于 X 的 ingress 进展、expected/actual PK digest、validated row/cell count、mismatch summary、验证前后 active/serving frontier 和耗时。

自动生命周期证据必须记录完整 task ID、task type/state、完整 source/target segment IDs、Mix lineage edges、L0 source states、segment storage version、active/serving frontier 及 handoff 结果；最终静默检查点还需记录 ingress drain、task quiet、active=serving、release/load 后验证结果、最终数据摘要，以及终态分类 `passed`、`ingress_failed`、`data_integrity_failed`、`lifecycle_failed`、`coverage_not_reached` 或 `indeterminate` 和对应原因。

审计日志本身不能替代正确性断言，但必须足以回答“哪些 action 进入 oracle、哪个 sequence boundary 被验证、验证期间 suffix 是否继续推进、哪次 MixCompaction 在活跃写入期间闭环、哪些 L0 sources 被退休、release/load 后 Delete 是否仍然生效、哪个 serving frontier 接管查询，以及失败影响了多少 row/cell”。

## 8. 结果判定

- **通过：** 所有计划内 ingress 全部成功、三次流式前缀验证、活跃写入期间的 MixCompaction 与 L0Compaction 闭环、serving handoff、release/load 后验证及最终完整数据验证全部成立。
- **Ingress 失败：** 任一 mutation batch 或 import job 出现 partial failure、返回数量不符、timeout、cancel、断连或最终结果无法确认，case 立即停止且该 collection 不再用于正确性判断。
- **数据完整性失败：** 出现数据缺失、意外数据、serving view 中重复可见 PK、visible winner 与 oracle 最后一次成功 mutation 不一致、已删除数据重新出现或任意字段值不一致。
- **生命周期失败：** Mix lineage、L0 source retirement、target adoption、segment 状态或 serving handoff 不符合预期。
- **覆盖未达到：** 在规定时间内没有同时观察到活跃写入期间完成的有效 MixCompaction 与 L0Compaction 闭环。
- **无法判定：** 环境受到外部干扰，或无法建立稳定可信的验证边界；Mutation 或 import job 本身不确定统一归入 Ingress 失败。

Phase 2A 的验收标准是：同一次有界测试运行中，所有 MutationIngress 与 ImportIngress 操作全部成功并持续并发推进、三次全量前缀验证、至少一次 mixed ingress 活跃期间完成的 MixCompaction 闭环、至少一次 L0Compaction source-retirement 闭环、serving handoff，以及 release/load 后的最终全量数据验证全部成立。
