# PK Inplace-update 实施计划

[English version](20261120-pk-inplace-update-implementation-plan.md)

在**一个 Milvus PR** 中实现[设计方案](20261109-pk-inplace-update-design.md)，按以下六个阶段拆分 commit。计划在 2026-11-20 对照 master `c31070a6cf` 审阅；生产 [PR #55742](https://github.com/milvus-io/milvus/pull/55742) 继续其已有的协议基础，基于 master `20746425a1`。Segcore 参考 rebase 后的 [Draft PR #55699](https://github.com/milvus-io/milvus/pull/55699)、commit `ccce0c06fe` 中的 feature patch，不复制其中无关的 master commits。该 draft 尚未合入 master，仅支持 Sealed segment 上的 INT64/VARCHAR POC，并非完整功能。

- 只使用 TS-vector＋Offset Dictionary＋updated-offset bitset；初始值保留在原生 base 存储中。
- 按下述编号顺序实施。所有阶段通过验收前，关闭新更新请求的入口。
- 每个阶段包含组件测试，同一 PR 包含 e2e 测试。仅在新 API 需要时修改外部 proto/SDK 仓库。
- 保留设计中已有的 schema/backfill 不支持范围、channel 级 L0/Mix 互斥和常规 Mix 调度。
- 复用 master 的事务、物化、TransformLog 和 QueryView 机制。本轮不加入额外的 TS-vector 性能实验、bitmap checkpoint cache 或其他索引结构；snapshot 正确性仍是必须满足的要求。

## 进入下一阶段前的 review

- 每次完成并 review 一个阶段，再进入下一阶段。Segcore 的 4.1–4.4 分别 review；阶段 5 接入运行时保护前，4.4 使用受控 GC 边界测试。
- 将实际 diff 与当前阶段的每条实施项逐一对照，记录遗漏和范围变化。修正非预期偏移；确有必要调整范围时，先确认调整再继续。
- 检查新增的数据结构、配置、cache、后台任务和状态转换。优先复用现有代码，删除实现当前行为不需要的部分。
- 运行并检查当前阶段的测试，覆盖适用的失败/重试、历史 snapshot 和边界情况；仅编译通过或成功路径通过，不代表这些检查完成。
- 提交简短的 review 摘要：已完成的计划项、改动文件、测试命令及结果、遗留问题。解决阻塞问题后，在同一 PR 中单独 commit 当前阶段，再进入下一阶段。

## 1. Proxy、请求和 WAL

代码入口：Proxy 请求处理、message types/builders、SN append 校验、CDC replication、paramtable 和 milvus.yaml。

master 已有 VChannel 路由和事务，但没有 PK-update request/message。复用 [PR #55742](https://github.com/milvus-io/milvus/pull/55742) 及配套 proto PR #700 的协议基础，后续 Milvus 阶段继续在同一个 feature PR 内提交。现有依赖读取的 DML auto-load/retry 链路，不是 literal PK-update 写入的前置条件。

### 代码改动

- 新增 PK-update RPC/request 和 WAL message。携带 collection/schema 标识、partition scope、`schemapb.IDs`，以及包含 `field_id` 和 typed literal 或显式 NULL 的 assignments。生成绑定代码并增加 SDK 调用。
- 对此 RPC 禁用 SDK 自动重试，包括网络错误和 schema mismatch。更新可能已提交但响应丢失，不能用新 timestamp 重放并覆盖后续写入者的值。发布前将临时 proto fork 依赖换成已合入的上游版本，并从下游 module 验证 SDK 编译。
- 在 paramtable 和 milvus.yaml 注册 `proxy.pkUpdate.enabled=false` 和 `proxy.pkUpdate.maxPkCount`；数量上限默认 10,000 个提交 PK；无效或非正数配置回退到 10,000。数量上限与字节上限独立生效。
- 在公开请求入口检查开关。关闭时拒绝新请求；已接受的更新继续参与回放、物化、CDC 和读取。
- 校验权限、PK 类型、scope、赋值类型、nullable 属性和受限字段。在去重、路由或 WAL append 前拒绝空 PK list 或超限请求，数量按提交的完整列表计算。
- 一次请求中每个字段只允许一份 assignment；入口拒绝重复 field ID。literal update 不应在 WAL append 前读取旧行或要求 collection 已加载。保留不同合法请求或事务 body 的有序写入。
- 独立检查请求字节数限制。输入 protobuf 和预构建 WAL envelopes 的总字节数都受 `proxy.grpc.serverMaxRecvSize` 限制；每个分片中重复编码的 assignments 都计入总量。按 VChannel 路由 PK，使用当前 WAL 后端的消息上限减去现有 envelope reserve，按完整编码大小拆分。RocksMQ 没有单条记录上限，使用请求预算。单个 PK 加共享赋值无法放入消息，或预构建总量超限时，必须在任何 append 之前拒绝。
- 使用现有事务机制提交每个 VChannel 的分片。消费端使用 CommitTxn TimeTick 并保留事务 body 顺序；维持现有跨 VChannel 原子性边界。
- 将 UPDATE 接入 SN schema-version 校验、消息分类和 CDC 转发。每次转换均保留 assignments 和源消息的回放标识。

### 测试

- 验证多 PK、多字段、显式 NULL、空值和 ARRAY 的编码与解码；拒绝不支持的字段和类型。
- 测试 PK 数量为 0、limit−1、limit、limit＋1，以及重复 PK 和多 VChannel 请求；被拒绝的请求不得 append。独立测试长 VARCHAR PK 和大 assignments。
- 测试 Kafka/Pulsar 配置不同的消息上限、envelope 预留空间、输入字节数和预构建分片总量的边界；所有预检查拒绝都必须保证零 append。
- 测试默认关闭、开关切换、重复 field ID、schema 不匹配、事务回滚和 CDC 回放。验证未加载 collection 的写入不会触发隐式 pre-read。关闭入口后，已接受的数据仍可读取。
- 测试更新已提交、响应丢失、另一写入者随后更新的场景；SDK 应返回 RPC 错误，不得发起第二次更新。

## 2. Delta 文件编解码和 SN 物化

代码入口：storage delta readers/writers 及 FFI、`vchannel/module.go`、`l0materializer.WALMaterializer.ObserveMessage()`、`walMaterializeTask.Execute()`、`SyncMaterializer` 和 durable M 发布。

master 在 `VChannelRecoveryModule` 中创建 `WALMaterializer`，当前运行路径保留 Delete/Flush handles。需扩展这条实际运行的链路，并保留 earlier-L1-publication gate，不能只修改留存的 Summary consumer。

### 代码改动

- 新增通用 typed Parquet writer：写入 mutation timestamp、原始事务 body 序号、operation、PK list，以及每个字段的 nullable assignment Struct。保存 field ID、logical type 和 collection/channel/scope/schema 元数据。
- 持久化 UInt32 `source_ordinal`：独立 mutation 为 0，事务内为原始 body 序号（包含未写入 delta 文件的 Insert 所占位置）；拒绝溢出。跨 scope 拆分、分片重切和重试保留该序号，按 VChannel、有效 TS、ordinal 和 PK 恢复顺序／去重；旧 delete 的同 TS 操作可使用 0。
- 分别编码未赋值、SET NULL 和 SET value。整个文件未使用的字段不写入；多个 PK 共用一份 assignments。
- 支持仅读取指定字段和公共列。将 legacy L0、V1/V2 和 packed V3 delete 文件适配为相同的 reader 输出。
- 在 `ClassifyTransformLogMessage()` 和 `BuildTransformLogEntry()` 中支持 UPDATE 和混合事务 body；不能将 update 转成没有 payload 的 barrier。同时修改 `ObserveMessage()`、任务 payload 提取和 writer，保留 assignments 并写入新格式。将 updates 纳入缓冲字节数和操作计数，不计入 delete 数量。
- 保持同一 effective TimeTick 的完整记录组。所有必要 scope 的输出注册成功后，才推进物化进度并通知异步持久化 durable M。
- 解码后只保留需要的值，释放输入 Arrow batches。限制读取窗口和并发；若缺少所需 FFI，更新 storage 依赖。
- 滚动升级期间，原有纯 DELETE 输出保留旧 writer；已是 typed 格式且含非零 source ordinal 的 DELETE 投影不能降级，必须保留其身份。含 UPDATE 的输出也使用 typed writer。已接受更新的落盘和读取不受当前写入开关影响，关闭开关只禁止新请求。

### 测试

- 使用原生 FFI 验证混合 delete/update 文件、稀疏字段、NULL 和共享 assignments；检查按字段读取及旧格式适配。
- 重跑宽表稀疏文件用例，分别记录文件大小、解码后的 Arrow 内存和峰值 RSS。
- 注入写文件或注册失败并重试。未完成的工作不能推进 M；已注册的重复记录保留原始 mutation 顺序。
- 跟踪真实 UPDATE message 和混合事务经过分类、retained handles 到原生文件解码的完整过程；断言 assignments 未丢失，payload 丢失或任一 scope 注册失败时均不能推进进度。

## 3. L0 dispatch 和 Mix compaction

代码入口：MixCoord L0 policy/task、DN `l0_compactor` 和 `mix_compactor`。

master 按 partition/channel 对 L0 输入分组。`L0CompactionTask.transformCursorUpdates()` 已根据 durable M 和未选输入形成的空洞限制完成后的 cursor；这不等于已经实现固定 M0 的输入选择。调整任务选择和 payload 执行时，复用已有的安全发布逻辑。

### 代码改动

- 每轮调度读取一次已持久化 M，记为 M0。选齐本轮覆盖范围内、截至 M0 的所有已注册输入，排除更晚输入。
- 任务在重试时保持原来的 M0、scope 和输入集合。阈值可以决定何时启动一轮，但不能截断任务声明的覆盖范围；被其他任务占用的输入不能视为已经应用。
- 没有符合条件的 ALL_PARTITIONS 输入时，按 partition 创建任务，各自包含该 partition 的 L0 输入和目标；执行仍遵守 channel 级互斥。
- 存在符合条件的 ALL_PARTITIONS 输入时，为该 collection/VChannel 创建一个任务，收齐符合条件的 partition 专属输入和 ALL_PARTITIONS 输入。目标包括没有私有 L0 的 partition。属于 partition P 的目标按 mutation 顺序接收 P 的输入和 ALL_PARTITIONS 输入。
- 完成任务或移除共享输入前，持久化所有适用目标的 deltalogs 和回放游标，包括零输出目标。重试或重启后恢复未完成目标。不能仅因一个 partition 完成就移除 ALL_PARTITIONS 输入。catalog 发布需要拆成多个事务时，先写非空目标的 deltalog keys，再写该目标 cursor；全部目标完成标记持久化之后才能移除输入。
- 同一个固定 M0 的 DN task 内按有界输入组处理，内存准入检查实际驻留的组；汇总所有目标输出并保留 mutation identity，全部输入组完成后才认证 M0。不能仅因整轮总量超过单批内存上限而永久拒绝。
- DN dispatch 和 Mix 使用通用 delta reader。按 mutation 生效顺序归并多个文件，对符合条件的行执行 PK-delete/update，再写入压实后的 base 数据。普通 sort/clustering 重写复用这套 mutation 应用；已排除的 schema/backfill 重写遇到不支持的 update 输入时，必须在发布前明确拒绝，不能丢弃更新。
- Mix 输出保留输入游标的最小值。将 update 文件和字节数纳入 backlog，保留现有调度准入和 channel 互斥。

示例：M0=200，P1@100、P2@130、ALL_PARTITIONS@160 放进同一任务；P1@220 留待以后处理。P1 的目标接收 100＋160，P2 的目标接收 130＋160；即使 P3 没有私有文件，其目标也接收 160。所有适用目标的输出/游标持久化后，才能移除共享输入。BF 无命中的目标仍需记录覆盖完成，但不写空文件。

### 测试

- 交错写入 partition 专属更新和 ALL_PARTITIONS 更新，包括 BF 无命中的目标、没有私有 L0 的 partition。
- 验证 durable M 之后的文件不会被选中。在部分结果发布后重试、输入移除前重启，确保所有目标的更新均保留。
- Mix 包含重叠文件、重复 SET 和 delete/update 序列的数据；重新加载输出并回放后续增量，与独立回放得到的最终值比较。

## 4. Segcore

代码入口：`UpdatesRecord`、`SealedUpdatesRecord`、`UpdatesRecordReaders`、Growing/Sealed segment apply、表达式 evaluator 和 output fill。

master 提供原生列 Scan/Take、scalar index 和 Delete MVCC，但没有这里的字段更新存储。将 TS-vector draft 接入下述完整写入/加载链路；独立的性能调优不作为功能阶段的前置任务。

### 4.1 存储和 apply

- 每个 physical segment 维护一个 UpdatesRecord。每个字段保存 `vector<Delta<T>>`，entry 为 `{timestamp, offset, value}`；同时保存 NULL 标记、`offset -> vector<entry_position>`、membership bitset B 和去重后的 updated-offset 数量。
- B 按 row offset 定位，不按 timestamp 排序；bit=1 表示存在保留的 delta，仍需按 Q_TS 选择版本。TS 顺序保存在 vector 和每个 dictionary history 中。在同一锁/generation 下，B 可以包含只有未来更新的 offset，但不能遗漏可见更新；查询时计算 U_Q，不保存逐 timestamp 的 B 副本。
- 在 draft 的 INT64/VARCHAR 基础上增加 INT8/16/32、BOOL、TIMESTAMPTZ 和普通 scalar ARRAY。ARRAY 更新替换整个值。
- 在 `Apply()` 中，根据 scope、严格的 `row_ts < mutation_ts` 和 delete 可见性定位所有匹配的 physical rows。同一有效 timestamp 新生的行不是该 update 的目标。先准备内存，再追加各字段 entry 和 dictionary position、设置 B，最后发布完成状态。
- 保留事务 body 顺序，按源消息标识去重回放。合法事务 body 共用 commit timestamp 时，替换 draft 中严格拒绝同一 cell 相同 timestamp 的逻辑。
- 对已经挂载的 Growing 和 Sealed 存储支持实时 apply。Growing insert 时扩展 row count 和 bitset 容量；未更新的 cell 继续使用原生 base reader。
- 定长值 inline 存储；变长 payload 明确所有权并按需共享。不复制 base version，不为未更新 offset 分配 history。

### 4.2 表达式执行

- 可走原生索引的 predicate 先保留其 bitmap I。`VisitUpdates()` 用 `upper_bound(Q_TS)` 定位后倒序扫描，每个 offset 只对首次遇到的可见 entry 求值，同时修正 result 和 validity。
- 每个保留的 offset 仅出现一次时，跳过 visited 分配。存在重复时，NULL 或不命中 predicate 的首次 entry 也必须标记；扫到可见前缀起点或选齐所有保留的不同 offsets 后停止。
- 只修正存在可见 delta 的 offsets：`(I - U_Q) union H_Q`。准确的 Q_TS 可见性属于正确性要求：清除 base 命中时不能用 B 代替 U_Q，只有未来更新的 offset 必须保留原 base 结果。在可见前缀遍历中推导 U_Q，或直接覆盖选中的 offsets；无需单独持久化 U_Q bitmap。
- 第一版不加入 bitmap checkpoint cache。若后续单独引入，必须从不晚于 Q_TS、受保护的 checkpoint 加上完整保留的 gap 重建 U_Q；包含未来更新的 latest bitmap 不能替代它。
- raw expression 保留原生 Scan/Take。通过 `ReadRangeUpdates()` 选出当前 batch 中需要更新的候选行；仅在存在 patches 时复制私有 batch，覆盖 values/validity 后执行 evaluator。
- 修正 ColumnExpr 和 CompareExpr，使其使用相同的稀疏 overlay。未更新行不查询 update dictionary；MVCC lookup 前先应用 candidate mask。
- 非 nullable 字段直接返回 all-valid。绕过过期 index 的 min/max、empty-result 和 all-null 快捷判断；对更新字段禁用不安全的 expression cache。
- 保留可更新 VARCHAR 的原生全文匹配语义。将选中的可见 delta values 放入有界、查询私有的 Tantivy 临时批次，使用已绑定 reader 实际注册的分析器和原 MATCH/PHRASE/FUZZY 参数；矫正完成后释放批次，不增加 segment 持有的 text delta index。

### 4.3 按 offset 读取和 output fill

- `Gather()`/`Lookup()` 使用 B 跳过未更新 offsets。对于更新过的 offset，先检查 dictionary 的最后一个 entry，否则按其引用的 timestamp 做 binary search，选择 Q_TS 下的版本。
- 没有可见 delta 时读取 base；可见版本为 SET NULL 时返回 NULL。保留请求顺序、重复 offsets 和 search padding；全部请求 offsets 都不需要 delta 时使用原生 bulk lookup。
- 从 filter 到 output fill、延迟 protobuf/Arrow 输出、排序、分组、聚合及其他更新字段读取，传递同一个 Q_TS 并保护同一个 physical store 的生命周期。
- 复用已有 bulk reader/output 接口，返回的 string/array 存储在消费完成前保持有效。进一步减少 wrapper 分配和格式转换属于独立性能任务。

### 4.4 GC

- 在 Go segcore wrapper 新增 `CSegment.GCUpdates(ctx, safeTS) error`，C ABI 新增 `CStatus GCUpdates(CSegmentInterface segment, uint64_t safe_ts)`，C++ 新增 `SegmentInterface::GCUpdates(Timestamp safe_ts)` 并转调 `UpdatesRecord::GC(safe_ts)`；名称暂定。调用期间 pin physical segment，使用现有 status 转换返回错误；C++ 不回调 Go。
- Go 根据该 physical store 受保护的读路径，从 R_G/R_T 选取 safeTS。对每个 offset 保留 safeTS 之前或恰好位于边界的最后一个 entry，以及之后的全部 entries；不能直接删除整个旧 timestamp 前缀。
- 同时回收 vector entries 和 dictionary references。位置变化时，一次发布重建后的两者，并保留被 reader 引用的旧存储。在同一锁或 generation 下保持 B 和计数一致。
- 先使用受控边界测试。阶段 5 能阻止需要已回收版本的查询进入后，再开启运行时回收。
- 测试 Go/C/C++ 调用链、重复或更旧边界的 no-op、segment release 与 GC 竞态，以及 GC 发布替换存储时旧 reader 的引用安全。

### 测试

- 覆盖值改走后改回、重复版本、多字段更新、NULL 转换、旧 Q_TS、所有支持类型和重复 physical PK。
- 对照独立回放验证倒序选择，包括 history 长度不均和只有未来更新的情况。r10 首次更新在 100、r20 首次更新在 200 时，B 包含两者，而 U_140 仅包含 r10。
- 测试两个有序事务 body 在同一 commit timestamp 给同一 cell 赋不同值：后一个 body 生效，重放不新增重复 mutation，文件或加载顺序不能改变结果。
- 检查稀疏分配、apply 失败时的原子性、append/GC 引用安全和 Growing insert/update 交错。
- 验证 index 矫正、raw overlay、ColumnExpr/CompareExpr、candidate-mask 跳过，以及同一 Q_TS 下按请求顺序返回的 lookup。记录选中的 delta 数和 dictionary probe 数。
- 在宣称完整测试进程通过前，解决或隔离 draft 中已复现的 executor 退出阶段崩溃；当前证据仅为 core tests 和 segment assertions 通过。

## 5. QueryView 投递、加载和恢复

代码入口：WALSummary/TransformLog、GrowingRuntime 实时与回放处理、QN physical-segment loader、segment apply 和 QueryMVCC 进度。

master 已有 Growing 的有限范围 Transform 回放及其后的实时 WAL 投递、Sealed deltalog 加载/Transform 投递，以及 QueryView streaming/requery；当前 mutation handlers 处理 Delete，尚不处理字段 assignments。QueryView lease 保护资源，Summary retention 保护回放历史，二者均不是现成的 Segcore 更新版本 GC 服务。

### 代码改动

- 在 WALSummary 和 TransformLog 中增加 UPDATE payload 构造和分类；保留 PK、assignments、commit timestamps 和完整事务 entries。
- Growing 加载/恢复：还原 insert/base 状态，将 `drainDeleteReplay()` 和 `applyTransformLogEntry()` 扩展到 PK-update TransformLog entries，回放至 BaseTransformTimeTick，再无缺口接入现有 QueryRuntime 实时事件。在当前实时 WAL-message dispatch 中增加 UPDATE，不新增第二条实时订阅。
- Sealed 加载：先加载 base，再通过阶段 2 的 reader 读取该 segment 已有的持久化 deltalogs，包括 legacy delete 和 typed update 文件。按 mutation 生效顺序归并，分别调用 native Delete/Update apply；即使后续没有实时更新，也必须恢复 update store。保留所需值后释放解码的 Arrow batches。
- 将已加载的 base/deltalog 元数据版本与 `transform_start_after_timetick` 绑定，从该 cursor 注册，再接入 TransformLog 历史 catchup 和实时 entries；精确 PK 匹配前按 scope/BF 筛选。不能用文件内最大的 timestamp 推导回放 cursor。加载/注册期间保留所需历史，按约定的源标识和顺序处理重叠回放。
- 区分 ACK-prefix 的快照发布与 consumer 的数据可见性。native apply 成功后才推进 runtime 已应用的 Growing/Transform 边界和 readiness；后续 barrier 不能越过失败位置。
- 保持准备阶段恰好回调一次 `OnReady` 或 `OnUnrecoverable`。准备完成后的 live apply 失败使用独立、异步的 runtime-failure 回调：拒绝该 runtime 的新读取、唤醒等待者，允许新一代重建，同时等待旧 handles 释放。Ready View 失败时向 Coord 报告 Unrecoverable；Up View 失败时保留持久化的 Up 恢复记录，沿用查询触发替换的路径。迟到回调必须核对 runtime 身份。
- 按加载范围固定每个物理 update store 跟踪的字段集合：field ID、逻辑类型、ARRAY element type 和 nullability。集合兼容时保留现有 base/cursor 共享；集合不同时，新建物理实例与 Transform registration，通过共享 buffer 从自身 base cursor 重放，完成后才能 ready。持久化与实时 assignments 使用同一固定集合投影；扩大字段范围不能复用窄实例已追平的边界。
- pin physical store，将成功查询 attempt 的 Q_TS 传递到 filter、streaming/requery 和延迟 output；复用现有 worker/retained-plan 链路。对更新字段绕过过期的 SN segment pruning。
- 先登记 active read requirements 和未来查询允许进入的最小 timestamp，再计算 R_G/R_T。协调登记与 GC 边界发布，为每个 physical store 选择安全边界并调用新增的 `CSegment.GCUpdates()`。仅有 latest applied progress 或资源引用计数不能确定安全边界；还需保留延迟 load/reconnect 所需的 Transform 历史。这些读取与恢复保护通过测试前，保持 GC 关闭。
- JSONL 注入仅用于显式开启的本地测试。生产路径直接对挂载的 store 执行 apply，不为每次 mutation 重建 Begin/Apply/Commit POC session。
- 记录 apply 进度、更新/版本数量、delta 内存及汇总后的读取路径计数；避免在热循环中逐 cell 打日志或更新共享 atomic 统计。

### 测试

- 通过真实 API 更新 Growing、Sealed 和未加载的 collection；达到所需 apply 进度后，验证 query/search 可见性及 output fields。
- 覆盖首次加载、部分持久化、重启、恢复与实时流衔接、release/reload 和 compaction 替换。
- 在没有实时增量的情况下，仅从持久化 update 文件恢复 segment；再测试 legacy delete/update 混合、NULL、历史 Q_TS、持久化与回放记录重叠，以及 load 注册期间到达的 update。与独立 mutation replay 对比结果。
- 测试 streaming 开关、历史 requery、部分输出后的重试和延迟 protobuf/Arrow fill。丢弃失败 attempt 的 metadata/results，保留成功 attempt 的 snapshot。
- 注入 apply 失败，验证查询不会把该 segment 当作已追平；检查全局 pruning 不会丢失更新后命中的行。
- 覆盖 Ready→Up 前后发生 live failure、等待者唤醒、旧 handles 仍被持有时新一代重建，以及失败代际的迟到回调。先加载窄字段范围，对未加载字段写入尚未持久化的更新，再扩大范围，验证重放补齐更新且旧 read 仍然有效。
- 保持旧 snapshot，同时执行 updates 和 GC；测试延迟准入和注册竞态。关闭新写入后重启/回放，验证已有更新仍然可见。

## 6. 本地 benchmark 和 e2e 验收

- 生成 base 数据、有序分散 updates 和独立计算的最终值 mirror。计时前先比较返回的 offsets/PKs 和投影字段值。
- 使用纯内存 `Segment::Retrieve`、单请求线程、空 output fields 和 limit 1000。对齐 runtime 的 raw/index 驻留方式，包括适用场景下只加载 STL_SORT。
- 使用一个 ClickBench 表达式结构：`BinaryRange(ClientIP) AND Term(URL IN ...)`，搭配 100 组确定的边界/值参数。先测试 1% 和 5% 的单次更新，再单独标记一个重复更新用例。
- 在相同原生索引配置下比较 TS-vector/dictionary 与 mirror；raw 和 indexed 配置分开测量。warmup 后每种配置至少执行 1000 次查询，关闭 expression cache，排除 setup/apply/plan 准备时间，并确认执行计数持续增加。
- 单独使用 equality 测 output lookup，对比空 output fields 和选择 INT64/VARCHAR 字段。记录延迟、扫描/选中 delta 数、dictionary probes、bitmap/overlay 工作量和 RSS；load/apply 单独计时。
- 使用相同资源和对齐的单 segment 布局执行服务 e2e。覆盖 legacy 文件、CDC、物化重试、durable M 滞后、跨 scope dispatch、compaction/reload、apply 失败和 GC 保护。
- 全部检查通过且支持的 schema/backfill 组合记录清楚后，才开启公开写入。保留已有测试 instances 和 collections。
