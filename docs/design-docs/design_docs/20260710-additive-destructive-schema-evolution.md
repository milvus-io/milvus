# 在线 Schema 变更安全:Additive/Destructive 模型 + Ready-Snapshot 基础设施 + 节点协同

- Issue: #50989
- 日期:2026-07-12
- 状态:设计文档(**社区 PR 前需译英文**)

本文档描述 Milvus 在线 schema 变更(加/减列、加/减函数)的安全模型、节点协同与长期数据正确性:一个 schema 变更如何在读写在途、跨多个 vchannel 非原子传播、且节点可能崩溃/重启/扩容的情况下,既不产生部分写、不脏数据、不拉黑健康节点,又让 additive 变更对用户透明。

---

## 目录

**Part A — 模型与原则**
- §0 一页纸
- §1 朴素单闸门方案为什么不够
- §2 核心原则 & 五条不变量(I1–I5)
- §3 责任边界(additive 透明 / destructive 只保证安全失败)
- §4 版本模型:只比上一步 + `change_class` 分类器
- §5 分类矩阵(逐情况)

**Part B — 基础设施(RCU 快照 + readiness)**
- §6 每 delegator 两级 schema 状态:applied(写)vs published(读,RCU)
- §7 state-derived publish gate
- §8 DDL readiness 握手
- §9 function runner 一致性 + rolling-upgrade 契约

**Part C — 读写路径(机制)**
- §10 写路径:节点 additive 放宽 + 分类器白名单 + 补齐零代码
- §11 读路径:版本 gate + segcore 止血(绝不拉黑)
- §12 无 tombstone:靠什么扛正确性
- §13 函数 & 远程 runner:本地 inline / 远程 Spark backfill

**Part D — 节点协同时序**
- §14 参与者与各自 schema 状态 + 全局时钟四条时序保证
- §15 加列泳道时序图 + 减列差异
- §16 写路径 corner case / 读路径 corner case
- §17 节点故障 corner case
- §18 长期正确性(稳态 / GC / backfill / compaction)

**Part E — 失败隔离与边界**
- §19 失败隔离 + `(version, barrierTs)` readiness + 物理回收
- §20 已知限制 / 边界场景
- §21 非目标 / 契约

---

# Part A — 模型与原则

## 0. 一页纸

把每个 schema 变更分成 **additive**(加字段/函数)和 **destructive**(drop / 改语义,后者=加新 id + 删旧 id),**永不原地 mutate 一个 live field id**。然后:

- **Additive**:系统对用户**透明**——读写在过渡期都成功,**无部分写、无无谓失败**。靠两件事:①**节点放宽**(超前 vchannel 接受旧版本写并补齐)②**读时补齐**。前提是 rootcoord 暴露的 `SchemaVersion` 只在 broadcast 完成后的 ack 回调里推进,天然 `≤ 所有 vchannel applied`,所以 proxy(含崩溃/重启/扩容的新 proxy)永不超前——**不需要额外的版本闸门**(§10)。
- **Destructive(drop)**:系统**只保证安全失败**(不崩、不脏、不拉黑),**用户先停用**是契约(§3)。靠 `max_field_id`(id 不复用)+ **读版本-拒绝**(陈旧读引用被删字段 → 干净硬拒绝,InputError、不拉黑,而非 assert;proxy cache 由 AlterCollection ack 回调主动失效、秒级收敛后自然恢复)。读侧 proxy 重编译透明自愈是可选优化、非目标(§11.1/§21)。
- **不引入 tombstone**:dynamic collection 上"被删 top-level 名字 → 落 `$meta`"是有意语义,dynamic 的名字解析正确性明确归用户(§3);struct 子字段严格解析、不受影响(§12)。

## 1. 朴素单闸门方案为什么不够

朴素的做法是对所有变更用一把闸门(写:`请求版本 == vchannel 版本`;读:`版本 <= 已发布` + 只保 ANN 的 guard)。两类变更的安全要求相反,单闸门必然在其中一类上出错:

1. **加字段类 → 跨 vchannel 部分写。** N→N+1 逐 vchannel 传播(非原子)。A 到 N+1 拒 N 请求,B 还 N 接受 N 请求 → 跨 A、B 的一次写只落 B → proxy 判整体失败 → 重试**双写** B。
2. **删字段类 → 陈旧读崩+拉黑。** 陈旧读引用被删 field id → segcore `Schema.h operator[]` 的 `AssertInfo` 抛默认 `UnexpectedError(2001)` → 通用 `ErrSegcore`(非 inputError、非 retriable)→ `lb_policy` 命中 `else` **拉黑健康 shard leader**;只有 ANN 字段被单闸门的 guard 拦成 InputError。

本设计对两类变更分开处理:加字段类靠节点放宽消灭部分写,删字段类靠 segcore 读入口重分类消除拉黑。

## 2. 核心原则 & 五条不变量

> 每个变更表达成 **additive 加** 或 **mark-delete(drop)**;改类型/语义 = 加新 id(additive)+ 删旧 id(destructive)。**永不原地 mutate live id 的数据或语义。**

**I1(地基).** DDL 层**结构性不可达**"原地改一个 live field id 的语义"(类型、nullable、default、analyzer、element_type…)。这是读路径"字段全在就服务"正确性的前提——见 §11。**不是约定,是必须在 DDL 接口层堵死的不变量。** `AlterCollectionSchema` 只有 Add/Drop 动作,无原地改字段类型/nullable 的 API;改类型/语义只能 drop-old + add-new(expand/contract,`max_field_id` 给新 id),所以 I1 由 API 结构保证。

**I2(写侧不超前).** proxy 永不超前于任何 vchannel。**天然成立、无需额外机制**:rootcoord 暴露的 `SchemaVersion` 只在 broadcast 完成后的 ack 回调里推进(§10、§14.2),所以 `SchemaVersion ≤ 所有 vchannel applied` 恒成立。

**I3(id 单调).** `max_field_id` 单调;dropped id 永不复用;re-add 拿新 id → 旧列数据永不被重解释。

**I4(读不崩不拉黑).** 读引用当前 schema 没有的 field id → 干净拒绝(不是 assert),不拉黑。见 §11.1。

**I5(destructive 契约).** destructive 变更系统只保证**安全失败**;**drop 前先停用是用户责任**(§3)。

## 3. 责任边界

| 类别 | 系统保证 | 用户责任 |
|---|---|---|
| **Additive** | 完全续跑 + 安全:读写过渡期都成功,**无部分写、无无谓失败**,透明。 | 无 |
| **Destructive** | **只保证安全**:不崩、不脏、不拉黑;掉队引用干净失败(重编译后 "not found" / dynamic 落 `$meta`);id 不复用。 | **先停用**:drop / 破坏性改语义前,停止发引用该字段的请求。 |

道理:加字段的部分写是用户躲不掉的系统缺陷,系统必须兜;drop 用户明知在删、理应已停用,系统只硬化底层失败模式。

## 4. 版本模型:只比上一步

rootcoord 的 `SchemaVersion` 既是内部版本、也是 DescribeCollection 暴露给 proxy 的版本(proxy 拿它盖读写请求的版本戳)——**不需要区分内部/暴露两个版本**,因为它只在 broadcast 完成后的 ack 回调里推进,天然满足 I2(§10)。

同 collection 的 DDL 串行(resource lock)+ 每次变更让 proxy 收敛后下个 DDL 才发生 → 请求正常最多落后**一个版本**。节点只需保留:当前 schema + 上一个版本号 + 上次变更的 `change_class` + 那一步的字段 diff。**无 `additive_since` 累积。** `change_class` 在 DDL commit 时由 **rootcoord**(算 old/new schema diff)盖到版本上,带 **fail-safe 默认:识别不了的一律 destructive**。

## 5. 分类矩阵(逐情况)

术语:**旧/新** = 按变更前/后 schema 编译;**补齐** = 读时对缺字段的旧行填 null/default/`{}`。物化方式(inline 算 vs 延后 backfill)与兼容类别正交,不改变分类。

| 变更 | 类别 | 旧写(过渡) | 旧读(过渡) | 新写 | 新读 |
|---|---|---|---|---|---|
| 加 nullable/default 列 | additive | 接受;新列物化 null/default | 正常 | 写新列 | 服务;旧行补 null/default |
| 加 nullable struct/子字段 | additive | 接受;子字段物化 null | 正常 | 写入 | 服务;旧行补 null |
| enable dynamic | additive | 接受;`$meta` 物化 `{}` | 正常 | 写 `$meta` | 服务;旧行 `$meta`=`{}` |
| 加 function output(本地/远程 runner) | additive | 接受;output 物化随 runner 局部性(§13) | 正常 | 写入 | 服务;旧行 output 由 backfill 渐进补 |
| drop 列/struct/function output | destructive | 上次 destructive → 拒绝+重编译;仍可能与滞后 vchannel 部分写(§3) | 引用被删字段 → delegator 拒绝 → 重编译;不引用 → 正常 | 无此字段 | 不引用;旧行被删列不可见,compaction 回收 |
| disable dynamic | destructive | 拒绝+重编译 | 引用 `$meta` → 拒绝+重编译;不引用 → 正常 | 不能写 `$meta` | 不暴露 `$meta`;历史 `$meta` 不可见 |
| 改类型/语义、改 BM25/MinHash signature | 加新 id(additive)+ 删旧 id(destructive) | 提供旧 id → 拒;新 id → additive | 引用旧 id → 拒+重编译;新 id → additive | 写新 id | 读新 id;旧行新 id 值由迁移 job 补 |

> **表中 destructive 读的"+重编译"= 可选透明自愈**(§11.1 ②③,非目标);不启用时为**干净硬拒绝**(InputError、不拉黑、可重试),安全性不变,proxy cache 秒级收敛后按新 schema 自然生效。
> **"drop 后同名 re-add" 不是一类**:= drop(destructive)+ add(additive)。`max_field_id` 给 re-add 新 id;旧 id 引用被拒+重编译;新读原子解析到新 id。
> **同版本 property/语义变更(TTL、mmap)是第三类**:不加不删字段,刻意不 bump 版本,走 §19 的 `(version, barrierTs)` readiness,不进 §10 写闸门。**注意 TTL 改的是行可见性——是 I1 的一个受控例外**,需在 §19 显式处理(否则 §11 纯版本读服务看不到它)。

---

# Part B — 基础设施(RCU 快照 + readiness)

本 Part 覆盖**过渡期时序**:一个 schema 变更在每个 shard delegator 真正就绪之前,不得对读可见。published-snapshot(RCU)+ readiness 握手是这套过渡期正确性的基础设施,被 additive/destructive 模型复用。

三类具体失败(基础设施要解决的问题):

1. **Function runtime state 与段加载竞态("load-wins")**:delegator 的 function-output 元数据若只由 `UpdateSchema` WAL 事件在 freshness guard 后重建,一次段加载(`PutOrRef`)先推进了 collection schema,就把该事件变成 no-op,于是对 MinHash/BM25 output 字段的搜索把原始 VARCHAR 占位符转发到 segcore(`Plan.cpp:163` data-type assert)。
2. **BM25 stats 可见性竞态**:一个 schema 版本可能在 reopen 仍在下载/激活其 sealed BM25 stats 时就对读暴露,让 BM25 搜索通过 gate 却静默返回空结果(`avgdl <= 0`)。
3. **无 readiness 契约**:若 consume pipeline 里的 `UpdateSchema` 失败被 warn-and-skip(永久把 delegator 卡在旧 schema 上,之后新 schema 的 insert 静默丢新字段),而 DDL 完成与 shard readiness 无任何关系,则 "AddField 返回" 并不意味 "新字段可用"。

## 6. 每 delegator 两级 schema 状态

| 状态 | 由谁推进 | 服务 | readiness 要求 |
|---|---|---|---|
| **applied**(live collection schema) | WAL `UpdateSchema`(按序),或段加载 `PutOrRef`(load-wins) | 写路径(`ProcessInsert` growing 创建)、index meta | 仅 WAL 顺序 |
| **published**(`readySnapshot`,RCU 原子指针) | 仅 `tryPublishReadySchema()` | 读路径(`search`/`Query`/`GetHighlight`/`RunAnalyzer`) | 下方完整 checklist |

**`published ≤ applied` 恒成立。** 读者无锁加载快照;快照发布后永不 mutate;发布按 `(schema version, barrier ts)` 单调。

## 7. state-derived publish gate(取代 control-flow 协调)

`tryPublishReadySchema()` 每次调用都从**状态**重新推导 readiness:

1. **Function runners ready** — `function.EnsureRunnersReady` 同步初始化该版本的每个 runner(无读路径 lazy init;失败是 error,由调用方的 retry 循环驱动)。
2. **无在途 BM25 stats 加载** — `LoadSegments` 在任何可能推进 applied schema 的步骤**之前**,把携带 stats 的段标进 `pendingBM25Loads`,并总是在返回时(成功或失败)清除,随后重试 publish。
3. **idfOracle 已激活** — `idfOracle.Ready()`:属于当前 serving target 的每个**已加载** sealed stats 条目都已激活(合入聚合 stats)。

Publish 还额外要求 schema **已被 WORKERS 知晓**,而不只是 delegator:一个已发布版本会准入按它编译的读,这些读扇出到 workers,其 segcore 必须已经持有该 schema。主 `UpdateSchema` 路径总是把 schema 扇出到所有 worker;load-wins 路径与 `UpdateSchema` no-op 分支(当某加载先推进了 schema 时到达,否则会整个跳过扇出)在其 publish 尝试前执行相同的 worker 扇出。

**条件 2–3 失败不是 error**:该次尝试是静默 no-op,读路径继续服务上一个 ready 版本;下次状态变更重新尝试。调用点:delegator 创建、`UpdateSchema`(normal 与 no-op 分支)、`syncCollectionIndexMeta`(load-wins)、`LoadSegments` 里的延后 publish、以及 idfOracle 激活回调(`SetOnStatsActivated`,由 `SyncDistribution` 触发)。因为每个 publisher 都过同一个 gate,UpdateSchema / loads / reopen 之间**不需要任何顺序协调**——无需 `deferPublish`/`resumePublish` 这类 control-flow 机制。

**渐进可见,而非穷尽 readiness。** `Ready()` 刻意**不**要求每个 target 段都有 stats:一个不在 oracle 里的段目前没有任何可加载的东西(它的 BM25 backfill——一个**独立的、外部的** job,经 `DataCoord.CommitBackfillResult` 提交——尚未运行)。要求它会让 gate 与 backfill 死锁,而 backfill 只能在 DDL commit 之后运行。旧行随 backfill + reopen 推进渐进变得可搜索;IDF/avgdl 永远与当前已索引语料一致。只有**在途**加载(pending set)与**已加载-未激活**条目 gate publishing——两者按构造都是瞬态,所以 gate 不会死锁。

pending gate 保护的是 **PUBLISHING**;已发布的版本永不被撤回。对于在 publish 之后到达的 backfill 驱动 reopen,由 `LoadSegments` 内部的顺序来关窗:reopen 的段的 BM25 stats 在 worker 换入新列**之前**下载并注册(下载失败在 swap 前中止 reopen,querycoord 重试),并在 swap 之后**紧接着**激活——所以不存在"新列可搜索但其 stats 尚未进 oracle"的区间(否则 avgdl 为 0 → 空结果,或 IDF 静默偏斜)。

## 8. DDL readiness 握手

- **Delegator 上报**:暴露 `ReadySchemaVersion()`(published 快照版本,首次 publish 前为 -1)→ 经 `GetDataDistribution` 报进 `querypb.LeaderView.ready_schema_version`。每次 ready publish 都 bump querynode 的 distribution-modify 时间戳:纯 schema publish 不改任何段分布,若不 bump,unchanged-distribution 快路径会一直返回没有 leader-view 的响应,把新版本对 querycoord 隐藏,使握手无限期停滞。
- **QueryCoord 校验**:`CheckSchemaReady(collectionID, version)`:当且仅当 collection 未加载,或每个 query-visible 副本的 shard leader view 都上报 `ready_schema_version ≥ version` 时为 true(collection 已加载但无副本 query-visible 时为 not-ready)。这是合并 coordinator 上的**进程内**契约,刻意不是服务 RPC:readiness 数据经常规 `GetDataDistribution` 心跳流到 querycoord,而唯一调用方——rootcoord 的 DDL 回调——总是同进程,所以没有 wire surface、这个调用也没有 rolling-upgrade 版本协商顾虑。
- **RootCoord ack**:`alterCollectionV2AckCallback`(所有 schema-bumping alter 共用;由 `Updates.Schema != nil` 检测)在 `ExpireCaches` **之前**轮询 `CheckSchemaReady`。有界轮询后仍 not-ready 返回 error,broadcast 框架带 backoff **无限期**重试(持有 per-collection resource lock,所以同 collection 的 DDL 串行)——内建后台补偿。因此客户端的 AddField 调用会阻塞到所有 shard 就绪或自身 deadline;无论哪种,cache 都只在 readiness 之后过期。

**cache 过期 gate 在 read-readiness 之后**:ack 回调里 `waitCollectionSchemaReady`(所有 delegator published)通过才 `ExpireCaches`。写侧的"所有 vchannel applied"由 ack 回调的触发时机天然保证(§10/I2)。fresh proxy 走 DescribeCollection 直取 `SchemaVersion`——写安全(所有 vchannel 已 N+1),读若 delegator 未就绪则命中既有 `v>P` not-ready 重试。**delegator 版本 gate 仍是对"过早观察到的 schema"(fresh proxy、cache miss、rebalancing leader)的正确性后盾。**

## 9. function runner 一致性 + rolling-upgrade 契约

### 9.1 function runner 跨版本一致性

读路径 runner 在 `LatestFunctionRunnerVersion` 解析:runner 按 function signature 建 key、跨版本共享,所以 latest-version runner 对任何已发布快照都正确——**前提是一个字段的 function signature 永不原地改变**。两条规则强制这点:

- **Detach 一个 BM25 或 MinHash function(保留其 output 字段)在 proxy 与 rootcoord 双端被拒**——在新 `AlterCollectionSchema` 路径上,以及 legacy `AddCollectionFunction`/`AlterCollectionFunction`/`DropCollectionFunction` API 上都拒:detach 或原地改参数会让 query-time hashing/scoring 与旧 signature 下索引的数据静默不匹配。移除这些 function 时总是同时 drop output 字段,所以 re-add 的 function 拿到全新 field ID。
- **搜索路径带 dropped-anns-field guard**:一个 version-gated 请求,其版本 ready 快照已覆盖,但 target 一个不在快照里的 field ID,被以清晰 input error 拒绝(O(1) set 查)——drop 是"旧版本永远可服务"的 non-additive 例外,转发这种请求会在下游 opaque 失败(或静默误执行 BM25/MinHash 文本搜索),并在 proxy LB 里拉黑一个健康 delegator。Version-0 legacy 请求跳过该 guard。
- **当 function 在一个 applied-but-not-yet-published 版本里被 drop**:latest runner 版本不再映射该字段;该读(它合法地通过了针对仍发布的旧快照的版本 gate)拿到可重试的 `ErrCollectionSchemaVersionNotReady` 而非硬 ServiceInternal——按构造是过渡态,一旦新版本发布就收敛。

### 9.2 rolling-upgrade 契约

本特性对 schema DDL **没有** "rolling upgrade into it" 保证,这是设计使然。存在三个 legacy 逃生舱,使**稳态**流量能挺过混合版本窗口:

- version-0 读请求跳过版本 gate;
- version-less insert 跳过 streamingnode exact-match 闸门;
- 升级前的 querynode 上报**无** ready 版本(握手视之为 ready)。

它们保护升级期间的既有流量;它们刻意**不**让 schema DDL 在该窗口安全——一个旧组件无法遵守它不知道的语义。因此运营契约是:升级顺序 **coordinator → querynode → proxy → SDK**,且**集群混合版本期间不做任何 schema-change DDL**。

这条"混合版本期间禁 DDL"的契约现在由代码强制,而不再只是文档约定:rootcoord 的 schema-change choke point(`broadcastSchemaChange`)在准入结构 gate 之前先跑一个 **cluster-min-version gate**(`checkClusterVersionForSchemaChange`)——它从 session registry 逐 role(proxy / querynode / datanode / streamingnode / 各 coordinator)列出存活组件,只要有任一组件的 major 版本 < 3.0(引入本协议的版本)就拒绝该 DDL。它 **fail closed**(registry 读不到、或某 session 版本无法解析都算拒绝),默认开启,并留了一个运行期急停开关 `rootCoord.enableSchemaChangeVersionGate` 给运维。稳态逃生舱本身保持不变(它们保护的是既有流量,不是 DDL),因此**契约仍是安全边界,而这个 gate 把"混合版本不做 DDL"这一条从约定变成了强制**。

### 9.3 Alternatives considered(基础设施层)

- **每请求 field-ID 依赖校验**:在 catch-up 期间放行无关读,但要求每请求 plan unmarshal、漏掉 semantic-only 变更(如 analyzer 参数)、并需脆弱的 expr 遍历。被版本 gate 取代;握手让 gate 拒绝在实践中罕见。(注:读路径又在版本 gate 之上补了 segcore 全字段拒绝作安全地板——见 §11.1。)
- **`deferPublish`/`resumePublish`(control-flow 延后)**:要求每个 publisher 协调顺序;被 state-derived checklist 取代。
- **把 DDL gate 在 backfill 完成上**:循环依赖(backfill 需已 commit 的 DDL);改用渐进可见。

---

# Part C — 读写路径(机制)

> **说明**:本 Part 讲**机制**(节点如何放宽、segcore 如何止血、名字如何解析);Part D 讲同一套读写的**时序**(泳道图、corner case、长期收敛)。两者交叉引用但不重复整段叙述。

## 10. 写路径:节点放宽(消灭加字段类部分写)

**关键前提(I2):`rootcoord 暴露的 SchemaVersion 本身就 ≤ 所有 vchannel 的 applied version`,不需要额外的版本闸门。** 因为 `SchemaVersion` 只在 `ApplyUpdates`(`meta_table.go:1062`)推进,而它**只被** `alterCollectionV2AckCallback`(broadcast 的 ack 回调,`ddl_callbacks_alter_collection_properties.go:457`)调用;ack 回调在 broadcast 完成之后才触发(V1),而 broadcast 完成 ⟺ 所有 vchannel 已 append+apply(V2)。所以 **SchemaVersion 变成 N+1 时,所有 vchannel 早已是 N+1**。DescribeCollection 返回 SchemaVersion,任何 proxy(含崩溃/重启/扩容的新 proxy,它走 DescribeCollection 直取 SchemaVersion)拿到 N+1 时都不可能超前于任何 vchannel。→ **"新 proxy → 滞后 vchannel"这半天然不存在,无需额外写闸门。**(V1/V2/I2 三条时序保证的权威描述见 §14.2。)

**节点放宽**处理另一半——broadcast 传播途中,某些 vchannel 已 N+1、而所有 proxy 还在 N(SchemaVersion 尚未在 ack 回调里推进),写 `insert(N)` 打到超前 vchannel。每节点(streamingnode `checkIfCollectionSchemaVersionMatch`)用自己当前 schema + 上一步:
- `请求版本 == 当前` → 接受。
- `请求版本 == 上一个`:上次 **additive** → 接受(闸门返回当前版本 N+1);上次 **destructive** → 拒绝 + 重编译。
- 更旧(≥2)→ 拒绝 + 重编译。
- 写请求引用被 mark-delete 的字段 → 拒绝(§12)。

写入的 header 携带 `SchemaVersion`;streamingnode 在 WAL append **之前**拒绝版本不匹配的 insert,并在 append 时**同步** apply AlterCollection(fence + flush 旧 growing,更新 schema 与 function runner)。WAL 全序 + 下面的 pipeline 规则保证 `ProcessInsert` 运行时 delegator 的 applied schema `≥` 任何 insert 的版本。

### 10.1 分类器:additive 白名单

`change_class` 由 rootcoord 在 DDL commit 时算(`isAdditiveSchemaChange`),采**白名单**判据:只在**每个现有 field 的所有写相关维度都不变** + **每个现有 function 不变**时才判 additive,其余一律 destructive(fail-safe)。

- **field 维度**:`DataType` / `ElementType` / `Nullable` / `IsFunctionOutput` / `TypeParams` 任一变化即 destructive。`IsFunctionOutput` 维度防止把已有用户数据字段重标成 function 输出列(见 §13);`TypeParams` 维度防止缩容(见下)。
- **function 维度**:每个现有 function 逐个 `proto.Equal`(覆盖 input / output / params / type),任一变化即 destructive——只比 `OutputFieldIds` 会漏"原地改函数 input/params(如 embedding model 换端点)但保持 id+output 不变"这类 destructive 变更。
- **加新字段/新 function**:加新 nullable/default field additive;**加带新输出列的新 function 仍是 additive**——本地 runner 写侧物化、远程 runner 留空待 backfill,与旧行一致。
- 删 field / 关 dynamic / 改任何现有维度 → destructive。

DDL 层还额外禁止缩小 `max_length`/`max_capacity`(新值必须 ≥ 旧值);缩容会让已存的长值在新约束下非法,属受控的 destructive 一类,直接在 DDL 拒绝而非放行。

**误判的代价是不对称的**,所以分类器 fail-safe 偏 destructive:把 destructive 误判成 additive = 静默损坏(旧写被放宽接受、或旧读被误服务);把 additive 误判成 destructive = 一次过渡窗口内的拒写+重试,无损坏。因此"识别不了的一律 destructive"。

### 10.2 补齐无需写新代码

接受一个缺新字段的 `insert(N)` 后,**下游既有机制自动补齐**——segcore 的 growing segment `Insert`(`SegmentGrowingImpl.cpp:666`)对缺失的 nullable/default 字段填 null/default(注释即为"insert used old schema, segment has latest"这个场景),querynode/streamingnode 只透传;函数 output 由现有 `materializeFunctionFields(N+1)` 在写时算(本地 runner)或留空由 async backfill 补(远程)。additive 加的字段恒为 nullable/default/function-output,segcore 只对"非 nullable 无 default"才 assert,不中招。**所以 streamingnode 侧只需放宽闸门 + 记录上一步分类,零 backfill 代码。**

**schema-change 消息永不被跳过**:`pipeline/delete_node.go` 原地带 backoff 重试失败的 `delegator.UpdateSchema`(`queryNode.schemaUpdateRetryTimes`,默认 10),阻塞该 vchannel 的消费(tsafe 停止推进;强一致读 stall;最终一致读继续服务旧快照),耗尽后 panic 使 querynode 靠 WAL replay 恢复。跳过该消息会让后续新-schema 的 insert 被按旧 schema 处理。

**为什么加字段类无部分写**:传播窗口内所有 proxy 看到 N、写 `insert(N)`;超前 vchannel(N+1)靠放宽接受+补齐,滞后 vchannel(N)匹配接受 → 两边都接受。窗口结束(ack 回调推进 SchemaVersion 到 N+1,此时所有 vchannel 已 N+1)后,proxy 才看到 N+1,无 lag。补齐确定性,两 vchannel 的 WAL 合法不同但读/replay 收敛。

**AlterCollection 幂等**:AlterCollection 采用严格版本判据,重放不覆盖 `PrevSchemaVersion`;不做回滚,靠幂等 + 既有无限重试收敛。这让 broadcast 的无限期重试(§8)与 replay 都安全——同一 alter 被应用多次不改变结果。

> **限制(需限定)**:"additive 消灭部分写"只在"最多落后一版"成立时为真——两次 additive 极快连发、请求落后 ≥2 版时,A 在 N+2 拒、B 在 N 接受,仍可能部分写(窄,受 DDL 速率约束)。本节成立依赖 apply-at-append(V2)+ "ack 回调在 broadcast 完成后触发"(V1)——见 §14.2。

## 11. 读路径

delegator 拿读(编译版本 v = 该 proxy 的 W),对照已发布版本 P:
- **v == P** → 热路径,直接服务,不解析 plan。
- **v > P** → shard 落后,not-ready 重试(既有 gate,`ErrCollectionSchemaVersionNotReady`=110)。
- **v < P** → 冷路径,解析 plan **全部字段依赖**(filter/output/group/order/aggregate、Query/QueryStream、delete-by-expr、struct 子字段、非 ANN):
  - 依赖 id **全在** P → 服务。**正确性依赖 I1**:present 的 id 语义不变,所以按 P 服务 = 按 v 服务。
  - 有 id **不在** P → 干净硬拒绝(§11.1 ①,`FieldIDInvalid`、不拉黑);**[可选、非目标]** proxy 重编译透明自愈(§11.1 ②③)。

proxy 在请求上附带它编译所用的 schema 版本(`internalpb.SearchRequest/RetrieveRequest.collection_schema_version`,在 search/query `PreExecute` 与 `deleteRunner` 构建的 delete-by-expression 读请求中设置)。delegator 只从已发布快照服务;Version 0(legacy proxy / rolling upgrade)跳过 gate。

物理删列 compaction 全 lazy、与读无协调——能引用被删列的读都先被拒。单版本 worker 足够:destructive 读靠**拒绝**保证安全,不靠服务旧数据。

### 11.1 绝不拉黑:一层核心 + 两层可选优化

根因是抛错分类错(缺字段走默认 `2001` → 拉黑)。**一层核心保证安全,两层可选给透明:**

- **① Segcore 地板(核心、无条件)。** 在 segcore **读执行入口**(plan 创建、解析前)显式校验:plan 引用的 id 缺 → `ThrowInfo(FieldIDInvalid=2020)`(`merr` 已归 `inputError`)→ `lb_policy` "是 InputError 就早返回、**不拉黑**"。**这一层单独就达成整个方案的安全目标(不崩、不拉黑)**,不依赖任何 plan 遍历。校验在 `CreatePlan`/`CreateRetrievePlan` **解析前**先 `CollectAccessFieldIDs`(ANN、predicate 列、group-by、order-by、aggregate、output 全覆盖)逐个 `CheckPlanFieldPresent`,缺则 `ThrowInfo(FieldIDInvalid=2020)`(跳过 system 字段);`FieldIDInvalid` 经 `plan_c.cpp:75` 原样传出。⚠️ 必须在解析前校验:若放在 `PlanNodeFromProto` 之后,filter/predicate 字段会先在 `ParseExprs → Schema::operator[]` 抛默认 `UnexpectedError(2001)`,校验对主场景(引用被删字段的过滤谓词)失效。⚠️ 只在读入口显式校验后抛,**不要全局改 `operator[]` 默认 code**。
- **②③ 透明自愈(可选优化、非目标 — 见 §21)。** ② delegator `v<P` 冷路径缺字段 → 返回可重试的"版本不匹配、请重编译"错(区别于 110"shard 落后、等一等");③ proxy 识别后失效 cache → 重拉 schema → 重编译 plan → 重试至收敛(真删 → 干净 "not found" / 落 `$meta`;drop+re-add → 解析到新 id,透明成功)。**目的仅是把过渡窗口内的一次硬失败磨平成透明重试,不改变安全性。**

**分工**:① **单独、无条件保证安全(不拉黑/不崩)**;②③ 只加"透明度"。**不做 ②③**:destructive 陈旧读引用被删字段 = 走 ① 干净硬失败(InputError、不拉黑),用户重试 + proxy cache 秒级主动收敛后恢复。

> ⚠️ **②③ 全或全无**:③ 的"proxy 失效+重拉+重编译+重试"在现状不存在(`PreExecute` 只编译一次、`lb_policy.ExecuteWithRetry` 复用同一请求同一版本戳)。**只做 ② 不做 ③** → 一个 `v<P` 可重试拒绝让 proxy 换节点**拿同一 v 无限重试**(重试风暴,比只有 ① 更差)。所以要么一起做、要么都不做。**当前只保留 ①**(§21)。

## 12. 无 tombstone:靠什么扛正确性

**不引入 `dropped_fields` tombstone。** 正确性由三件独立的东西保证:**`max_field_id`(I3)** + **读版本-拒绝(I4/§11)** + **写侧 `SchemaVersion` 天然不超前(I2/§10)**。tombstone 唯一能多做的是"dynamic 上被删名字静默回落 `$meta`"——这件事按 §3 归用户。

名字解析回到既有、不依赖 tombstone 的规则:live 字段(含 struct 子字段)命中即返回;不命中且 dynamic → 落 `$meta`;不命中且非 dynamic → "field not found"。**保留** `verifyDynamicFieldData` 既有的 static-name 检查(拒绝 `$meta` key 撞 live 静态名)。

### 12.1 dynamic + `$meta`:归 §3

去 tombstone 后,dynamic collection 上一个被删的 **top-level** 字段名,再被引用会**静默解析到 `$meta`**(和任何未知裸名字一样)——这可能返回该名字在 `$meta` 里的历史数据、或让 delete 打到 `$meta` 匹配的另一批行,**无报错**。这一族"静默解析对了、结果可能不对"的行为**归 §3(用户 drop 前先停用)**;§11.1 的核心网只保"可用性(不拉黑)+ 非 dynamic 正确性",不覆盖它(没有 id 缺失,不触发任何一层)。

**范围收窄**:
- **只在 dynamic collection 的 top-level 字段**。裸标识符走 `parser_visitor.go` 的 `translateIdentifier` → `GetFieldFromNameDefaultJSON` → 不是静态字段就静默落 `$meta`。
- **struct 子字段不受影响**:只能用显式语法 `docs[text]` / `$[text]`,解析走**严格的 `GetFieldFromName`**(`VisitStructField`/`VisitStructSubField`),缺就 loud "struct field not found",**从不落 `$meta`**。所以 struct 子字段无需任何 tombstone。
- **非 dynamic collection**:被删/未知名字直接 "field not found"。

**无 corruption**:被删列(旧 id)数据 pre-GC 不被任何读访问(陈旧读被 §11 拒);id 不复用(I3);`$meta` 撞 live 静态名有独立检查(保留)。唯一残留是上面 dynamic + top-level 的静默名字解析,归 §3。

> 若将来想收掉这个静默:让 top-level 裸标识符也走**严格解析**(对齐 struct 已有行为)、动态键一律显式 `$meta["x"]`——一个 parser 层改动,仍不需要 tombstone。

## 13. 函数 & 远程 runner

**一条 DDL 规则(function 输出列必须是新建的空列)。** add-function 的输出**永远是本次新建的空列**,不是复用一个已有的数据字段——由 DDL 强制:
- 正路 `AlterSchemaAddFunctionField`(新字段 + function):`ValidateAlterSchemaAddFunctionPlan` 强制 `output == 本次新建的字段`(`alter_schema_add.go:127`),向量输出还必须带 index 参数(`alter_schema_add.go:137`)。
- 侧路 `AlterSchemaAddFunction`(只加 function、不新建字段):对 BM25/MinHash 一律拒绝(`alter_schema_add.go:115-119`),这两类 add-function 必须走"新字段 + function"的正路。
- legacy `AddCollectionFunction` API 硬拒 BM25/MinHash(`function_task.go:101`)。

这条规则保证用户数据字段**不会**被 relabel 成 function 输出列——否则历史用户数据会被当成 function 计算结果重解释。§10.1 分类器的 `IsFunctionOutput` 维度是同一保证在写路径分类上的对应项。

**proxy insert 排除 function 输出字段的用户数据**:proxy 在 insert 校验时把 auto-gen(function 输出)字段的用户提供数据静默丢弃(`util.go:1865`/`util.go:2321`),不允许用户写 function 输出列——新写入不会毒化输出列,一律由 function 自己算。

**两条 backfill 路径。** 加 function 是 additive;输出列的旧行补齐随 runner 局部性走两条不同路径:

- **本地 runner(BM25/MinHash)—— 写侧 inline 物化。** 稳态新写在 WAL append 路径 inline 从输入算 output(`materializeFunctionFields`);**过渡窗口滞后 vchannel 上写的行、以及变更前旧行**从该 vchannel 视角是"apply 前的行",走 **schema-bump compaction** inline 物化(`bumpSchemaVersionPolicy`,默认关闭)。所以 §10"读/replay 收敛"对 output 列靠 compaction backfill 成立,不是纯 inline。drop / 改 signature 走 加新 id + 删旧 id。
- **远程 runner(TextEmbedding)—— 外部 Spark backfill。** 加**仍是 additive**,但 output **不在 WAL append 路径同步调远程**(会把摄入绑死外部 endpoint,同 §19 crash-loop),一律延后 async backfill,且**不走 datanode schema-bump compaction**。旧行补齐靠**外部 Spark backfill**:外部 Spark 算 embedding → `DataCoord.CommitBackfillResult`(`services_commit_backfill.go:44`)→ 旧段 `DataVersion++` / manifest 前移(`meta.go:1476`/`meta.go:1603`)→ querynode reopen → 旧行可读。**只动 `DataVersion`/`Binlogs`/`ManifestPath`,不碰 `SchemaVersion`。** 因此 TextEmbedding 旧行补齐是**非自动、需外部 Spark backfill**,而非自动物化。schema-bump compaction 只服务本地 runner;对 TextEmbedding 缺列段报错是良性的(不阻碍 Spark、不改 `Binlogs`)。

function-add 的 readiness 需含**写路径 runner 就绪**(仅对本地 inline 相关;远程只需 backfill job 就绪)。

---

# Part D — 节点协同时序

> **说明**:本 Part 是**协同时序视角**——把"加一列 / 减一列"时五类节点如何一步步配合、每种 corner case(尤其长期)如何同时保证**数据正确**与**服务可用**讲清楚。

## 14. 参与者与全局时钟

### 14.1 参与者与各自持有的 schema 状态

| 节点 | 持有的 schema 状态 | 何时前进 | 作用 |
|---|---|---|---|
| **Client** | 无(靠 proxy) | — | 发请求 |
| **Proxy** | cached schema,版本 `v` | rootcoord `ExpireCaches` 推送后重拉;或 cache miss 重拉 | 编译 plan、把 `v` 盖在读写请求上(`collection_schema_version`) |
| **RootCoord** | 权威 `SchemaVersion` | **只在 broadcast 完成后的 ack 回调里**推进(I2) | DDL 串行(resource lock)、broadcast、暴露版本给 proxy(DescribeCollection) |
| **DataCoord** | — | — | 加列旧行的 **backfill job**(远程 runner);compaction 回收被删列 |
| **StreamingNode (SN)** | 每个 vchannel 的 `applied` schema | **apply-at-append**:schema 消息 append 到 WAL 时同步 apply(V2) | 写闸门:按 vchannel 当前版本校验/放宽 insert |
| **QueryNode (QN) delegator** | `applied`(写路径)+ `published`(读路径,不可变 `readySnapshot`,RCU),恒 `published ≤ applied` | `applied` 随 WAL/段加载;`published` 只经统一 publish gate 前进 | 读:只从 `published` 快照服务 |
| **QN segcore** | growing/sealed 段,按 **field id** 存列 | 段级 | 执行;读入口按 plan 引用的 field id 校验(§11.1 ①) |

**两个 id 级不变量(长期正确性的地基)**
- **I1**:DDL 层结构性不可达"原地改一个 live field id 的语义"(类型/nullable/default/analyzer…);改语义 = 加新 id + 删旧 id。→ "field id 在 ⟹ 语义不变",读才能"字段全在就服务"。
- **I3**:`max_field_id` 单调,dropped id **永不复用**;re-add 拿新 id。→ 旧列数据永不被新列重解释。

### 14.2 全局时钟:四条时序保证

> 整套协同的正确性都挂在这四条上,均以 `file:line` / 函数名指明其在代码中的位置。

- **V1**:`Broadcast` 只在**所有 vchannel append 成功**后返回(`BlockUntilDone`)。
- **V2**:schema 消息 append 到某 vchannel 时**同步 apply**(`handleAlterCollection` 在 `appendOp` 前)→ append 成功 ⟺ 该 vchannel 已 apply。
- **I2**:rootcoord 暴露的 `SchemaVersion` **只在 broadcast 完成后的 ack 回调**里推进(`ApplyUpdates`(`meta_table.go:1062`)← ack 回调,broadcast 后)。合 V1+V2 ⟹ **`SchemaVersion` 变 N+1 时,所有 vchannel 早已 applied=N+1**,不需要额外写闸门。
- **推论(proxy 永不超前)**:任何 proxy(含崩溃重启、扩容新 proxy,走 DescribeCollection 直取 `SchemaVersion`)拿到 N+1 时,所有 vchannel 必然已 N+1。→ "proxy 超前于某 vchannel"这半**天然不存在**。

## 15. 加列泳道时序图 + 减列差异

### 15.1 加一列(additive)—— 泳道时序

传播窗口 = **T3–T9**;窗口内**所有 proxy 仍看到 N**(SchemaVersion 到 T8 才推进)。

```
时刻  Client            Proxy(cache=N)      RootCoord(ver=N)         SN(各 vchannel)          QN delegators
──────────────────────────────────────────────────────────────────────────────────────────────────────────
T0   稳态               N                   N                        全部 applied=N            published=N
T1   AlterCol(add f)──►
T2                     ──── DDL ──────────► 取 resource lock
T3                                          change_class=additive
                                            broadcast AlterCol(N+1)─► (逐 vchannel,非原子)
T4                                                                   收到 schema msg:
                                                                     apply-at-append→applied=N+1
                                                                     此后本 vchannel insert 按 N+1
T5                                          ◄── 所有 vchannel append+ack (V1) ──
                                            ack 回调触发
T6                                          (a) 推进 SchemaVersion=N+1  (I2:此刻所有 vchannel 已 N+1)
T7                                          (b) waitCollectionSchemaReady(N+1)───────────────────►
                                                                     (schema msg 也经 WAL 到 QN)
                                                                                              UpdateSchema→
                                                                                              publish gate 过→
                                                                                              published=N+1,
                                                                                              report Ready=N+1
T8                                          ◄── 所有 delegator Ready=N+1 ─────────────────────────
                                            (c) ExpireCaches ──►
T9                    cache 失效→下次重拉→N+1
T10  稳态              N+1                  N+1                       全部 applied=N+1          published=N+1
```

**为什么这样排:** (a) 让 fresh proxy 一旦见 N+1 就写安全(所有 vchannel 已 applied);(b) gate (c),保证 **cached proxy 的 cache 只在所有 delegator 能服务 N+1 后才失效**;fresh proxy 若读到未就绪 delegator,由 not-ready 重试兜底(§16)。

### 15.2 减一列(destructive)—— 与加列的差异

时序骨架同 §15.1,只有两处不同:
- **T3**:`change_class=destructive`。
- **窗口内(T3–T9)的写/读**:超前 vchannel 对滞后请求**不放宽**(见 §16);掉队读引用被删列会被**干净拒绝**(见 §16)。

> **改类型/改 BM25/MinHash signature = 减+加**:drop 旧 id(destructive)+ add 新 id(additive)。两条时序叠加,`max_field_id` 保证新 id 不撞旧 id。

## 16. 写路径 corner case / 读路径 corner case

### 16.1 写路径 corner case(传播窗口内)

单条 insert 按 pk 路由到**一个** vchannel;但一个 InsertRequest 的多行会**分散到多个 vchannel** → 部分 vchannel 超前(N+1)、部分滞后(N)是"部分写"的根源。

| 变更 | 请求(proxy 看 N,盖 v=N) | 打到超前 vchannel(已 N+1) | 打到滞后 vchannel(仍 N) | 结果 |
|---|---|---|---|---|
| **加列** | insert(N),不含新列 | SN 闸门:`v==上一版 且 上次 additive` → **放宽接受**;缺的新列由 segcore growing `Insert` 补 null/default(零补齐代码) | `v==当前` → 接受 | **两边都接受,无部分写,透明** |
| **减列** | insert(N),**可能仍带被删列的数据** | SN 闸门:`v==上一版 且 上次 destructive` → **拒绝**(可重试 `SchemaVersionNotMatch`) | `v==当前` → 接受 | 跨 vchannel **可能部分写**;proxy 判整体失败→重试;窗口收敛(proxy 见 N+1、不再带该列)后正常。**归 §3 用户责任(drop 前先停用)** |

**关键前提(加列无部分写):** 窗口内所有 proxy 都看 N(I2:SchemaVersion 未推进),超前 vchannel 靠放宽+补齐吸收、滞后 vchannel 匹配 → 两边接受。窗口结束时所有 vchannel 已 N+1,proxy 才见 N+1,无 lag。
**限制(窄):** 仅当请求落后 **≥2 版**(两次 additive 极快连发)时,A 在 N+2 拒、B 在 N 接受仍可能部分写;受 DDL 串行速率约束,窗口极窄。

### 16.2 读路径 corner case

Delegator 只从 `published`(版本 P)服务;请求带编译版本 `v`:

| 关系 | 场景 | 行为 | 可用性 |
|---|---|---|---|
| **v == P** | 稳态,绝大多数 | 热路径直接服务,不解析 plan | ✅ |
| **v > P** | proxy 超前 shard(加列后 proxy 已 N+1、某 shard 还没 publish) | delegator 返回**可重试** `ErrCollectionSchemaVersionNotReady`(110) | ✅ LB 重试至 shard 追上 |
| **v < P, additive** | proxy 落后一版,期间只加列 | 引用字段都在 P → segcore 校验通过 → 服务(**I1**:present 字段语义不变 ⟹ 按 P 服务=按 v 服务) | ✅ 透明 |
| **v < P, destructive, 不引用被删列** | proxy 落后,期间删了列,但读没用到它 | 字段都在 → 服务 | ✅ 透明 |
| **v < P, destructive, 引用被删列** | proxy 落后 + 恰好引用刚被删的列 | segcore 读入口:field id 不在 P → `ThrowInfo(FieldIDInvalid=2020)`(InputError)→ proxy LB **早返回、不拉黑**(§11.1 ①) | ⚠️ 这一次**干净硬失败**;proxy cache 由 ack 回调**主动失效**、秒级收敛后按 N+1 重编译 → 真删=干净 "not found",drop+re-add=解析到新 id 成功 |

> **绝不拉黑(核心保证)**:根因是缺字段走默认 `UnexpectedError(2001)`→通用 `ErrSegcore`(非 InputError)→ LB 命中 else **拉黑健康 shard leader**。segcore 读入口把它改成 `FieldIDInvalid(2020)`(InputError),**单独一层**就消除拉黑,与任何 plan 遍历无关。
> **透明重编译(可选、非目标)**:把上面 ⚠️ 那格的秒级硬失败磨平成透明重试,需 proxy 重编译-重试循环(动读热路径),取舍不划算,不做;该场景本就是用户违约(drop 前该停用)。详见 §11.1/§21。

## 17. 节点故障 / 边界 corner case

| 情形 | 会发生什么 | 正确性 / 可用性 |
|---|---|---|
| **Proxy 崩溃重启 / 扩容新 proxy** | 新 proxy 走 DescribeCollection 直取 `SchemaVersion` | 写安全:拿到 N+1 ⟹ 所有 vchannel 已 N+1(I2),永不超前;读:delegator 未就绪则 `v>P` not-ready 重试。**无需额外版本闸门** |
| **QN 崩溃重启** | 重放 WAL 重建 `applied`;重新过 publish gate 重建 `published` | 恢复前对该 shard 的读 `v>P` not-ready 重试;恢复后正常。`published≤applied` 始终成立 |
| **SN 崩溃 / vchannel 迁移** | 该 vchannel 重放 WAL,apply-at-append 重建 `applied`(V2) | schema 消息按 WAL 顺序重放,applied 确定性重建;写闸门行为不变 |
| **schema 消息应用失败(如远程 runner endpoint 不可达)** | pipeline 原地重试 `UpdateSchema`(阻塞该 vchannel 消费,tsafe 停,读仍服务旧 published);耗尽后 **panic**(无 flowgraph recover → 杀进程,WAL replay 再撞) | ⚠️ **已知限制**:crash-loop 整个多租户 QN。目标行为=按 vchannel 冻结、不杀节点(§19.1) |
| **连续 DDL(N→N+1→N+2)** | resource lock **串行**;每次变更等 proxy 收敛(ExpireCaches)后下个才发 | 请求正常最多落后**一版**;节点只需保留 `当前 + 上一版号 + 上次 change_class + 该步 diff`(无累积) |
| **动态列表 drop top-level 名字后又引用它** | 无 id 缺失 → 静默解析到 `$meta`(可能返回历史数据 / delete 打到别的行) | ⚠️ **有意接受、归用户**(§12.1);struct 子字段走严格解析、非 dynamic 直接 "field not found",均不受影响 |

## 18. 长期正确性(稳态 / GC / backfill / compaction)

窗口过后进入稳态,数据如何**最终正确**:

| 方面 | 过渡期 | 长期机制 | 稳态保证 |
|---|---|---|---|
| **加列:旧行的新列值** | 读时缺列由 segcore 补 null/default | 本地 runner:inline / schema-bump compaction 算;远程 runner(TextEmbedding):**外部 Spark backfill**(`DataCoord.CommitBackfillResult`+reopen)渐进补 | 所有行最终有真实值;补齐前读到 null/default 是**合法**中间态 |
| **减列:被删列的旧数据** | 读路径靠版本拒绝,掉队读**不访问**被删列 | compaction **lazy** 回收(与读无协调,无 read-drain/GC watermark) | 物理删除;`max_field_id` 保证 id 不复用 ⟹ **旧列数据永不被新列重解释**(I3) |
| **掉队读安全** | `v<P` 缺字段 → 干净拒绝(§16) | 回收前引用被删列的读都先被拒 | 单版本 worker 足够:destructive 读靠**拒绝**保证安全,不靠服务旧数据 |
| **函数 output 列** | 过渡窗口滞后 vchannel 写的行 + 变更前旧行,走 backfill | 稳态新写 inline(本地)/ backfill(本地 compaction / 远程 Spark) | output 列最终一致 |

**长期不变量收口:** I1(不原地改语义)⟹ present 字段永远可安全服务;I3(id 不复用)⟹ 新旧列物理隔离;读版本-拒绝 ⟹ 掉队引用在物理回收前一律被拒。三者独立,**不依赖 tombstone**。

### 18.1 汇总:可用性矩阵

| 操作 × 阶段 | 写 | 读 |
|---|---|---|
| 加列 · 稳态 | ✅ | ✅ |
| 加列 · 传播窗口 | ✅ 透明(SN 放宽 + segcore 补齐) | ✅ 透明(冷路径字段全在,I1) |
| 减列 · 稳态(不带/不引用删列) | ✅ | ✅ |
| 减列 · 窗口 · 不引用删列 | ✅ | ✅ |
| 减列 · 窗口 · **引用删列** | ⚠️ 重试至收敛(窄窗部分写,归 §3) | ⚠️ 干净硬失败(**不拉黑/不崩**)→ cache 秒级收敛后恢复 |
| 任意 · runner 持久失败 | — | ⚠️ 当前 crash-loop(§19.1 待硬化) |

**一句话:** 加列全程对用户透明;减列**只承诺安全失败**(不崩、不脏、不拉黑),用户 drop 前先停用是契约;长期数据靠 `max_field_id` + backfill + compaction + 版本拒绝四者收敛到正确。

---

# Part E — 失败隔离与边界

## 19. 失败隔离 + `(version, barrierTs)` readiness + 物理回收

### 19.1 失败隔离(独立于过渡模型)

function-runner 初始化失败**不能**杀多租户节点。现状 `delete_node.go` 调 `mlog.Fatal`(注释写 "panic")——经 `zap.OnFatal(WriteThenPanic)` 实为 **panic**,但 flowgraph/pipeline **无 `recover()`** → panic 上抛**杀整个进程**,重启后 WAL replay 再撞同一失败 → crash-loop 整个 querynode。正确行为:**冻结/隔离出问题的 collection/vchannel**(停其 tsafe、继续服务旧 published snapshot、readiness 报 not-ready),其余照常。这需要给 flowgraph 加每-vchannel 失败隔离(recover + 冻结),pipeline 现无 error 传播路径,非一行改动。

### 19.2 readiness `(version, barrierTs)`:同版本语义变更(TTL)

- **cache 过期握手**见 §8(唯一权威描述)。
- **同版本语义变更(TTL)**:刻意不 bump 版本 → 握手必须携带并比较 **`(version, barrierTs)`**,否则旧快照瞬间满足、cache 在 barrier 应用前过期。且 §11 的读服务需要一个 barrierTs 维度或 delegator 执行时按最新 barrier 重算——**这是 I1 的受控例外,需专门处理**(TTL 只改可见性、不改字段身份)。这条 lockstep 跨 proto(`LeaderView`)+ querycoord `CheckSchemaReady` + delegator 上报。

### 19.3 物理回收(GC)

drop 后旧列由 compaction **lazy** 回收,**与读无协调**(§11 版本检查兜住掉队读)。无 read drain、无 GC watermark。代价:回收前继续占存储/索引;drop 罕见,可接受。

### 19.4 失败语义(收敛重试,而非物理回滚)

- **Load/reopen 失败**:`LoadSegments` 返回 error;querycoord 的 checker 重生成 task。Delegator 状态收敛(幂等 `PutOrRef`、singleflight + 只下缺失字段的 stats、单调 gated publish)。失败永不产生读可见状态:applied 可能已推进,published 未推进。Pending 标记在失败时清除,所以被放弃的加载退化为渐进可见而非永久阻塞 publish。
- **UpdateSchema 失败**:阻塞重试后 panic + replay(见 §10/§17;硬化目标见 §19.1)。
- **Runner init 失败**(如远程 embedding endpoint down):`tryPublishReadySchema` 报 error → 外围 retry 循环(pipeline / load / DDL 回调)重驱;读留在上一版本,永不停在半初始化版本。

## 20. 已知限制 / 边界场景

> **定位:本 PR 交付的是"feature-flag 保护下的在线 schema 变更基础能力",不是"生产无停流加减列"的完整方案。** 下列**阻塞项**(P0/P1)在把在线加/减列开放为默认生产能力之前,必须修复或用 feature flag 关闭;**良性项**不破坏 §2 核心安全不变量,只在特定配置/极窄窗口有可用性或成本代价。这些结论来自对 PR 代码的逐条核查(见每条的 `file` 引用)。

### 阻塞项(开放为默认能力前必须处理)

- **[P0] TextEmbedding 可原地挂到已有(可能非空)字段并重解释其语义。** `internal/proxy/function_task.go`(`AddCollectionFunction`)与 `internal/rootcoord/ddl_callbacks_collection_function.go` 只拦"已是别的 function 输出"的字段,**不拦**把 TextEmbedding 挂到已有数据字段——复用其 field id、原地置 `IsFunctionOutput=true`;`internal/util/schemautil/alter_schema_add.go` 侧路已拒 BM25/MinHash,但**未拒 TextEmbedding**。分类器虽把 `IsFunctionOutput` false→true 判为 destructive,但读闸门仍服务旧版本读(field id 仍在),过渡期(backfill 完成前)返回新旧 embedding 空间**混合的静默错误结果**而非干净失败。**修法:与 BM25/MinHash 一致禁止 in-place,强制全新空 output 字段(新 id → backfill → 新索引 → 切读 → 删旧列)。**
- **[P1] 加列窗口内 stale partial upsert 会清空已写入的新列。** partial update 是"读后写":proxy 用其缓存 schema 做 requery 与 merge(`internal/proxy/task_upsert.go` update 分支遍历按 proxy schema 投影的 `existFieldData`、insert 分支遍历 `it.schema.Fields`),都用 proxy 可能落后的 schema → 重写行**不含新列** → segcore 回填 null/default → 覆盖已写入值。**同步 proxy-last DDL(I2 + broadcast 阻塞到 `ExpireCaches`)把单集群窗口压到 `ExpireCaches` 扇出 + in-flight 任务(亚毫秒);但 SN 重启(下条)或滚动升级会把"部分 writer 停在 N、其余在 N+1 写新列"拉长成真实窗口。** 修法:cache 收敛前 drain partial upsert;或 requery 用服务端最新 schema 且 merge 保留未知列。
- **[P1] StreamingNode 重启丢失 additive relaxation → 跨 vchannel 部分写。** `PrevSchemaVersion`/`LastChangeAdditive` 不持久化(`shard_manager.go` 注释明确),恢复只保留最新 schema、二者归零 → relaxation 关闭。若 vchannel A 已到 N+1 后重启、B 仍在 N:A 拒绝 N 写、B 接受 N 写,同一多行 insert 跨 A/B 部分成功;delete 消息不带 schema version、跨 vchannel 非原子,整单重试下 autoID insert 可能生成重复逻辑行、非幂等 partial upsert 可能重放。(§17"SN 崩溃/迁移—写闸门行为不变"那格仅指单点 fail-safe;此为跨 vchannel 不一致。)修法:持久化最近两版 schema/分类,或从 recovery snapshot/WAL 确定性重建。
- **[已实现] 滚动升级期间的 schema DDL 现由 cluster-min-version gate 在服务端强制拒绝。** rootcoord 的 schema-change choke point `broadcastSchemaChange` 在结构准入 gate 之前先跑 `checkClusterVersionForSchemaChange`(`internal/rootcoord/schema_change_version_gate.go`):逐 role 从 session registry 列出存活组件,任一 major 版本 < 3.0 即拒绝该 DDL;fail closed(registry 读不到、或 session 版本无法解析都算拒绝),默认开,留运行期急停开关 `rootCoord.enableSchemaChangeVersionGate`。这把 §9.2 的"混合版本禁 DDL"从**契约**变成了**代码强制**,从而在正向升级(2.x→3.0)窗口内从根上堵住了"新字段静默丢失"这条路径——`internal/querynodev2/pipeline/insert_node.go` 会把当前 schema 里没有的字段 skip 掉(带 warning+metric),但该窗口内既然无法新增字段,携带未知列的 insert 就不会产生。三个稳态 escape hatch(旧 QN 无 `ReadySchemaVersion` 记为 ready `internal/querycoordv2/services.go`、legacy read `version==0` 跳读闸门 `internal/querynodev2/delegator/delegator.go`、legacy insert 无 version 跳写闸门 `shard_manager_collection.go`)刻意保留——它们保护的是既有流量而非 DDL。**残留风险**:gate 以 session registry 的存活性为真相源——若运维手动关闭开关,或存在一个"活着但未注册 session"的旧节点,则不在保护内;旧 QN `UpdateSchema` 重试耗尽仍是 `delete_node.go` 的 `mlog.Fatal()` fail-stop(可观测、非静默继续推 tsafe)。
- **[P1] DDL-ready ≠ backfill/data/index-ready;backfill 提交路径三处风险。** readiness 只等 schema 发布 + function runner init + BM25/IDF 就绪(`delegator.go` `tryPublishReadySchema`),**不等**历史 backfill / QN reopen / function-output 索引构建 / 历史行补齐;DataCoord **不调度** Spark、只经 `CommitBackfillResult` 接收外部结果 → "所有数据最终正确"依赖外部 orchestrator,非本 PR 自动保证。其中:(i) 512 段分批提交,部分批失败时顶层仍返回 `Success`(`internal/datacoord/services_commit_backfill.go`,失败经 per-segment status 暴露);(ii) 缺 job id / snapshot·schema fencing / 严格幂等(proto 仅 `result_path`);(iii) **V3 提交只推进 `ManifestPath`、不更新 `ChildFields`,而 index inspector 依赖 `ChildFields` 判定 function-output 是否有数据 → V3 历史段的 function-output 索引可能永不启动**(需 E2E 证明或修复)。

### 良性项(不阻塞核心安全)

- **本地 runner 缺列段的 schema-bump 失败重试循环。** 当开启 `bumpSchemaVersion`(默认关闭)时,schema-bump compaction 对 **TextEmbedding**(远程 runner)缺列段会无限失败重试,`SchemaVersion` 永不收敛——良性浪费(不阻碍 Spark backfill、不改 `Binlogs`)。代价:CPU/调度浪费,无数据损坏。收敛做法:选段/物化时跳过远程 runner 输出列。
- **BM25 reopen 的 swap→activate 亚毫秒窗口。** reopen 换入新列(swap)与激活其 BM25 stats(activate)之间存在亚毫秒窗口;落在此窗口的 BM25 搜索可能打分偏差或返回空,窗口过后自愈。彻底关闭需在 swap **之前**激活,但那会引入反向的不一致(stats 已激活而列未换入),所以当前取"swap 后紧接激活"的折中,残余窗口极短且自收敛。
- **additive ≥2 版窄窗部分写。** "additive 消灭部分写"仅在请求最多落后一版时绝对成立;两次 additive 极快连发、请求落后 ≥2 版时仍有窄窗部分写(§10.1/§16.1)。受 DDL 串行速率约束,窗口极窄。

## 21. 非目标 / 契约

- DDL 间不做 snapshot isolation。
- **drop 前先停用是用户责任**;destructive 只保证安全失败,不保证透明。
- **读侧 proxy 重编译-重试透明自愈 = 可选优化、非目标**。destructive 陈旧读引用被删字段默认**干净硬失败**(InputError、不拉黑、安全),而非自动重编译;proxy cache 由 AlterCollection ack 回调(`ddl_callbacks_alter_collection_properties.go`,`waitCollectionSchemaReady` 后 `ExpireCaches`)主动失效、秒级收敛后自然恢复。安全(不拉黑/不崩)已由 §11.1 ① 单独达成;透明自愈只把一个秒级、自收敛、且属用户违约的过渡窗口内硬失败磨平成透明重试,用它换掉读热路径最高的改动风险(动 `lb_policy` 重试循环 + 从非幂等的 `PreExecute` 抽幂等重编译)不划算。若将来要透明,轻量做法:只对 `FieldIDInvalid` 做一次 cache 失效 + 重编译重试,不建完整收敛机制。**② 与 ③ 全或全无**——只做 ② → 重试风暴(§11.1)。
- 不引入单请求跨 vchannel 原子性;additive 靠放宽绕开,destructive 是拒绝而非部分应用(部分写属 §3)。
- **dynamic 上 drop top-level 名字后引用它 = 落 `$meta`**,不 loud 报错(归用户;§12.1)。struct 子字段严格解析、非 dynamic 直接 "field not found",均不受影响。
- rename 语义未定,列为后续。

### 21.1 不变量收口(五条 checklist)

- **I1** 不原地 mutate live id 语义(DDL 层堵死)——读"字段全在就服务"的前提。
- **I2** proxy 永不超前(`SchemaVersion` 只在 broadcast 后 ack 回调推进)——写不部分、新 proxy 不超前。
- **I3** `max_field_id` 单调、id 不复用——旧列永不被重解释。
- **I4** 读引用当前无的 field id → 干净拒绝(`FieldIDInvalid`),**不拉黑**。
- **I5** destructive 只保证安全失败;drop 前停用是用户责任。
