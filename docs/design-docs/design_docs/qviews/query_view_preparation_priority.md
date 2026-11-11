# QueryView 准备优先级与协作式资源调度

关联需求：[milvus-io/milvus#55284](https://github.com/milvus-io/milvus/issues/55284)。

状态：2026-11-11 已接通第一版待执行任务优先级。本文区分已实现的排队调度与后续更细粒度协作方案；访问统计的采集与聚合由 [ViewFeedback](view_feedback.md) 定义。

## 当前实现范围（2026-11-11）

用户明确第一版只改变尚未执行任务的顺序，不抢占正在执行的任务。已实现：

- `QueryViewControl.preparation_priority` 使用 `sint32` 字段 2，取值 [-20, 20]，0 缺省、越大越优先；创建后 immutable。Builder 拷贝 Control，Coord lease 控制只加强 lease 字段；SN 同 identity 重发保留首次接收的准备优先级，包括旧消息省略字段的情况。Coord/SN 恢复使用持久化值。
- Coord 正常规划、published-schema 恢复规划和手工迁移创建新 View 时计算分值。既有 View 不因统计变化被重建或重排；同一操作保留当前配置下各 replica 的不可变反馈快照，并按 RG 汇总最近 6 个窗口的 rate，分子与分母使用同一组快照。
- `nodeScheduler` 的 `WithPriority(int32)` 和 `PriorityTaskHandle.SetPriority(int32)` 使用同一执行器。通常按分值、同分 FIFO；每 8 次优先派发后给最老 runnable 一次机会。SetPriority 保留排队年龄，不提前 retry deadline，不取消或重新启动 Execute。Running 的新有效值仅用于后续 retry。
- QN 保存每个 SegmentResource 的任务句柄，对具有已解析依赖、尚未准备、未失败、未处于 retry/catch-up 等待的 binding 取 max；所有分值为负时仍取负值。依赖加入、释放、失败时重排待执行任务。资源内先发布可复用 read，再按优先级选择 eligible binding，同分保持 cursor 顺序，每 8 次优先选择给轮转起点一次机会。一次 reconcile 至多执行一次物理准备后重新入队，沿用原重试 timer 与 cleanup。
- SN 共享 build 取未完成依赖的最大值，依赖移除时可以降分；优先级不改变最老 View 的构建快照。逐 View ready task 使用其固定值。WAL scoped handle 透传 SetPriority，不改变任务追踪与关闭语义。

当前共享调度器完整引入 PR #55398（b36d566118），替换原本地实现。除优先出队外，它还按 ≤-10 / ≤-1 / ≤20 对运行任务设置 50% / 75% / 100% 的累计 worker 上限（向下取整、至少 1）。低档不能借用高档保留容量，公平派发也不能越过容量上限。分值变化因此同时影响出队顺序和后续 admission 的容量档位；已运行 attempt 仍按原档计费。

ACK 固定为 20，release 固定为 5，data 默认 -5；recovery-tail 压力目标内尚未执行的 data 任务可提高到 5。QueryView build/ready 的默认 0 由 View 分值或共享依赖最大值替代。达到 20 的 View 与 ACK 同档竞争，保留容量不再是 ACK 独占。

当前策略以用户需求选择场景，再叠加场景内流量分和配置偏移。此前 LOW=-10、独立活动时间加分和线性 rate 加分规则已替换。

| 场景（按判断顺序） | 基础分 | 流量分上限 | 含配置后的范围 |
| --- | --- | --- | --- |
| 近期访问，缺少存活 UpView | +10 | 8 | [10,20] |
| 有待完成的 AutoLoad/readiness 请求 | +5 | 0 | [5,7] |
| 近期访问，已有存活 UpView | 0 | 2 | [0,4] |
| 无近期访问，已有存活 UpView（冷 handoff） | -10 | 0 | [-10,-8] |
| 已知长期无访问，缺少存活 UpView（冷恢复） | -5 | 0 | [-5,-3] |
| 无访问历史、无存活 UpView、无 AutoLoad 需求（首次/手动加载） | 0 | 0 | [0,2] |

- 配置偏移：HIGH（含 proto 默认值）+2，LOW 0，未知 enum 0。不使用 enum 底层数值。
- 近期访问：LastAccessTime 非零，距离本轮规划时间 ≤10 分钟；未来时间按近期处理。LastAccessTime 负责场景选择，不另加一个活动分。
- 流量映射：`share=clamp(replica.Last6Windows / same_RG_total,0,1)`，`heat=log1p(100*share)/log1p(100)`，`traffic=round(limit*heat)`。分母 ≤0 时流量分为 0。
- 总分为 `clamp(base + traffic + config, -20,20)`；当前策略实际产生 [-10,20]，wire/scheduler 合法范围仍是 [-20,20]，不要求用满。
- 最紧急档中，1%/5%/10%/50%/100% 占比分别得到 1/3/4/7/8 流量分。配置 +2 不会跨越主要场景之间的顺序。

ViewFeedback 将超过 2 分钟的窗口清理，但保留 replica 的最后活动时间直到 replica 撤销。窗口归零不等于变冷：仍在 10 分钟内就保留该场景基准分。最近 6 窗口不假定固定 6 秒。缺失统计不能证明长期无人访问；没有历史的缺 UpView 场景使用首次/手动加载基准，不凭空创造访问记录。

**尚未实现的细分方案：** 下文第 6、12 节的独立 Yield/Wait/Event 接口、collection 分组公平，以及 Transform catch-up 独立 drain 池、IDF permit 和 live-event dispatcher 内部的优先级细分。第一版保持这些已有执行边界与预算；一旦 Execute 已开始，其中的 I/O/native/模块阶段不会因更高分到达而中断或重新 admission。因此当前实现证明主要准备任务的排队优先级，不表示所有下游队列都能按 View 优先级排序，也不承诺墙钟恢复延迟。


## 1. 目标与已确定约束

资源竞争时，Coord 能根据用户配置及 collection 的访问活跃度，为新 QueryView 指定准备优先级。QN/SN 将该值传递到真实资源任务，通过协作式调度优先推进更紧急的准备，预期改善故障恢复时的资源分配。

核心约束：

1. 优先级是范围 `[Pmin, Pmax]` 内的有符号整数，`Pmin < 0 < Pmax`。0 为默认基准，负数更低，正数更高。
2. 它是绝对分值，不是增量命令；收到 `+10` 始终表示值为 10。
3. **同一个 QueryView identity 的优先级 immutable**。创建时计算，此后准备、重试、重发与恢复均保留原值。
4. Coord 使用目标 replica 的 LastAccessTime 和最近 6 个窗口的 rate，以同一 RG 内所有 collection 的所有 replica rate 总和计算占比。SN 通过 GetQueryPlan 采集，ReplicaFeedbackStore 提供 replica 快照，生命周期独立于 View；Balancer 不扫描 shard/window，不恢复旧 Contribution/Registry，QN 不新增访问统计。
5. RG 内制定策略，不创建跨 RG 的唯一恢复目标或串行屏障。无实际资源竞争时并行推进。
6. 优先级不改变 placement、readiness、schema/MVCC、lease、引用计数或 WAL 顺序。

issue 最初提出的“已有 View 准备过程中调高/调低优先级”不属于本方案。统计持续更新，但只在正常流程创建新 View 时被消费，不为重新设置分值而替换 View。

## 2. 原设计的接入点清单（完成情况以开头实现范围为准）

| 位置 | 当前行为 | 需要实现 |
| --- | --- | --- |
| `pkg/proto/view.proto` | QueryViewControl 有 `preempt_serving_lease` | 追加 immutable 准备优先级字段，旧值缺省为 0 |
| `internal/views/coord/balancer/` | 规划 View/placement，没有读取 replica Priority 进行准备排序 | 创建 View 时读取域统计、用户配置并计算固定值 |
| `internal/views/coord/coordview/` | View 持久化后同步到节点 | 保持 priority 的创建、持久化、重发和恢复一致性 |
| `pkg/util/nodescheduler/` | runnable FIFO，延迟任务按 nextRun 排序 | 数值优先级、公平选择与协作式继续/等待语义 |
| `internal/querynodev2/qnview/` | Segment 资源任务与资源内 binding 轮转 | 将依赖 View 的优先级传入资源间、资源内选择 |
| `internal/querynodev2/transformlogbuffer/` | 单独的 catch-up drain 队列 | registration 继承依赖优先级，保持批次让出和顺序 |
| `internal/streamingnode/server/wal/vchannel/queryresource/` | 共享 runtime build 和逐 View readiness | 共享任务继承、可继续的阶段执行、依赖等待让出 |
| SN IDF/事件应用等 admission | 有独立 permit/dispatcher | 在实际竞争点接入相同调度输入，保持各自预算 |

`LoadReplicaConfig.priority` 当前被保存在 load 配置并投影到管理数据，但没有在 QueryView 准备调度中生效。DataCoord 的 `query_view_segment_load_info.go` 当前为 QV Segment 和索引加载填写固定 HIGH。本设计让 replica 配置成为 **Coord 计算新优先级的输入**，不是沿用现有 QV 排序行为。

## 3. 优先级表达、持久化与兼容

### 3.1 Control 字段

建议字段形态如下；实现前核对 proto 编号：

```protobuf
message QueryViewControl {
    bool preempt_serving_lease = 1;
    sint32 preparation_priority = 2;
}
```

业务数值范围不必占满 sint32。比较时数值越大越优先；差值不自动表示资源份额倍数。负值仍是可执行任务，不表示暂停或拒绝。

Control 的字段有各自语义：lease 控制可按现有规则加强，准备优先级固定。现有将整个 Control 替换的路径必须改为保留 priority；Control 的注释也不能继续把所有字段描述为同一种单调控制。

### 3.2 创建链路

```text
Balancer 正常规划出新 QueryView
  -> 读取目标 replica 的统计快照、同 RG 总 rate 及 replica 配置
  -> 计算并固定 preparation_priority
  -> View 持久化
  -> SyncQueryView 下发同一 View
  -> QN/SN 首次 Acquire 时保存固定优先级
  -> 节点资源管理器计算实际任务有效优先级
```

优先级在 View 发布前确定，先持久化后同步沿用既有 dirty flush 规则。一次已提交 View 的消息重试不重新运行评分函数；同 identity 的重复 apply 不覆盖资源注册保存的 priority。

QN/SN 将固定值与当前 acquisition 一起保存。不能为重排而替换需求 Meta 指针，尤其 SN `prepareViewRef` 用 Meta identity 与 runtime identity 识别注册代次；不能重新 Acquire/Release、重开订阅或丢弃准备进度。

### 3.3 不可变性和升级

- 不新增 UpdatePriority、priority control revision、applied-priority ACK 或更新专用重传。
- 用户配置和统计变化只影响以后创建的 View。正常故障/版本演进确需新 View 时，使用当时输入重新计算。
- Coord 和 SN 恢复同一 View 时使用持久化值。SN 本地 UpRecovering 不另加临时 View 优先级。
- 旧数据或旧消息缺少字段时为 0；旧节点忽略新增字段。混合版本期间调度效果可能不完整，但不能阻断原有生命周期。
- 同 identity 的非零值不会被旧格式重发中的缺省 0 静默重写。正常创建/恢复路径应保证一致值；不建立“最后一次消息获胜”的优先级协议。

在此约束下，故障发生后已有普通 Preparing View 不会就地升级，已有高分 View 也不会因环境变化自动降分。这是明确接受的简化，不用额外创建 View 绕过。

## 4. Coord 如何计算分值

### 4.1 统计与策略范围

统计输入来自 ReplicaFeedbackStore 的不可变 replica snapshot：

- 最后活动时间：目标 replica 的 `LastAccessUnixNanos`。
- 分子：目标 replica 的 `RequestRates.Last6Windows`。
- 分母：当前配置中同一 RG 内所有 collection 的所有 replica 的 `Last6Windows` 之和；缺少 snapshot 的 replica 贡献为 0。不是 collection 内总和，也不是 Store 的集群级 `GetGlobalRequestRates()`。

Store 已将各 shard 的窗口 rate 按 collection 的全部 shard 数平均成 replica rate（未上报 shard 按 0）。Balancer 直接消费这个 rate，不重新换算为请求数。有效性由 Store 的 2 分钟窗口策略负责，不额外引入报告超时。

每次规划/手工操作首次需要评分时，遍历当前配置的 replica，保存不可变 snapshot 指针并累计各 RG rate。初始化时间和临时空间为 O(replica 数 + RG 数)，随后每个 View 的统计项读取 O(1)，健康判断遍历该 UpView 的 QN 列表，不扫描反馈 shard 或窗口。各 replica 的读取并非跨 replica 原子事务，但一次操作内保留相同快照，分子与分母不会因后续反馈而变化。

新 View 不需要自己的查询历史。同一 replica 在一次操作中创建的 View 共享统计输入；同一 collection 的不同 replica 使用各自的热度。没有 snapshot 时结合加载需求与 UpView 状态选择场景；分母为 0 时只取消流量加分。

已有 QN placement 按 `(CollectionID, ResourceGroup)` 规划，同 collection 的 replica 目标节点集可互斥，不同 collection 可共享节点。因此 RG 是策略范围，实际优先级选择发生在有竞争的节点/资源队列上，不能因为 RG 内另一 View 未完成就闲置独立执行资源。

### 4.2 场景评分

```text
recent = LastAccessTime != 0 && now - LastAccessTime <= 10min
(base, limit) = SelectScenario(recent, healthyUp, pendingAutoLoad, knownHistory)
share = clamp(replica.Last6Windows / same_RG_total, 0, 1)
traffic = round(limit * log1p(100 * share) / log1p(100))
P(view) = clamp(base + traffic + ConfigOffset, -20, 20)
```

具体场景与固定参数见开头映射表。总 rate ≤0 时 traffic=0；无 snapshot 或零 LastAccessTime 代表没有访问历史，不等同于已知长期无人访问。没有额外采集 NQ、执行成本或准备进度。

同一 replica 的多个 shard 共用访问反馈，但 UpView 健康状态按 `(ReplicaID,VChannel)` 分别判断，因此可获得不同分值。同一 RG 的其他 replica 即使存在 UpView，也不会抵消目标 shard 的恢复紧急程度。配置只提供 +2 偏移，不颠倒主要场景顺序。

### 4.3 UpView 与节点健康

复用 Balancer 的 `healthyUp`：ShardStats 必须存在 UpVersion，UpNodes 中每个 QN 必须仍在节点快照中且 Alive。QN 已掉线但 Up 元数据尚未清理时，仍判定缺少健康 UpView；Stopping 仅禁止新 placement，不使仍存活的 UpView 失效。正常规划和手工迁移都捕获节点快照，不因手工路径缺少节点数据而误报掉线。

这里消费既有状态和拓扑，不通过 ViewFeedback 再建立可服务状态。SN 故障由现有 PChannel assignment/恢复链路处理，不向 QN 节点表混入 SN。恢复同 identity 的 SN UpView 仍保留持久化 priority，不因故障重打分；此限制来自 immutable 约束。本次不改变 SN 故障恢复或 View 生命周期。

### 4.4 AutoLoad 需求的生命周期

AutoLoad 的入口是 QueryCoord `EnsureCollectionReady`。在共享 singleflight 操作开始时登记 collection 的 `AutoLoadDemand.Begin`，在成功、错误、服务停止或操作超时退出时释放。已存在 LoadConfig 但尚不满足 readiness 的请求也登记，因为用户此时确实在等待。已经 Ready 的快速路径不登记。

`AutoLoadDemand` 是独立的内存需求标记，按 collection 计数，`Pending(collectionID)` 提供 O(1) 读取。它不修改 ViewFeedback、不伪造 LastAccessTime/rate、不持久化、不创建 goroutine/timer。并发登记只有最后一次释放才移除；单个 Proxy 取消不提前清除仍在执行的共享加载操作。Coord 重启后由重试请求重新登记，不保留永久 AutoLoad 属性。

默认策略通过 `WithAutoLoadDemand` 注入；本轮首次对某个 collection 评分时捕获其 pending 值，后续复用。未注入反馈读取器时不扫描全部 collection；AutoLoad 仅按需读取目标 collection。AutoLoad 可覆盖已知冷历史，表示新出现的查询需求；近期访问且缺 UpView 的故障恢复仍比它更紧急。手动首次加载无历史时自然落在普通档，不额外建立持久化的手动加载标记。

已接受的 Preparing View 不因新 AutoLoad 请求到达而重新创建或调分。统计和请求需求只影响以后正常创建的 View，配置与场景输入不会被节点重新计算。

## 5. 固定 View 优先级与共享任务优先级

资源管理器负责把 View 需求映射到实际资源工作。对可推进一组需求的共享任务 R：

```text
effective_priority(R) = max(P(V) | V 仍需要 R 的当前工作阶段)
```

单个 View 的 P 不变，但依赖集合会因加入、满足、失败或释放而变化。因此共享任务有效优先级可以变化，这是节点内部的聚合，不是 View 控制更新。

例：资源原本只服务 P=-10 的 View A，后来 View B（P=20）需要同一工作，任务变为 20。B 的该项需求满足或退出后，任务按剩余依赖重新计算。所有依赖都是负值时结果仍为负，不能以 0 初始化 max。

聚合必须按兼容物理资源和工作阶段判断：

- QN 不能只因 SegmentID 相同，就提高另一种不兼容 manifest/schema/read requirement 的无关工作。
- SN 一个 VChannel 的 runtime 可被多个 replica/RG 的 View 共享；只执行一次共享准备，满足多个依赖。逐 View 的 load scope 等独立工作使用各自需求。
- 已 Ready/Up 的引用仍保护资源生命周期，但已满足的准备需求不能永久提升无关准备任务。
- 没有剩余准备依赖时移除准备任务；如果还有服务/释放用途，转由对应既有职责管理，不能随意关闭被引用资源。

队列内有效值改变时必须重新定位任务。仅让 heap comparator 读取可变原子值不能维护堆序。资源管理器在自身锁下更新依赖，向调度器提交已计算值；调度器不能持锁反向调用资源管理器。

## 6. 协作式调度契约

### 6.1 执行单位

QN/SN 共享调度语义与通用能力，各自实现资源任务。不是跨节点全局队列，也不要求把 CPU、I/O、Transform 和 live-event 的资源预算合并成一个池。

一次执行推进一段有界工作，并返回以下概念结果；Go 接口和错误/结果表示在实现时确定：

| 结果 | 语义 |
| --- | --- |
| Done | 本任务完成，释放执行槽并通知依赖者 |
| Yield | 保留进度，释放执行槽，按当前有效优先级重新排队；不增加错误退避 |
| Wait | 注册依赖通知后退出 runnable 队列，释放执行槽，依赖满足后唤醒 |
| RetryAt | 暂时失败，按原有 backoff/deadline 延迟，到期后才参与排序 |
| Terminal / Cancel | 遵循现有失败或 owner 退出处理，终止工作并清理所属引用/回调 |

任务任一时刻最多运行一次，唤醒与重复 submit 合并。检查依赖与注册等待需要无丢失唤醒的契约，依赖先完成或后完成都能继续。

Yield 不是成功完成，也不是失败重试；Wait 不是让一个 worker 阻塞在依赖 future 上。不能通过 Cancel+Submit 重排任务并重置 backoff、引用或 replay 游标。

### 6.2 保留进度与依赖等待

可继续的步骤将游标、已完成阶段和未发布的中间资源保存在任务/资源 owner 中。在一个步骤完成、下一段开始前参与调度。正在进行且暂不可切分的 native/load 调用允许完成本次调用，再让出；不承诺任意指令处抢占或固定毫秒内响应。

等待 I/O permit、共享加载结果、WAL coverage 或 barrier 时应退出通用 runnable 队列。实际 I/O 仍有自己的受限执行容量；不能通过无限 goroutine 规避并发和内存限制，也不能同步等待同一个耗尽执行池中的子任务。

Yield/Wait 保留的资源继续计入内存预算，不将暂停等同于释放。已启动的一次 I/O 保留其 permit 到完成，下一次 admission 再接受优先级选择。释放/取消必须能唤醒等待并按既有 ownership 规则收尾。

协作让出保留正确的中间状态；真正执行失败仍遵守原有重试契约。例如 SN build 的失败可能要求丢弃部分 runtime 并重新捕获快照，不能把错误恢复和正常 Yield 混为一谈。

### 6.3 公平性

优先级表达顺序，不直接充当权重。推荐第一版采用“优先选择 + 有界连续优先派发”的规则：

1. 通常选择有效优先级最大的 runnable 任务。
2. 相同优先级在逻辑需求组之间轮转，组内维持稳定顺序；同一 View 的大量 Segment 不应获得无限额外份额。
3. 每连续 K 次优先派发，给等待最久的可运行组一次机会，防止普通/负优先级任务长期没有进展。

K 与组内执行 quantum 是待定参数。资源管理器提供稳定的 fairness key；共享工作只占一个任务位置，不能按依赖 View 数量重复入队。可按 collection/vchannel 形成需求组，具体 key 与已有 owner 结构一起确定。

这是派发机会保证，不是墙钟延迟或 CPU 份额保证；长时间不可切分调用仍限制响应速度。处于 Wait/RetryAt 的任务不因等待年龄跳过依赖或 backoff。

SN 的 WAL 推进、持久化与释放同样需要执行机会。[TransformLog 设计](../wal/transform_log.md) 已记录 bootstrap 等待占满 NodeScheduler 的风险。协作等待是必要机制；多 worker 场景可另限制 preparation 同时占用数量，不能只排序队列。并发为 1 时也不能让任务同步等同池子任务，不能用零准备配额处理保留容量问题。

## 7. QueryNode 接入

参考 [QN 资源准备](qnview/querynode_queryview_resource_preparation.md)。

| 调度位置 | 实现要求 |
| --- | --- |
| `SegmentResourceManager.Acquire` | 在注册 View 需求时读取固定 priority，保留原有同步注册/引用保护 |
| `segmentResource` runnable 任务 | 汇总可由本资源推进的依赖，选择节点内下一项工作；共享集合变化时更新队列有效值 |
| `nextBindingLocked` | 先发布已有兼容 ready read，再在 eligible binding 中按优先级与公平规则选择；blocked 的高分需求不能堵住其他可执行需求 |
| Segment load/read/index 操作 | 保留原有准备结果，在可继续的安全阶段之间让出；逐步细分粗粒度调用 |
| Transform buffer drain queue | 将实际 registration 的依赖优先级带入批次选择，每次有界推进；单 registration 的 Delete/SyncUp 应用顺序不变 |
| collection/schema/metadata 前置依赖 | 作为依赖等待，不占着通用 worker；若有实际排队 admission，传递对应需求优先级 |

同资源准备与不同资源准备都需要选择规则。只改变节点任务队列而保留任意 binding 选择，会让高分 View 得到一个执行槽后仍推进无关低分工作。

底层 native/对象存储执行器不一定已经支持新的数值优先级。第一步需控制新工作的 admission 和自然协作边界；不能因为传入 Control 就宣称运行中的 CGO 或已提交底层 I/O 可被抢占。无需将新有符号分值强制塞入旧 HIGH/LOW 枚举。

## 8. StreamingNode 接入

参考 [SN 资源管理](snview/streamingnode_resource_manager.md)、[Growing Runtime](snview/growing_segment_runtime.md) 和 [IDF Runtime](snview/idf_oracle_runtime.md)。

### 8.1 共享 runtime 与逐 View 任务

`PChannelRecoveryManager` 按 VChannel 路由到资源 module；一个 runtime 服务多个 View。共享 build 使用依赖 View 的最大固定优先级，逐 View readiness 使用本 View 的需求及共享子任务继承。

优先级不改变共享 build 的最老需求基线、DataVersion admission 或历史保留条件。不能直接用分值最高的更新 View 替换构建快照，导致较老依赖失去所需资源。

### 8.2 协作边界

- runtime 初始化按可继续的模块/资源阶段推进；历史 replay 在有界批次后可以让出，保持接收游标和未发布资源。
- 逐 View readiness 的 partition stats、load scope、schema/IDF/index 准备需要识别共享等待，避免等其他任务时占据 worker。
- IDF 文件加载在申请 permit 时考虑依赖优先级；正在运行的低分任务不能不经重新选择就连续申请后续所有文件。保持 permit 覆盖加载/解码/合并的现有内存约束。
- live-event dispatcher 若承担准备所需的应用 barrier，应在有界事件批次之间重新选择 runtime。保持每个 VChannel 内事件串行，不能重排 Delete/DDL/barrier，也不能饿死已经服务的 runtime。
- 本地 UpRecovering 读取持久化 View 的固定值，不重新计算或追加临时分值。

SN 仍共享物理执行器，不按 RG 复制 runtime 或引入新 RG 配额。不同 RG 的业务统计独立，一个共享 runtime 的准备完成可以同时帮助多个 RG。

`recovery/scopedTaskScheduler` 会包装任务。优先级/等待句柄的设计必须穿透该包装并保留 owner 取消、追踪与 Close 等语义，不能只给内层任务加接口而在包装后失效。

## 9. 实施顺序

1. 完成独立的 [SN 访问统计与 Coord 聚合](view_feedback.md)，验证采样和统计未知的回退行为。
2. 增加固定 priority 字段及创建评分入口，打通持久化、恢复与首次 Acquire；处理 lease Control 更新保留字段。
3. 扩展通用调度与必要等待/继续机制，先验证去重、取消、retry、公平和共享继承。
4. 接入 QN 资源任务、binding、Transform catch-up，以及 SN build/readiness、关键 permit/dispatcher。
5. 对粗粒度 native 调用及下游队列记录实际限制，在故障实验中验证哪些竞争点已经受控。

实现可分批提交，但不能将只改变发送顺序或只有主队列排序描述为完整的协作式准备优先级。具体已实现范围与限制见开头；验证证据记录在 docs/plans。

## 10. 验证要求

### 10.1 功能与并发

- 默认 0、正负边界、分项截断和溢出；相同输入得到相同分值。
- 创建后统计/配置变化不修改已有 View；重发、retry、Coord/SN 恢复保留原值；lease 更新不覆盖 priority。
- 共享依赖加入、满足、失败、释放后正确继承/撤销；全负依赖、兼容性不匹配和无人等待均有覆盖。
- 高分任务在协作边界获得机会；低分任务有保底进展；同分轮转、大 View 不垄断；高分 blocked 不阻塞 eligible 低分任务。
- Yield/Wait 不完成 handle、不丢进度、不重置 backoff；无丢失唤醒、重复执行或取消后迟到回调污染。
- 单 worker、动态并发变更和 owner Close；SN 多 bootstrap 等待时 WAL/持久化/释放仍能推进。
- QN ordered Transform 和 SN ordered live-event/retention/schema/readiness 契约不变。
- 新旧消息与旧节点的缺省行为，不因不支持优先级而阻断生命周期。

### 10.2 故障恢复收益

在相同 placement、数据规模、并发和冷/热缓存条件下，对照关闭/开启准备优先级：

- 同 RG 中活跃与冷 collection 同时需要准备；检查活跃 collection 的恢复等待和持续查询成功时间。
- 不同 RG 各自恢复，确认没有跨 RG 人为串行化；同 RG 独立 QN 资源也可并行。
- 同 VChannel 多 View 共享 SN runtime；验证只做一次共享准备且固定分值继承正确。
- QN 丢失、SN/WAL owner 更换、慢存储、Transform 积压、大 Segment 和 BM25 文件竞争；同时观察普通任务及 WAL 进展。

最终以故障到持续查询成功的时间衡量收益，区分检测、资源排队、实际准备和原有路由恢复成本。不把任务先出队、Ready 回调更早或构建成功当作恢复收益证明。已有 View 不变分值和不可切分调用造成的限制应一并记录。

后续按仓库 build/cluster/test/feature-verify skills 执行验证，新增/修改生产函数 statement coverage 至少 90%；本文没有运行结果或性能提升结论。

## 11. 已定参数与后续讨论

- 第一版范围、+2 配置偏移、需求场景、10 分钟近期阈值、最近 6 窗口 RG 内 rate 占比的对数映射与未知统计策略已确定，见第 4 节及开头映射表。
- 当前 scheduler 每 8 次优先派发后进行一次年龄公平派发；collection fairness key 和细分资源阶段的协作 quantum 留待后续。
- 第 12 节接口命名与兼容适配的实现细节，以及各 native/I/O 阶段的可继续边界。

这些参数与实现接口可以继续评审；已固定的架构边界是：SN 两项访问统计、Coord replica 反馈与 RG 内 rate 占比、创建时固定数值、节点协作执行及共享依赖继承。

## 12. 资源准备调度器接口与实现方案

本节给出可直接据以拆分实现的接口契约，符号是拟新增 API。优先扩展 `pkg/util/nodescheduler` 的同一个执行器，不再新增一套无关联的 preparation worker pool。

### 12.1 提交与任务句柄

保留现有 `Scheduler.Submit(Task, ...Option) TaskHandle` 和 `Task.Execute(context.Context) error`。新增显式协作接口，避免仅靠类型断言发现新方法而被 WAL scoped wrapper 遮蔽：

```go
type Priority int32
type FairnessKey string // 资源 owner 提供稳定分组；调度器不理解 View/RG

type CooperativeScheduler interface {
    Scheduler
    // 非阻塞 admission；closed 时拒绝，不取得 task ownership。
    SubmitCooperative(task CooperativeTask, opts ...Option) (CooperativeHandle, error)
}

type CooperativeHandle interface {
    TaskHandle // Cancel(), Wait(context.Context) error

    // 仅更新此资源任务的有效值，不更新 View Control。
    // 入队/等待/执行期间均可调用；已终止返回 false。
    SetPriority(priority Priority) bool
}

func WithPriority(priority Priority) Option
func WithFairnessKey(key FairnessKey) Option
func GetCooperative() CooperativeScheduler
```

`GetCooperative()` 与现有 `Get()` 返回同一 nodeScheduler 实例，共用并发配置和 worker。现有任务默认 priority=0，原有 WithBackoff 继续有效。准备资源 owner 保存成功提交的句柄；不能像当前部分 Submit 调用一样丢弃句柄，否则无法更新共享任务的有效值或正确取消。

同一资源 owner 序列化 SetPriority 调用，例如在资源锁下计算并提交；调度器只接受数值，不在自身锁内调用 owner。旧 acquisition 的句柄不能修改后来注册的新任务。对于尚未到期的 retry 或等待依赖的任务，SetPriority 只更新数值，不将其提前变成 runnable。

SubmitCooperative 成功后调度器取得任务执行 ownership；拒绝时不调用 OnFinished，提交者负责撤销此次 admission/引用。现有 Submit 的接口和关闭返回行为保留，通过内部 adapter 进入同一执行状态机。

### 12.2 协作步骤与完成回调

```go
type StepState uint8
const (
    StepDone StepState = iota
    StepYield
    StepWait
    StepRetry
    StepFailed
)

type StepResult struct {
    State      StepState
    Dependency WaitToken // StepWait
    NextRun    time.Time // StepRetry；zero 使用既有指数退避
    Err        error    // StepRetry / StepFailed
}

type CooperativeTask interface {
    RunStep(ctx context.Context) StepResult

    // 成功提交的任务恰好调用一次，包括首次执行前被取消的任务。
    // 在 scheduler/event 锁外执行，用于发布结果、通知 owner。
    OnFinished(err error)
}
```

可提供 Done/Yield/Await/Retry/Fail 构造函数，避免调用者构造互相矛盾的结果。RunStep 的 context 属于整个任务，Yield/Wait 不取消它，也不创建新任务 identity。

| 结果 | scheduler 行为 | 句柄是否完成 |
| --- | --- | --- |
| Done | 标记终态，OnFinished(nil) | 回调返回后完成 |
| Yield | 保存同一 entry，重新参与公平队列，不推进 backoff | 否 |
| Wait | 安装一次等待订阅，退出 runnable | 否 |
| Retry | 保留 entry/backoff，进入 deadline heap | 否 |
| Failed | OnFinished(Err)，不隐式重试 | 回调返回后完成 |
| Cancel | 取消任务 ctx，撤销等待；运行中步骤退出后 OnFinished(ctx.Err()) | 回调返回后完成 |

OnFinished 只做短时结果发布/内存通知，不执行 I/O、不等待本执行池的其他任务，不自行遍历释放大量资源。复杂资源清理由 owner 的独立 drain/释放流程负责。该回调不与同任务 RunStep 并发。

TaskHandle.Wait 保留既有“等待任务结束”的含义，返回调用者的等待错误，不把业务执行错误改成既有接口的新语义。业务结果由 OnFinished 发布到资源 owner；需要 Result 的 resourceBuildTask 在该处保存结果并关闭自己的完成通知。

### 12.3 无丢失唤醒的依赖接口

```go
// Event 属于 dependency owner，只保存变化代次和等待订阅。
type Event struct { /* private */ }
type WaitToken struct { /* event identity + observed generation */ }

func NewEvent() *Event
func (e *Event) Observe() WaitToken
func (e *Event) Signal()
```

典型步骤先取 token，再检查依赖：

```go
token := dependency.Changed.Observe()
value, ready, err := dependency.TryResult() // 不阻塞
if !ready {
    return StepResult{State: StepWait, Dependency: token}
}
if err != nil {
    return StepResult{State: StepFailed, Err: err}
}
return task.advanceOneStep(value)
```

Event 由依赖 owner 在状态改变后 Signal。Observe 必须发生在检查条件之前；反向顺序可能错过变化。通知不承诺依赖成功，唤醒后 RunStep 重新检查，失败由原有业务分类处理。

调度器内部注册等待时，Event 在自己的短锁内比较代次：已经变化则返回“立即可重试”，尚未变化才登记唤醒函数。注册函数不内联调用唤醒回调；Signal 在 Event 锁外调用已摘下的回调。这样无需为每个等待项创建 goroutine，也不需要轮询。

entry 带 wait generation，唤醒只在 entry 仍为 Waiting 且 generation 匹配时入队一次。取消/重新等待后旧 callback 即使迟到也无效。取消撤销订阅；Event/任务/handle 终止后不得互相持有引用。多个前置条件可以由资源 owner 用自己的一个变化 Event 汇总，不为首期增加通用 DAG 调度系统。

### 12.4 内部状态、队列与公平选择

```text
Queued -> Running -> Done / Failed -> Completing -> Finished
                 -> Yield -> Queued
                 -> Wait -> Waiting -> signal -> Queued
                 -> Retry -> Delayed -> deadline -> Queued

Cancel: Queued / Waiting / Delayed -> Completing
        Running -> canceled context -> step returns -> Completing
```

每个成功提交的 entry 保存：task/context、handle、effective priority、fairness key、状态、ready sequence、队列索引、retry backoff/deadline，以及等待 token/generation/退订句柄。不会复制 task 以创建另一条继续执行任务。

建议数据结构：

- `groups[(priority, fairnessKey)]`：本组 runnable entries 按 ready sequence 排序；可用带索引的 min-heap，便于 priority 改变后保留原等待顺序。
- 非空 priority bucket 的 max-heap；每个 bucket 内的 group 做 round-robin。
- group 最老 runnable entry 的 age min-heap，支持每 K 次优先派发后选择最老 group。
- deadline min-heap 和一个 timer，沿用当前延迟重试设计。
- Waiting 表及 Event 订阅；不进入 runnable/deadline 队列，不消耗 worker。

普通派发选最高 priority bucket 的下一个 group，再取该组最早 ready entry；公平派发选 age heap 的最老 group。出队和 SetPriority 都在 scheduler 锁内维护所有受影响索引，不通过全队列扫描找任务。

SetPriority 对 Queued entry 做 remove/reinsert，保留本轮等待起点，防止重排洗掉等待年龄。Running/Waiting/Delayed 仅记录新数值，在下一次真正 ready 时使用。Yield 表示用过一次执行机会，重新排队时分配新的 ready sequence；持续被新任务挤压的旧 runnable entry 保留其等待年龄。

每个资源只提交一个活跃 task；FairnessKey 可由资源 owner 采用稳定的 collection/vchannel 分组。共享依赖增加不增加任务数量或重复调度份额。其他 legacy 任务缺省 key 使用其通用默认组，保留组内稳定次序。

入队、有效值调整、到期转移通常为 O(log N)，选择不扫描全部 View；各 heap/list 的准确常数成本在实现时测量。并发配置调整沿用 worker resize，缩容只在步骤边界退出，不丢失 Queued/Waiting/Delayed 项。

### 12.5 完成、取消和锁规则

- worker 不持 scheduler 锁执行 RunStep 或 OnFinished。
- Cancel 非阻塞，只取消 context、撤销 runnable/delayed/wait 注册并安排终态处理；不能在调用 Cancel 的资源锁内同步回调 OnFinished。
- 终态 entry 进入内部完成队列，worker 在锁外执行一次 OnFinished；Close 也必须排空此队列，不能直接关闭 queued 任务的 handle 而漏掉结果通知。回调完成后再关闭 handle。
- running entry 被取消时不提前释放 task，等待本次 RunStep 返回后终止，不再接受其 Yield/Wait/Retry 结果。
- 等待注册不内联回调；Event.Signal 在自身锁外调用短小的 scheduler wake，scheduler 不反向调用 dependency 的业务方法。
- resource owner 不持资源锁调用 handle.Wait，避免完成回调需要同锁而相互等待。

任务句柄完成只证明 RunStep 与 OnFinished 不再运行。如果任务启动了受限异步 I/O，owner 还要取消并 join 这些操作、等待 operation refs 清零后才能释放底层资源；不能把 Wait 当作全部资源生命周期已结束。

若全局 scheduler 已关闭，后续复杂清理由 owner 现有关闭流程完成，不能依赖向已关闭执行池提交一个 release 任务。单 worker 同样遵守“不同步等待同池子任务”规则。

### 12.6 兼容接入与资源任务实现

**Legacy 适配。** 内部 adapter 将 Execute 的 nil 映射 Done、ErrDelay/DelayUntil 映射 Retry，其余错误映射 Failed；不改变现有重试期限和默认 backoff。旧任务在 Execute 返回前仍不可协作抢占，此限制应保留在验证报告中。

**SN scoped wrapper。** `scopedTaskScheduler` 同时实现 CooperativeScheduler，并持有同一全局执行器。新增 trackedCooperativeTask 转发 RunStep，仅在 OnFinished 中完成 scoped entry；转发 CooperativeHandle.SetPriority。不能像旧 trackedTask.Execute 那样把 Yield/Wait 当作非 ErrDelay 的结束。owner 关闭取消其 handles，等待完成，不关闭全局调度器。

**资源优先级聚合。** QN/SN resource manager 保存不可变 View priority 和依赖集合，以及单个 resource task handle。依赖加入/满足/退出后在资源锁内计算 max 并 SetPriority；无准备依赖时取消任务。普通独占任务只在提交时设置 WithPriority。没有 View 级 UpdatePriority API。

**QN Segment task。** 把当前 reconcile 拆为可继续的“选择 eligible binding -> 取 metadata/前置依赖 -> 一次物理准备阶段 -> 发布兼容 read/安排 catch-up”状态。短时选取/发布在资源锁内，I/O/native 步骤在锁外；本阶段无法推进则 Await，完成一段仍有工作则 Yield。已经满足的兼容绑定先发布，不重新加载。绑定选择同样使用固定优先级和公平次序。

**QN catch-up。** registration 继续保持唯一执行与按批次推进的语义，其现有 drain pool 可复用相同的优先级队列和有效值来源，而不改成第二份无预算的全局任务。页完成后 Yield/重新入队，等待 coverage 则停放并由 buffer arrival 唤醒。若迁入统一执行器，必须保留原有 catch-up 并发预算，不能只移队列而放大并行度。

**SN build/ready task。** task 保存 stage、当前构建快照、未发布 runtime、replay cursor 和已有模块结果。阶段之间 Yield，shared future/barrier 未满足时 Await；真正 build 失败遵循原有 fresh-build 重试，不在 Yield 时丢弃 runtime。Ready 仍要求全部既有条件满足。

**I/O 与 permit。** 任务先尝试有预算的 admission；拿不到 permit 则 Await 容量 Event，拿到后发起一次受限操作，等待完成 Event，之后推进阶段。异步操作本身持有 permit 与 operation ref，完成/取消路径负责恰好释放一次。不能只将阻塞调用移进无限 goroutine。对暂不可拆的 native 调用，承认本次 RunStep 粗粒度，在返回后再让出。

**SN live-event。** 保留单 runtime 串行应用，将一个 drain 改为有界 batch；更多工作回到队列参与选择。准备依赖可以提升该 runtime 的有效优先级，但已经服务的 runtime 继续享有公平进展；事件内部顺序不变。

### 12.7 必需的接口级验证

除第 10 节场景外，新增：提交拒绝不转移 ownership；执行前取消仍恰好一次 OnFinished；Wait 在 Yield/Wait/Retry 后不提前返回；通知发生在依赖检查前、检查后、等待安装中和取消后均不丢失或重复执行；SetPriority 在四种非终态及终态中的行为；优先级变化保留排队年龄；单 worker 下依赖与完成通知推进；scoped wrapper 不提前移除 task；Close 排空完成队列；旧 Execute/ErrDelay 调用方兼容。

实现提交时需对具体步骤的继续状态、取消与资源 drain 分别测量验证；以上接口不表示可以只增加一个优先级 heap 就完成整个功能。
