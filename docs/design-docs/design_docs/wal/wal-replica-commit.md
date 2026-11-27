# WAL 副本的 checkpoint、Buffer 与查询恢复

## 哪个位置可以释放

RW 复用 RecoveryStorage 的发布顺序：冻结连续完成前缀，保存各组件元数据，最后持久化全局 checkpoint。分批写 etcd 时也保持此顺序。checkpoint 只证明其所需的效果已可恢复。组件可以单独领先；长期没有新输入的组件也无需把自己的 TimeTick 刷到全局位置。Summary 的上界来自 LastAcked，恢复还可能探测已上传的连续 chunk 尾部。[发布实现](../../../../internal/streamingnode/server/wal/recovery/recovery_background_task.go) · [Checkpoint 约定](checkpoint-persistence.md) · [Summary 恢复](../../../../internal/streamingnode/server/wal/walsummary/recover.go)

RO 接收当前 writer 公布的已持久化 checkpoint，只在本地 Buffer 释放它对连续已观察前缀的引用。逻辑截断上界取已发布 TimeTick 与本地完整观察位置的较小值；原始事务组装继续持有所需消息。活跃读者持有自己的引用。重复或较旧的通知幂等处理；checkpoint 领先时记住目标，等 scanner 自己追到再释放。此操作不调用共享 WAL 的 Truncate，也不推进查询 MVCC。物理重读使用与 TimeTick 配对的 LastConfirmedMessageID，允许重读并过滤已观察前缀。发生 AlterWAL 时按已有切换状态重取基线，不跨后端比较物理 ID。[现有 scanner 重读](../../../../internal/streamingnode/server/wal/adaptor/scanner_switchable.go) · [消息引用](message_ack.md)

## 加载与截断并发

按下面的本地顺序准备 View 或 writer 状态，无需 RW 为每个 RO 持有一个截断点。

1. 在本地观察与截断共用的临界区记录 Buffer 下界 L 和完整观察位置 H，取得 (L,H] 的消息引用，同时注册接收 H 以后输入的消费者。退出临界区后做存储 I/O。这样既保护了历史后缀，也接住了加载期间的新消息。
2. 从 etcd 的一致读视图取得已发布 checkpoint 和适用的组件基线，复用现有发布、Schema 可见性和组件重放规则。基线须能衔接 L；较旧读视图、失效版本或已经缺失的物理区间应重新取基线。元数据可能领先 H，查询可见进度仍受本地实际消费位置限制。
3. 加载指定 View 版本的持久数据，只补每个组件尚未覆盖的消息，再按顺序接上注册后的输入。同一批输入按各组件游标去重。成功、取消、失败重试或本地实例关闭时都释放本轮引用及订阅。重试用新捕获的基线。慢消费者超出本地预算时退出本次加载并重试，不静默丢弃消息。
4. 指定 View 的 Transform 起点可能早于 L。用该版 View 及实际 Segment base 对应的游标，从仍受保留保护的 Summary 对象补齐历史 Delete，再接 Buffer 的连续后缀。对象或版本缺失时保持未就绪。接入可复用现有有界 Delete replay 和 QueryRuntime 捕获、排队、drain 机制；RO 需要独立的只读加载入口。[查询快照](../../../../internal/streamingnode/server/wal/vchannel/query_resource_module.go) · [Transform 起点](../qviews/transform_start_after_timetick.md)

例如 L=100，RO 已观察到 140。准备任务先取得 (100,140] 并注册实时输入，随后 checkpoint 推到 120。Buffer 可释放自己的前缀，任务仍持有其所需的引用；若读到的 Segment 基线已经覆盖到 125，任务只对该 Segment 应用 125 之后的输入。

## Shard 发布下界与 Preparing 的顺序

DataCoord 在首次发布 Segment 的事务中更新该 shard 的 `required_streaming_version`。QueryCoord 把选定 DataView 的值带入 QueryView。同一 collection 的其他 shard 可以独立前进。

RW 接到 Preparing 时开始本地资源准备。在发布 Preparing 信号前，它在该 shard 的恢复模块内截取已经开始 final commit 的 Segment。能够产生目标版本的提交都早于这次截取；更晚开始的提交属于未来的 DataView。RW 等待截取范围中仍可能影响目标版本的 assignment 完成 catalog 持久化。DataCoord 的成功响应和 SN catalog 的持久化回调是两个独立的时点。版本 10 的回调先于版本 8 完成时，RW 仍须等 8。完整确认的下界仅在本任 writer 内存中复用。

收到 Preparing 的 RO 此时可以直接从 SN catalog 读取 assignment。两种角色的 QueryRuntime 都在本地 WAL 覆盖相应位置之后应用首次 sealed 绑定，再用资源事件屏障确认 Growing 和 IDF 已处理整批更新。同一 View 引用的 Ready 已包含这次确认，收到 Up 后可直接复用。新加入的 RO 即使首帧收到 Up，也要先在本地完成准备。较旧的 View 按自己的 DataVersion 区分 Growing 和 sealed。已有 Up View 在此期间继续服务。

订阅者从 writer 已经公布的信号集合取得首帧。新 writer 在 RecoveryBarrier 后重新确认未提交的 Preparing，持久化 Up 的记录已经包含以前完成同一确认的事实。恢复首帧不能省略仍然待确认的 View，否则 RO 会将缺项解释为退休；在这些初始信号可发布前，订阅可重试。Down 或 Dropped 取代待确认的意图时取消对应等待。正常 Up/Down 的多副本确认仍由 writer 协调。[发布与恢复](../../../../internal/streamingnode/server/wal/adaptor/replica_views.go)

## 冷启动和升主

恢复以底层 WAL retention 覆盖已发布 checkpoint 之后的日志为前提。

RO 从 etcd 读已发布 checkpoint，从安全物理位置打开只读 scanner，并恢复具体 View 需要的持久基线。如果 RW 在读 checkpoint 后已经截掉起点，重新取可恢复的 checkpoint 和元数据，验证连续性后再服务。RO 只能读取对象；现有 writer 恢复入口会领取 checkpoint 所有权、恢复 Summary 并调度后台任务，不能直接用于 RO。[当前 writer 恢复入口](../../../../internal/streamingnode/server/wal/recovery/recovery_persisted.go)

协作升主时先等旧主 Prepare 成功，准接任者再按上述捕获顺序构建 writer 的完整内存状态，恢复控制元数据、Segment、VChannel 以及 S3 Summary 等，并按组件覆盖过滤尾部。此阶段保持只读，延迟迁移写、Summary PUT、DataCoord 操作和后台持久化。等旧主 Seal、写授权 CAS 及后端 fencing 完成后才执行需写权的动作。原 scanner 观察本任 RecoveryBarrier，writer 追到该屏障才开放写入。故障接管无法冻结旧主，须先获授权并隔离旧发布者，再从权威存储恢复。失去连续 Buffer 的实例须从安全位置重读，不能假定内存始终覆盖。[交接时序](wal-replica-protocol.md)
