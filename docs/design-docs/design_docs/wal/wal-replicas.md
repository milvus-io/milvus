# WAL 多副本工作流

同一 PChannel 的各副本独立读取共享 WAL。RW 负责 Append、Strong 查询和完整 RecoveryStorage 的持久化；RO 持有本地 WAL Buffer、scanner 及按需加载的 QueryRuntime。RW 仅把已发布的 WALCheckpoint 和 QueryView 信号发给 RO。两端共享恢复所需的 etcd、对象存储及 WAL。[副本状态机与 Coord 收敛](wal-replica-state-machine.md) · [协议与时序](wal-replica-protocol.md) · [截断与加载的正确性条件](wal-replica-commit.md)

| 流程 | 输入和完成条件 |
| --- | --- |
| 分配 | Coord 按 SN 资源组为每组放置一个副本，writer 优先放在 streaming.primaryResourceGroup。持久化 desired topology 后向所有存活 SN 广播，重启或未收敛时重发。SN 独立发布自己的可服务路由。 |
| 冷启动 RO | 从 etcd 读取已发布 checkpoint 和该 PChannel 已持久化的 Up View，从安全 MessageID 开 scanner，重建 Buffer；按 View 版本读取查询元数据和对象、补齐尾部。已持久 Up 走 UpRecovering，满足本地资源、追赶和租约条件后才服务。首次装入以持久状态为入口；有 RW 时再对齐当前 QueryView 目标和 checkpoint。 |
| 稳态 | RO 观察到的有序 WAL 进入 Buffer，同时分发给已注册的读者。RW 成功持久化的 checkpoint 推进 Buffer 的释放边界。准备新 View 时才读取它需要的 Schema、Segment 元数据、数据文件和历史 Transform。 |
| 冷启动 RW / 故障接管 | 持有目标 writer term 的实例先隔离旧发布者，从 etcd 恢复 writer 状态、从 S3 恢复 Summary，按组件边界补齐共享 WAL。完成后端 fencing 并观察本任 RecoveryBarrier 后开放写入并公告。普通 RO 无权自行接管。 |
| 协作交接 | 已追近的 B 请 A 暂停并排尽持久化。在 A 仍可 Append 时，B 构建 writer 的内存状态并追尾。最后 A 停写、撤下 RW 公告，B 领取新 term、fence 后端并观察本任屏障，随后开放写入并公告。 |

接任者在交接中退出时，Coord 根据 SN session 重新选主，持久化更高任期的目标拓扑并广播 Sync。各 SN 按最新目标收敛。底层旧 writer 的隔离由 WAL fence 提供，本 PR 直接使用这一前置条件。[接任者退出时序](wal-replica-protocol.md#接任者在交接中退出)

全频道显式关闭后若立即收到重新开放的意图，Coord 先确认所有存活 SN 已接受包含空拓扑的全量 Sync，且本频道在途实例均已排空，再分配新 ID 与更高任期。其他 PChannel 可继续收敛。[关闭与重新开放](wal-replica-state-machine.md)

## 重构外层 WALLifetime

现有 walLifetime 按 (term, available) 排序，一旦更换 term 就关闭整个 WAL 再 Open。多副本模式须改造外层 WALManager 的状态收敛与请求准入。一个本机 PChannel + WALReplicaID 持有同一个读侧实例。跨节点迁移创建新 ID；同一实例的角色切换由本地 WALLifetime 串行推进。拓扑修订去除陈旧目标；同一目标的 Sync 或交接重试复用已完成的动作。[当前生命周期](../../../../internal/streamingnode/server/walmanager/wal_lifetime.go) · [当前请求准入](../../../../internal/streamingnode/server/walmanager/manager_impl.go)

| 转换 | 实例持有的资源和动作 |
| --- | --- |
| 新分配的 ID → RO / RW | 创建 scanner、事务组装器、Buffer 和查询资源管理器，按上表相应的冷启动流程初始化。 |
| RO → RW | 保留读侧实例、QueryRuntime 和在途查询。在 Prepare 成功后从稳定基线**新建 writer 侧 RecoveryStorage**，对它单独重放尚未覆盖的 Buffer 尾部。写侧恢复重放只投递给 writer，既有查询只继续接收原 scanner 的一次实时分发。 |
| RW → RO | 在 Prepare 排尽持久化，Seal 时关闭 Append 和 Strong 准入并排尽在途写。仍属目标成员时将同一条 session 公告改为 RO；交还写权后释放专属 writer 状态。实例的 Buffer 在 RW 期间也持续维护，已有 scanner、QueryRuntime 和查询引用继续工作。 |
| RO / RW → Closed | 退出或被新 ID 替换时停止准入，撤下路由并履行旧主的交接或隔离义务。取消订阅、停止后台任务、排空引用，再关闭实例。目标仍含旧 ID 时重报 Closed，目标移除该 ID 且本地排空后移出清单。 |

所有查询均向当前 RW 取 MVCC，RO 持续追赶 WAL 和准备 QueryView。RO 查询路由留待后续接入。查询的 WAL 部分在提供 MVCC 的同一实例执行，按主键回查也沿用此实例。Append 和 Strong 另行检查写准入和 writer term；从 A Seal 到 B 完成本任屏障且被客户端发现，两者会有等待窗口。角色切换本身不触发已 Up View 的重建。[查询发现](../qviews/query/replica_discovery.md)

## QueryView 多副本确认

QueryView 的逻辑身份为 ShardID + QueryViewVersion。RW 从当前 Watch 取得报告者的 WALReplicaID，以 ShardID + QueryViewVersion 匹配本地状态；重复回报幂等。RW 协调当前参与集合的 Ready 和 Down；RO 使用同一查询状态机准备、服务、排空，只回报本地状态。RW 在收到 Coord 的 Up 目标且参与者均已 Ready 后，先持久发布 Up，再通知 RO 开放该版查询并上报 Coord。Down 时先分发排空意图，各参与者遵守已有租约及操作引用；收齐 Down 或安全退出证明后，RW 才删除持久 Up 并上报。Dropped 继续走原释放流程。重连或换主后从新流收集本地状态；成员变化时按新集合判断，新增成员单独确认。[QueryView 状态机](../qviews/query_view_state_machine.md) · [成员确认规则](wal-replica-protocol.md)

## 运行代价

稳态 RO 无需镜像每个 Segment 或维护可发布的 Summary。View 加载和升主时承担相应的元数据读取、对象读取和尾部重放。长时间停滞的 checkpoint 或加载消费者会保留更多本地消息引用。协作时大部分恢复能在旧主仍可写时完成；故障接管时这段工作计入写服务的恢复时间。

## 需要在实现中闭合的边界

| 场景 | 当前依据与待验证条件 |
| --- | --- |
| RO 加载早于 Buffer 下界的历史 | 进程内消息引用只保护内存。历史 Delete 或 Segment 版本若已从对象存储移除，重新读取同一份元数据也无法恢复。沿用权威版本及现有保留范围，缺失时保持未就绪并上报；跨节点 Summary GC 继续作为独立议题。[Summary 保留边界](summary.md) |
| 长时间无 checkpoint 通知或没有新数据 | 可在断流、重连或 Buffer 压力下重读已持久 checkpoint 以区别漏通知和发布停滞。真正停滞时仍要有内存预算和背压。B 在启动交接前也需要获得近期持久写入的追近证据；空闲 WAL 上不能无限等待“下一条消息”。具体后端的尾部观测方式和时限仍需验证。 |
