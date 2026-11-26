# WAL 多副本协议设计

四个 RPC 属于 StreamingNodeWALReplicaService。Sync 传递期望拓扑和实际状态，Watch 同步 checkpoint 和 QueryView，Prepare 与 Seal 协调写权交接。[生产 proto](../../../../pkg/proto/streaming.proto) · [副本状态机与 Coord 收敛](wal-replica-state-machine.md) · [多副本工作流](wal-replicas.md)

| 使用点 | 消息和责任 |
| --- | --- |
| 目标分配与恢复 | Coord 在 SyncWALReplicas 上发送完整拓扑，SN 回报本地实例及就绪情况；服务路由来自各 SN session。 |
| RO 冷启动 | 从 etcd 及共享 WAL 建立本地基线；根据历史查询需求读取对象。 |
| 稳态、重连和换源 | RO 打开 WatchWALReplicaState，首包全量对齐 checkpoint 和 QueryView 信号；后续只发送变化。RO 在同一条流回报 View 的本地状态。 |
| A/7 → B/8 | B 向 A 依次请求 Prepare 和 Seal，中间从冻结的持久状态恢复 writer。两个 RPC 各有独立的空响应。 |
| 异常接管 | Coord 确认原目标 writer 的 session 失效后重新选主并广播新拓扑；新接任者按授权从持久存储和 WAL 恢复。 |

## 1. Coord 保存期望，SN 回报实况

**Coord catalog。** 每个 `streamingcoord-meta/pchannel/<name>` 只存一份 WALReplicaTopology。非空成员集合有唯一的目标 writer，其余为 RO；空集合表示关闭。成员 ID 绑定节点，角色切换保留 ID。拓扑 revision 来自全局单调分配器，任期也须防止复用；单纯改变 RO 不换 writer term。空目标保留修订和已分配的任期上界，writer ID 为零。SN 取全量 topologies（含空目标）的最大 revision 作为全集版本，空全集取 0。查询成员确认和实况回报均不推进它。副本地址及实例进度由运行时提供。多副本模式下，Coord 持久化的目标只包含该拓扑。原均衡冷却时间可在恢复首轮 Sync 后重新计时，仅影响可选重平衡。

**全量 Sync。** Coord 向所有存活 SN 下发完整目标。SN 接受目标后回报 observed_revision 及完整本地清单。每个实例在操作执行中报 Preparing，结束后报实际的 RO、RW(term) 或 Closed。目标中仍包含已关闭 ID 时持续重报 Closed；拓扑移除该 ID 且本机已排空后才可从清单省略。Coord 用当前控制流的同版本报告驱动下一次规划，客户端从 SN session 获取实际服务端点。[完整判断规则](wal-replica-state-machine.md)

**为什么还要 confirmed_query_memberships。** 例如从 A/B/C 收缩到 A/C，B 已摘掉路由，仍可能正在排空一个 View。Coord 收齐覆盖当前目标的完整清单或取得 B 的 session 退出证明后，才确认 A/C 已包括所有仍可能服务者。此字段按包内 PChannel 的修订解释，可在同一全集修订下稍后补发。它不要求所有成员的所有 View 已 Ready，也不写入拓扑。控制连接重建时重新确认；RW 只在当前授权和当前成员确认有效时提交新 View 结果。

## 2. SN session 发布 WAL 路由

SessionRaw.WALReplicas 保存本节点可服务实例的 WALAdvertisement 列表，地址、NodeID 和 lease 取自外层注册。每个 PChannel 在一个节点上最多一个可服务条目。[SessionRaw 定义](../../../../internal/util/sessionutil/session_util.go)

**WALReplicas。** 初次注册时为空。实例开放本地准入后发布 RO 或 RW，关闭先停止准入再撤销。writer_term 存在且为正数表示 RW，可接受该任期的 Append 和 Strong 请求。RO 省略 writer_term。RO 订阅源的任期由 OpenWALReplicaWatch 标识，换源时 RO 公告可保持原值。RO 公告用于拓扑收敛与交接，所有 WAL 查询均由 RW 提供。session 只在本地可服务状态变化时重写，全节点用一个串行发布入口与 GoingStop 共用，PUT 须校验注册身份、读取版本并保留原 lease。结果未知时重读后幂等重试，不得重新建立已失效的注册。

**CChannel。** Coord 在 SyncWALReplicasRequest 中传递已有的固定映射。只有映射指向的 PChannel 的可服务 RW 在自己的 WALAdvertisement 上设置 is_cchannel=true。RO 和其他 PChannel 的 RW 使用 false。StreamingClient 从该条目同时得到控制频道和可写端点。交接中旧条目撤下、新条目尚未发布时，客户端等待新的可写端点。

**客户端。** 对 SN 注册前缀做一致的全量读取，再从下一 revision watch。PUT 替换该 SN 全表，DELETE、原 lease 失效或 Stopping 撤下对应路由。断线或 compaction 时重新对账。找不到唯一 RW 时等待刷新；旧客户端缓存打到已 Seal 的 A，由服务端拒绝。普通 session watcher 需区分新注册和同一次注册的内容更新。[现有 session resolver](../../../../internal/util/streamingutil/service/discoverer/session_discoverer.go)

接入多副本后，AssignmentDiscover 只保留复制配置、StreamingVersion、控制版本及原有错误反馈用途。其响应字段 2 (assignments) 和 3 (cchannel) 退出客户端发现路径。QueryView 目录的 SubscribeReplicas 必须携带当前 writer term，订阅 RW 上已 Up 的逻辑查询副本。对已替换实例的旧连接需按 ID 拒绝。

Strong 由当前 RW 提供 MVCC，客户端携带 writer term。Bounded、Session 和 Eventually 携带 WALReplicaID，由当前 RW 按已完成写入取点。Customized 携带有效时间戳 B 时走 barrier_mvcc，在 RW 上固定读 B。RW Seal 时结束目录订阅，客户端在新主上重新订阅并取得首份快照。TODO：按所选物理副本订阅目录后接入 RO 查询。客户端把取点成功的 NodeID 和 WALReplicaID 与该分片的计划一起保留。Search、Retrieve、两阶段搜索和按主键回查中所有需要访问 WAL 的操作都发往这个实例，sealed 部分按计划发往相应 QN。SN 执行时校验实例身份、View 版本和 MVCC。来源失效时，客户端丢弃该分片本次的部分结果，重新取点和执行。已经依赖归并结果的回查失败则由外层重试整个操作。副本选择保存在调用方内存中，QueryView 和 WAL 同步协议无需增加字段。

旧升级用途的 access_mode、Assign/Remove 分配流程及其专用类型退出该设计。CollectStatus 的节点指标用途可继续沿用。

## 3. 主备订阅只有两类信号

WatchWALReplicaState 是按 PChannel 建立的双向流。RO 首条请求为 open，指定由 session 发现的 writer 实例和任期，以及本机 subscriber 实例。RW 校验身份和角色；从该端点不再有写权起，旧流的回调和回报失效。每次连接的第一条响应是 snapshot，之后是 changes，两个分支共用以下字段。

| WALReplicaState 字段 | 首包 | 增量及 RO 的动作 |
| --- | --- | --- |
| checkpoint | 当前已持久化的 WALCheckpoint，仅投影 time_tick 和 message_id；尚未发布时省略 | 持久提交成功才通知，首次值和同源后续值都可以合并到最新位置。仅用于本地 Buffer 的截断。省略表示本批无变化。 |
| query_views | 当前 PChannel 的完整有效信号集，每个逻辑 View 一项，包括仍待排空的目标 | 发送发生变化的 View。WALQueryViewSignal 携带逻辑键 (ShardID, QueryViewVersion) 和目标状态。Preparing、已持久的 Up、Down、Dropped 均可表达。省略表示该 View 保持原状态。 |

RW 从已恢复的权威 QueryView 目标及 SN 持久 Up 建立初始有效集合。Coord 的 QueryView catalog 支持恢复全量，现有 syncer 重连补发**仍待确认的命令**；仅收到一批补发命令不足以判定全集为空。RW 初始化期间先登记/缓存新命令，与权威基线按 View 对齐后才提供首包。Down/取消优先于残留的持久 Up；尚未持久发布的 Up 目标向未准备的成员发 Preparing。RW 在本地同一临界区固定信号集、最新 checkpoint 并注册订阅。其后的变化进入有界待发队列，同一个 View 可合并为最新动作；要撤销 Preparing 时显式发送 Down 或 Dropped。慢订阅断开后以新首包对齐。RO 只把**完整首包**中的缺项按现有退休、租约及引用排空规则处理。[Coord 恢复](../../../../internal/views/coord/coordview/shard_view_registry.go) · [Syncer 补发语义](../qviews/syncer.md)

RO 从 SN catalog 恢复的 Up 走 UpRecovering；Preparing、Down 等在处理中的目标以 Coord 持久意图及 RW 同步为准。没有 RW 时仍可从 etcd 启动，依据可确认的目标、已恢复的资源及原有读租约服务；无法确认的 View 留在恢复状态。收到信号后按键取目标版本的持久定义和控制信息。发现更新目标已经覆盖本次任务时，放弃陈旧结果并对齐当前信号。Dropped 的持久目标可能已删除，仍可凭逻辑键在本地完成排空。Coord 原有发送顺序是先持久化再同步。[SN 的 Up 恢复记录](../../../../internal/metastore/kv/streamingnode/kv_catalog.go) · [Coord 发布顺序](../../../../internal/views/coord/coordview/dirty_view_flush_scheduler.go)

冷启动时可能读到“Coord 已要求 Down，但 RW 仍在等其他副本排空”的持久 Up。新进程没有可继承的本地服务租约；只读到 SN 的 Up 记录不能重新开放查询。还要对齐当前授权成员和有效目标，Down 或无法确认目标时保留未就绪状态。这条限制只影响对外准入，scanner 和资源加载均可继续。[现有恢复租约语义](../qviews/query_view_lease.md)

## 4. Up 和 Down 的多副本确认

1. RW 收到 Preparing，将其分发到已确认参与集合。每个副本在本地固定 Buffer 后缀和实时订阅，加载具体 View 的持久基线，核对 Schema、目标 DataVersion 的 growing/sealed 归属并赶上所需进度，才报告 Ready。RW 也执行本地准备，聚合 Ready 后回报 Coord，避免 Coord 与 RO 相互等待 Up。
2. 收到 Coord 的 Up 目标且本轮参与者均已 Ready 后，RW 在当前写授权下持久化 Up。确认成功后才发 Up 给 RO 并向 Coord 报告。新加入或重启的 RO 收到已持久 Up，需要经过本地 UpRecovering 和同样的资源核对，不能直接开放查询。
3. QueryCoord 的 Syncer 向当前 RW 发送 Down，RW 将同一目标同步给参与者。RW、RO 共用本地 QueryView 状态机。Up 等待服务租约和在途引用排空，Preparing、Ready、UpRecovering、Unrecoverable 收到 Down 可直接进入 Down，保留资源直到 Dropped。RO 只向 RW 回报本地 Down，已释放的版本可报 Dropped。RW 确认自身和当前成员均排空（退出成员由 session 确认安全退出）后，才删除 SN 的持久 Up 记录并向 QueryCoord 确认。RW 宕机后，Syncer 在新 RW 上补发尚待确认的目标。
4. RO 用 WALQueryViewReport 回报逻辑 View 键与本地状态。RW 从当前 Watch 取得 WALReplicaID 和 writer term，按副本及 View 分别记录状态。只有符合当前状态机转换的报告才能推进确认。同一副本重复 Ready 只计一次；目标进入 Down 后，迟到的 Ready 无法满足排空条件。RO 的异步完成回调须仍属于当前资源引用，沿用现有本地准备检查。[现有回报匹配](../../../../internal/views/coord/coordview/syncer/pending_sync_query_views.go) · [资源准备检查](../../../../internal/streamingnode/server/wal/vchannel/queryresource/manager.go)

对于同一 `(ShardID, QueryViewVersion)`，RO 一次加载失败后保持 Preparing，按退避策略重试当前版本。成功时通过当前 Watch 向 RW 报 Ready，无需 QueryCoord 重发命令。RW 即使已经在本地 Ready，仍须等当前参与成员全部就绪才能向 QueryCoord 报 Ready。只有 RW 自己在准备期间进入 Unrecoverable，它才通过 ViewSync 向 QueryCoord 报告该版本准备失败。已有的 Up 版本继续按原租约服务。

角色交接保留正在准备的资源。RO 升为 RW 后，后续加载失败按 RW 的规则分类。旧 RW 降为 RO 后，如果新主仍要求准备一个在旧任期失败的版本，本地保留该版本的资源引用并重新发起准备，避免更高 DataVersion 抢先推进回收水位。旧准备任务的迟到回调无法推进新加载。健康版本及其租约继续使用。

如果 RO 缺少该版本必需的历史基线，同一输入下重试仍无法达到 Ready。只要该 RO 仍是参与成员且该版本仍是目标，首次发布 Up 就要等待。基线恢复、目标版本更新或参与成员被替换后可继续推进。Down、Dropped 到达后以退休目标为准，迟到的加载完成不能重新激活该版本。已持久化 Up 的版本在新加入的 RO 上独立恢复，其他已就绪副本继续服务。

参与集合从 A/B 扩到 A/B/C 时，保持原实例、原 Watch 且仍有效的 B 的 Ready 可以沿用，新增的 C 必须报告自己的状态。更换实例、断流重连或换主时丢弃被替换 Watch 的回报及缓存，并从新流收集当前状态。若同一 View 键在同一副本需要重新获取资源并使既有 Ready 失效，先替换该副本的 Watch 再参与确认，隔离原连接上已发出的 Ready。普通目标重推直接重报本地状态。缩容时按前述成员确认规则等待排空或安全退出。

RW 在串行处理回报时再次核对当前有效 Watch、WALReplicaID、View 键及目标状态；旧流的收包任务即使在换流前已经入队，也要在执行时丢弃。换流的生效点在 RW 安装新 Watch 时。RO 在本地关闭 socket 并不能同步撤销远端已收的 Ready；同键重新获取资源时应先停止本地服务准入，重新就绪前即使收到已持久的 Up，也只能走 UpRecovering。

RW 在当前 PChannel 上串行处理参与集合、收票和提交。通过确认后，CommitReplicaQueryView 读取持久 checkpoint 并核验 writer term。保存 Up 或删除旧 Up 的同一次 etcd 事务比较目标拓扑和读到的 checkpoint，任一发生变化就重新对齐和确认。RO 执行本地资源与状态转换。[提交实现](../../../../internal/metastore/kv/streamingnode/kv_catalog.go) · [副本确认](../../../../internal/streamingnode/server/wal/adaptor/replica_views.go)

等待全部参与者具有可用性代价：任一仍属有效成员的慢 RO 可以延迟新 View 上线；普通 Down 还会等待现有租约。重试超时不构成排空证明。已可读且仍满足 MVCC 的 View 按本地状态继续服务。[现有租约语义](../qviews/query_view_lease.md)

如果任务仍在准备，checkpoint 可以继续推进。具体的引用保存、按组件去重及 Ready 边界见[截断与加载条件](wal-replica-commit.md)。

## 5. 协作交接的时序

首次恢复期间，目标 writer 的注册仍有效、操作还在进行且尚无首次 RW 公告时，Coord 暂缓改派。若操作结束，SN 对该目标报实际状态。RO 表示保留读侧，再次授予此 ID 写权时使用更高 term；Closed 表示该 ID 已终结，重新部署分配新 ID，重新授予写权也提高 term。Coord 优先处理当前版本的确定结果。目标注册退出则按故障接管处理，Coord 重启从持久期望和全量清单恢复。[Coord 的响应与异常时序](wal-replica-state-machine.md)

| 时刻 | A/7（旧 RW） | B（原 RO，目标 RW/8） |
| --- | --- | --- |
| 拓扑变更 | 仍按已公告角色服务 | 收到目标授权，确认已观察到 A 最近的持久写入后，调用 PrepareWALHandoff(P, 8)。单次 checkpoint 更新不能证明已经追近 WAL 尾部。 |
| Prepare 成功 | 暂停新的外部持久化和 View 提交，排尽在途对象、DataCoord 及 etcd 发布，随后返回 PrepareWALHandoffResponse；继续 Append 和 Strong | 保留当前读侧，隔离旧主过期回调，从冻结后的 etcd/S3 读取 writer 基线。单独对 writer 重放所需尾部并持续跟进原 scanner。 |
| Seal 成功 | 关闭 Append 和 Strong 准入、排尽写，确认 session 已改为 RO 或撤下，再返回 SealWALHandoffResponse | 预恢复完成后才请求 SealWALHandoff(P, 8)。核验最新拓扑并 CAS 领取 WALCheckpoint.term，隔离旧后端 writer。 |
| B 激活 | 继续按本地能力读，发现 B/8 后订阅其 checkpoint 和 QueryView 信号 | 写入本任 RecoveryBarrier，使用原 scanner 观察并完成 writer 恢复；随后开放写入和持久化，发布 B/8。 |

两次 RPC 按调用方 SN 注册身份、本地阶段、PChannel 和目标任期校验，同任期成功效果可重试。已经发起 Seal 后，A 负责把旧任排空并完成公告更新，B 重试未决的 Seal 或核实其效果。即使旧 RW 公告尚未撤下，也不能要求 A 重新写入近期 TimeTick 才允许这项收尾。A 仍存活而应答丢失时 B 向同一源重试；临时无 RW 路由不足以判定 A 已排空。A 已失去原 session 时改走故障接管。B 未获得写权就只继续读。追近依据在接入时验证。新任期的独占写入依赖底层 WAL fence；各后端的 fence 实现和验证在本 PR 范围之外。Checkpoint 不承担交接切点或选主凭证的职责。

### AlterWAL 与升主并发

RO 从持久 checkpoint 的物理位置打开逻辑 scanner。读到旧后端的 AlterWAL 记录时，scanner 从记录携带的目标起点继续消费，并在本地保留最近一次切换的 TimeTick 和目标 MessageID。即使 checkpoint 已推进并释放这条消息的 Buffer 引用，这个起点仍可用于选择候选 writer。副本间的 Watch 无需增加后端切换消息。

升主时固定本地 Buffer 的恢复游标，在候选后端写入本任 RecoveryBarrier。从同一逻辑 scanner 观察到匹配的 TimeTick、后端及 MessageID 后，writer 才可使用该屏障结束有界恢复。若先观察到更新的 AlterWAL，则跟随新起点重新写屏障。这样，旧主在写下切换记录后、推进 etcd checkpoint 前宕机时，接任者仍可从旧 checkpoint 恢复到新后端。开放写入和发布 session 公告继续遵守本任恢复流程。[AlterWAL](alter-wal.md)

### 接任者在交接中退出

A/7 正在向 B/8 交接时，若 B 的 SN session 已退出，Coord 重新选定 writer，例如 C/9，持久化目标拓扑后向所有存活 SN 广播 Sync。Coord 若同时重启，从持久拓扑和当前 session 重新执行相同的分配流程。目标节点仍存活、仅 Sync 或应答丢失时，Coord 补发同一拓扑，SN 幂等处理。

若 A/7 仍可接受写入，C 追近后按新授权向 A 发起 Prepare 和 Seal，已冻结持久化的 A 可复用排空结果。若 A 已开始 Seal 且旧公告尚可见，A 继续排尽并更新公告，C 在等待追近时需重新发现写源，随后按最新实况接管。若 A 已 Seal 并公告为 RO，C 按 C/9 的新授权和常规故障恢复流程接管。A 的 session 可以继续存活，C 无需取得 B/8 的 Seal 应答。旧任期的迟到请求按最新授权丢弃。

## 6. 实现验收

| 场景 | 可观察结果 |
| --- | --- |
| RO 冷启动时 RW 不可达 | 从 etcd 取得恢复入口，独立启动只读 scanner；无写入、无持久化任务。 |
| 捕获加载引用时并发截断，加载失败后重试 | 成功时无缺口、重复事件按组件过滤；失败、取消和替换后引用均可释放。 |
| 指定 View 需要 checkpoint 之前的 Delete | 按目标版本从对象存储完成有界重放，查不到所需历史时不报 Ready。 |
| DataView 已发布，SN 元数据尚未写回 | 按权威首次 sealed 绑定准备；旧 View 和新 View 各自不漏行、不重复。 |
| 对同一 View 重报 Ready、目标进入 Down 后又收到 Ready、旧 Watch 被替换 | 重复报告只计一个副本；Ready 不能确认 Down；原 Watch 的报告无效。 |
| RO 已 Ready 但跳过 Up，直接收到 Down | RO 回报本地 Down 并保留资源。RW 收齐确认后删除 SN 的持久 Up，Dropped 到达后各副本释放资源。 |
| 同一 Shard 的 V1 已 Up，RO 准备 V2 时加载失败，RW 先到 Ready | V1 继续服务。RO 重试同一 V2 成功后报 Ready，RW 收齐当前参与者的 Ready 后推进 V2。 |
| RO 仍在准备 V2，RW 本地准备 V2 进入 Unrecoverable | RW 立即向 QueryCoord 回报 V2 的失败，RO 随后按退休目标清理 V2。 |
| 新增成员、原成员仍在排空、Up 写成功后 RW 宕机 | 新成员独立确认；仍存活的排空者不可跳过；新主从持久 Up 补报。 |
| 交接期间执行普通查询与写入 | 已就绪的普通读保留原 runtime；A Seal 后拒绝旧 term，B 屏障就绪后才对外写；既有查询无重复 replay。 |
| Prepare 后 B 退出；另一次在 Seal 后 B 退出且 Coord 重启 | Coord 按 session 重新分配更高任期并广播；所有 SN 幂等收敛到最新拓扑，旧任期的请求不再生效。 |
| 旧主的 QueryView Up/Down 元数据提交迟到 | catalog 事务的拓扑和所有权校验阻止旧提交覆盖新主的 View。 |
