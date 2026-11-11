# ViewFeedback：Shard 近期访问反馈

关联需求：[milvus-io/milvus#55284](https://github.com/milvus-io/milvus/issues/55284)。

ViewFeedback 按 ShardID 聚合，通过已有 SyncQueryView stream 尽力上报。SN 只维护当前时间窗口，
Coord 保留最近 **2 分钟**的有效窗口并自主过期清理。统计允许丢失，不实现可靠投递、
历史累计总量、加载优先级或调度评分。

## 1. 统计口径

`qviews.ShardID = (ReplicaID, VChannel)`，不携带 QueryView 版本。

`GetQueryPlan` 取得实际 View lease、完成 schema/collection 校验后，进入 MVCC 等待和
计划构建前调用 `SNQueryViewHandler.RecordShardAccess`。未知 Replica 请求使用 lease
解析出的真实 ReplicaID。Search、Retrieve、Statistics 每次计划构建各记一次；后续失败
不回滚，重试重新计数，Phase 2 不重复计数。

每个样本包含窗口内访问次数和最后访问的 SN Unix 纳秒时间。每个窗口携带公共起止时间，
次数不是累计值，也不是成功查询数或 NQ。大小触发会产生不足 5 秒的窗口；窗口次数不能
直接当作 QPS。

## 2. SN：只保存尚未取走的窗口

顶层 `SNQueryViewHandler` 按 PChannel/WAL 实例存在，持有私有 `viewFeedback` 子结构：

```text
viewFeedback
  mu
  window start
  ShardID -> (window request count, last access)
  capacity-one size notification
```

Observe 在统计自己的短锁内更新当前窗口。它不获取 handler View map 锁、snShardView
锁或状态机锁，也不编码 protobuf 或发送网络请求。

`TakeViewFeedback` 在同一把锁下交换出当前 map，并以取走时刻开始空窗口。第一次访问
初始化未开启的窗口。交换是 O(1)，不遍历 Shard；交出的 map 完全属于发送方，之后的
Observe 只写新窗口。因此发送方可以在锁外遍历、编码、释放 map。

不再维护 groundtruth、累计 counter、上次发送缓存或历史 Shard 表。View 换版、Down、
Dropped 不影响尚未取走的窗口；取走后数据被消费，无论发送是否成功都不恢复。
SN 重启或 WAL owner 迁移可以丢失当前窗口，不重建统计。

## 3. 触发和传输

初始化时由 qviewsRuntime 调用本地 `GetClusterChannels(OptIncludeUnavailableInReplication())`，
把全量 PChannel 名单传给 ReliableSyncer，预建常驻 resumable stream；无需等待 View 或
assignment owner 就绪。之后新 PChannel 在第一次 `SyncViews` 时补建，建好后持续保活。
新增但尚无 View 的 PChannel 暂不建流，因为尚无可服务的 QueryView 产生访问统计。
SN owner 解析和迁移重连继续由已有 HandlerClient 负责；Syncer 不增加持续发现监听。

沿用现有发送循环：

- 建流时取走当前尚未发送的窗口；没有历史重放。
- 此后取窗后满 **5 秒**，或当前 map 达到 **1024 个不同 Shard 条目**，触发下一次取窗。
- 阈值衡量聚合条目数，不是请求数。热点 Shard 的高频 Observe 不会独自产生高频上报。
- 大小触发的相邻取窗至少间隔 **1 秒**（同一 stream 内）；过早通知安排到该时刻，等待期间继续处理状态/关闭报告。
- 大小通知容量为一，等待时暂不接收重复通知；不会反复延后截止时间，也不需要新的 SN goroutine 或订阅者列表。
- 空窗口不发送；取走窗口时清除旧大小通知，每次实际取窗重置 5 秒发送计时。
- 1024 是触发阈值，不是硬内存上限；发送循环受阻时，新窗口仍可继续聚合。

取走的窗口按每批最多 256 个 Shard 编码，批次之间优先处理 pending 状态/关闭报告。
不要求整个窗口分批全部到达，Coord 可独立接收每批。发送失败、取消或关闭后，剩余
统计直接丢弃；不回填、不 ACK、不去重重放，也不添加 sequence、epoch 或补偿队列。
重连只恢复 stream 和原有 View pending，不能重发已经取走的 ViewFeedback。

窗口取走不再占用发送循环做全表扫描、变化比对。编码、网络发送仍有成本，已经开始
的 gRPC Send 无法被后来的状态报告抢占；本方案不承诺慢网络下严格隔离状态同步延迟。

### 协议

复用现有 `viewpb.ShardID` 和 `SyncResponse.view_feedback`：

```proto
message ViewFeedback {
    ShardID shard_id = 1;
    uint64 request_count = 2;       // 当前窗口内次数
    int64 last_access_unix_nanos = 3;
}
message ViewFeedbackReport {
    repeated ViewFeedback shards = 1;
    int64 window_start_unix_nanos = 2;
    int64 window_end_unix_nanos = 3;
}
```

不改变 views/close 字段或 RPC 签名。缺少窗口时间的旧累计 ViewFeedback 消息直接忽略，不能
把累计值当增量相加；master 原有不带 ViewFeedback 的节点继续正常进行状态同步。

## 4. Coord：Replica 生命周期和窗口生命周期

ReliableSyncer 检查样本属于 stream 的 PChannel，直接调用 `ReplicaFeedbackStore.OnViewFeedback`。
Store 检查 Replica 当前存在及 VChannel 的 collection 归属，不经过 Registry 或状态机。

`LoadConfigStore.RegisterLoadConfigListener` 的初始回放和增量通知管理 Replica：保留
ReplicaID 则保留其有效窗口，RG/字段/索引变化不清空；撤销 Replica 或 collection load intent
立即删除统计，迟到报告不能复活。View 的创建、换版、删除不决定统计生命周期。

每个 Shard 保存近期收到的窗口样本及失效时间：

- 通常在 `window end + 2 分钟` 过期。延迟到达且已经过期的窗口直接丢弃。
- 断线或发送阻塞会延长实际窗口；超过 2 分钟的整窗直接丢弃，不将长时间积累的计数当作近期活动。
- 对 SN 时钟超前，过期时间最多是 `Coord 接收时间 + 2 分钟`，避免未来时间无限滞留。
- 不将重复投递当作正常恢复方式，不维护跨流去重协议。流中窗口只消费一次。
- 按整个窗口过期，不对跨越 2 分钟边界的窗口做请求级切分或比例折算；这是一份近似统计。

Store 启动一个独立 goroutine，每秒清除过期窗口和已经没有有效窗口的 Shard 条目。
即使没有新的上报、没有任何读取，物理清理仍会进行；空 Replica 容器随配置保留。
`GetViewFeedback` 在读时也过滤过期窗口，因此不需要等下一次清理 tick 才停止暴露过期值。
返回值为有效窗口次数之和、有效窗口中最后访问的最大值；没有有效样本返回 `found=false`。
未知不等同于严格零访问，网络丢失会造成低估。

配置差分、收样和过期清理使用 ViewFeedback 自己的锁；LoadConfig listener 不做 I/O、不反向
获取 LoadConfigStore 锁、不操作状态机。统计锁竞争仍可能间接延迟配置通知，不能将
模块解耦描述为完全没有共享 CPU、分配或锁等待成本。

## 5. 启停

Coord 恢复 LoadConfig 后创建 Store、回放 Replica，并启动清理任务，再把 Store 交给
ReliableSyncer。关闭时先停止 Syncer，再注销 listener、停止并等待清理 goroutine、
释放统计。初始化失败执行同样清理，Close 可重复调用。

Coord 重启不恢复旧窗口，也不要求 SN 补发已消费窗口；等待后续访问产生新窗口。
已建立的 SN/PChannel stream 在没有 View、没有 ViewFeedback 时继续保活。QN 不生产 ViewFeedback。

## 6. 验证

- Observe 与取窗并发：窗口互不影响，正常取走无重复或计数丢失；主动丢弃窗口不补发。
- 持有 View 锁时仍能 Observe/取窗；View map 清理不影响当前窗口。
- 5 秒和大小触发、空窗口、大小通知合并、热点不触发大小上报。
- 连续大小触发至少间隔 1 秒；等待期间状态/关闭及时处理，停止通知后已安排的发送仍会执行。
- 状态/关闭报告批次优先；失败、重连只发送新窗口；无 pending View 也能建流。
- Coord 累计近期窗口、按期淘汰、无上报/无读取自主清理、未来时间限制、晚到旧窗口丢弃。
- Replica 删除、RG 变化、runtime 恢复失败和重复关闭；race 与变更函数覆盖率。
- 真实 SDK 查询后停止访问超过 2 分钟，验证统计变为 unknown；再查询从新窗口计数。

实际构建、测试、benchmark、覆盖率和 E2E 结果记录于本工作区 docs/plans，不以文档
验收清单替代已执行证据。
