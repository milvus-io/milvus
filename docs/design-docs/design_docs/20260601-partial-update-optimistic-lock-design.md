# StreamingNode 上基于乐观锁的 Partial Update 设计

| | |
|---|---|
| **状态** | 草案（实现已对齐） |
| **作者** | Wei Liu |
| **创建时间** | 2026-06-01 |
| **Issue** | [#49980](https://github.com/milvus-io/milvus/issues/49980) - Concurrent Partial Updates on the same PK |
| **Feature PR** | [#51845](https://github.com/milvus-io/milvus/pull/51845) |

## 摘要

Partial update 保持现有 read-merge-write 模型：Proxy 查询当前完整行并执行 merge，
StreamingNode 在 WAL `CommitTxn` 写入前执行 optimistic CAS admission。

每个 attempt 严格执行：

```text
resolve all PChannel terms
  -> allocate attempt-scoped readTS
  -> query at readTS
  -> commit(term, readTS)
```

StreamingNode 为当前 PChannel term 维护近期 `PK -> lastWriteTS` 索引。普通 Insert
通过 ShardManager 中的 collection schema 提取 PK；Delete 和 CAS Insert 直接从消息
提取 PK。无法枚举 PK、但会整体改变数据可见性的 `TruncateCollection` 使用轻量
`collectionFenceIndex`。

新 term 不继承旧 term 的 PK index，也不执行 cold warm-up。正确性由
`term -> readTS -> query` 顺序保证：Proxy 只能在观察到当前 term 后分配该 attempt
的 `readTS`。如果 term 在 query 或 commit 前变化，SN 通过 term mismatch 拒绝旧
attempt。

WAL recovery 保持原有 transaction 语义。Partial-update interceptor 不读取 TxnBuffer，
而是用是否在当前生命周期观察到 `BeginTxn` 判断 runtime write set 是否完整。普通或
replicated recovered transaction 可以继续提交，但会发布 vchannel 级保守 fence；本地
CAS 缺少完整 runtime proof 时返回 retryable error，由 Proxy 重建整个 attempt。

该方案不新增 Proxy 到 SN 的 RPC，不新增 partial-update 专用 WAL message，也不使用
`AppendResult.Extra` 返回行级结果。

## 背景

当前 partial update 在 Proxy 中先读取旧行，再把用户字段 merge 成完整行，最后向
WAL 追加 Delete 和 Insert。读与提交之间没有冲突检查，因此两个并发请求可能读取
同一个旧版本，并用各自的 merge 结果互相覆盖。

把查询和 merge 下沉到 SN 会引入 QueryCoord client、shard leader 路由、schema
处理和 merge 逻辑，改动范围过大。SN 已经拥有 WAL 排序点，因此本方案保留 Proxy
读写路径，只把 commit-side CAS 放到 SN。

## 目标

- 同一 PK 的并发 partial update 不产生 lost update。
- 查询路由和 merge 逻辑保留在 Proxy。
- 下游仍消费标准 Delete/Insert WAL transaction。
- 客户端 API 保持不变。
- SN 重启或 channel 迁移后不能把索引缺失当作无冲突。
- 不引入需要定时 warm-up 或后台重建的 index readiness 状态机。

## 非目标

- 不提供跨 vchannel 或跨 collection 原子性。
- 不提供 strict serializability。
- 不持久化独立 row-version storage。
- MVP 不改变现有 Streaming transaction 在断流、超时或响应丢失时的自动重发和
  ambiguous-outcome 语义，也不提供 transaction-level exactly-once 或 idempotency token。
- MVP 不对 `ARRAY_APPEND`、`ARRAY_REMOVE` 的确定性 CAS conflict 执行自动重试。
- MVP 暂不保证 partial update 与 Import、RestoreSnapshot、backfill 等独立数据可见性
  流程并发执行时的正确性。

## 正确性契约

### Attempt 顺序

每次 attempt 必须遵守：

```text
route original PKs to vchannels
  -> resolve and snapshot every touched PChannel term
  -> allocate readTS from TSO
  -> query with GuaranteeTimestamp = MvccTimestamp = readTS
  -> attach the same term and readTS to CAS metadata
```

不能先分配 `readTS` 再解析 term。否则新 owner 可能已经启动，但该 attempt 的
`readTS` 仍早于新 term，空的 per-term index 无法证明这段时间内的写入历史。

调度 task 的 ID 和 `BeginTs` 在整个请求生命周期内保持不变。`readTS` 是独立的
attempt-scoped timestamp，首次执行和每次 retry 都重新分配。

### Read fence

Partial update 内部查询使用：

```text
ConsistencyLevel = Customized
GuaranteeTimestamp = readTS
MvccTimestamp = readTS
```

`queryTask.PreExecute` 之后仍需把 `GuaranteeTimestamp` 和 `MvccTimestamp` 固定为
`readTS`，确保 QueryNode 实际读取的 MVCC 快照和 CAS metadata 完全一致。

### Term fence

Proxy 把查询前观察到的 term 写入 metadata：

```text
observedPChannelTerm = assignment.Channel.Term
```

SN admission 首先比较当前 WAL term。读和提交之间发生 SN 重启或 channel 迁移时，
旧 attempt 必须失败并由 Proxy 重新执行完整流程。

term 只处理 owner 生命周期变化。它不能发现同一 term 内发生的普通 DML 或 Truncate，
因此仍需要 PK version index 和 collection fence。Import 和 RestoreSnapshot 不在本方案
的并发正确性范围内。

### Commit fence

本地 CAS `CommitTxn` 在 lock interceptor 中获取 vchannel 写锁。该锁覆盖 TimeTick
分配、transaction body 收敛、CAS admission、WAL append、index publication 和
transaction state transition：

```text
acquire glock.RLock + vchannel.Lock
  -> allocate commitTS
  -> RequestCommitAndWait
  -> verify marker, runtime state, term, retention, fence and PK versions
  -> append CommitTxn
  -> publish PK/fence indexes
  -> CommitDone or RejectCommit
release vchannel.Lock + glock.RUnlock
```

CAS reject 必须满足：

- `CommitTxn` 不写入 WAL；
- transaction body 对 consumer 不可见；
- txn session 不进入 committed 状态；
- admission state 和 txn metadata 被清理。

### VChannel admission lock

lock interceptor 使用统一的层级顺序：先获取 PChannel `glock`，再获取 vchannel lock，
释放时顺序相反。

| 消息 | 锁 |
|---|---|
| 普通 DML、transaction body、普通 CommitTxn | `glock.RLock + vchannel.RLock` |
| 本地 CAS CommitTxn | `glock.RLock + vchannel.Lock` |
| vchannel exclusive DDL | `glock.RLock + vchannel.Lock` |
| PChannel exclusive DDL | `glock.Lock` |

CAS CommitTxn 使用专用分支，不能触发 exclusive DDL 的 `FailTxnAtVChannel`，否则会终止
正在提交的 CAS transaction 自身。

普通写入一直持有 vchannel 读锁，直到 WAL append 和 PK/fence publication 都完成。
因此 CAS 获得写锁时，同一 vchannel 不可能存在已经开始、但尚未发布 write index 的
普通写入；CAS 成功后也会在发布 index 之前继续持有写锁。该顺序同时解决两种竞态：

- 普通写先进入时，CAS 等待其 publication 后再校验；
- CAS 先进入时，后续普通写等待 CAS append 和 publication 完成。

同一 vchannel 的 CAS commit 会全部串行，即使 PK 不相交；不同 vchannel 仍使用不同的
锁，可以并行。锁只覆盖 commit 临界区，不覆盖 Proxy query、merge 或 transaction body
append。

### Complete write coverage

所有纳入本方案支持范围、且会改变 logical row data 的写入必须进入 CAS proof：

- Delete 从消息主键列表提取 PK；
- CAS Insert 使用 metadata 中的 PK field ID 从 Insert body 提取 PK；
- shard interceptor 先完成普通 Insert 的 schema version 校验，再由 partial-update
  interceptor 使用 ShardManager schema 提取 PK；
- shard 兼容接受、但没有可用 schema 的 legacy Insert 使用 collection fence，不能直接
  跳过 write coverage；
- transaction 内的精确 PK 在 `CommitTxn` 成功后统一发布；
- `TruncateCollection` 更新 collection fence；
- Flush、segment lifecycle、index、Drop、普通 AlterCollection 等不更新 row version。

Import、RestoreSnapshot、backfill 等独立可见性流程暂不加入 proof，也不通过额外 gate
阻止并发 partial update。调用方必须避免这些操作与 partial update 并发执行。

## 请求流程

```text
Client Upsert(partial)
  -> Proxy routes original PKs
  -> Proxy resolves assignment terms
  -> Proxy allocates attempt readTS
  -> Proxy queries at readTS
  -> Proxy merges current row and partial fields
  -> Proxy builds standard Delete/Insert DML
  -> Proxy attaches PartialUpdateCAS metadata to every Insert chunk
  -> streaming.WAL().AppendMessages(...)
  -> producer wraps same-vchannel DML in a transaction
  -> SN records transaction PKs and metadata
  -> producer marks the local CAS CommitTxn
  -> lock interceptor acquires the vchannel write lock
  -> TimeTick waits for transaction body completion
  -> partial-update interceptor validates and appends CommitTxn
  -> partial-update interceptor publishes indexes before unlock
  -> WAL backend
```

Proxy 按现有 PK hash 和 namespace sharding 规则 fan-out。每个 vchannel group 是独立
transaction；跨 vchannel 不提供原子性。即使某个 group 只有一条 CAS Insert，也必须
经过 transaction path。

## Proxy 设计

### 查询和 merge

Proxy 继续使用现有 `queryPreExecute` 和 merge 行为，包括 nullable/default、dynamic
field、generated function output、array operation、nullable vector compact format 和
partition key immutability check。

AutoID partial update 采用 update-only 语义。请求必须携带已有 PK，query snapshot
必须返回全部请求行；任意 PK 不存在时，Proxy 在 WAL append 前拒绝整个请求。merge
后的 Insert 保留原 PK，因此 Delete、Insert 和 CAS 始终按同一 PK 路由到同一
vchannel。内部 RowID 仍按现有逻辑重新分配。普通 AutoID upsert 继续分配新 PK，
不受该规则影响。

### CAS metadata

Proxy 使用原始请求 PK 计算 attempt 涉及的 vchannel。metadata 不复制 PK，只记录
PK field ID；每个 Insert chunk 携带固定大小 metadata，SN 从该 chunk 的完整行
payload 提取 PK。

```proto
message PartialUpdateCAS {
    uint64 read_ts = 1;
    int64 observed_pchannel_term = 2;
    int64 primary_key_field_id = 3;
    int64 collection_id = 4;
}
```

metadata 写入 Insert body 的 `MsgBase.Properties["_puc"]`，因此 cluster encryption
启用时与 DML payload 处于同一加密边界。外层 message property 只保留空值 `_puc`
marker，供 producer 强制选择 transaction path。Proxy repack 必须在 message builder
执行 encryption 和 `BuildMutable()` 前写入 metadata；pack 完成后的检查只验证 marker、
vchannel snapshot 和消息大小。若 marker 缺失，Proxy 直接返回 internal error，不重写
已经构造或加密的 message body。

### Proxy 重试

SN 使用 `STREAMING_CODE_PARTIAL_UPDATE_RETRYABLE` 表示确定性 CAS reject。Producer
必须把该错误直接返回 Proxy，不能在 producer 内重试携带旧 `readTS` 和旧 term 的
transaction。

如果 CAS transaction 在 producer 内遇到 `TxnExpired`，producer 将其转换为
retryable CAS error 并交还 Proxy，从 term resolution 开始重建完整 attempt；普通
transaction 保留已有的 producer 内过期重试。

Proxy 只对可以安全重建的 `REPLACE` partial update 自动重试确定性 CAS reject。每次
retry 必须：

1. 恢复首次 function generation 和 query/merge 前保存的原始 partial payload；
2. 重新生成 function output fields；
3. 重新路由 PK 并获取 assignment terms；
4. 在 terms 获取成功后分配新的 attempt `readTS`；
5. 使用新 `readTS` 重新 query 和 merge；
6. 重建 MutationResult 计数和 WAL transaction。

非幂等相对操作遇到 CAS reject 时，Proxy 返回
`ErrCollectionPartialUpdateConflict`，并设置 `Retriable=false`，阻止 SDK 自动重放
`ARRAY_APPEND` 或 `ARRAY_REMOVE`。

上述 retry 契约只覆盖 SN 明确在 CommitTxn 进入 WAL append 前返回的
`STREAMING_CODE_PARTIAL_UPDATE_RETRYABLE`。BeginTxn、transaction body 或 CommitTxn
发送后的 transport failure 继续沿用现有 resumable producer 语义，消息可能在 producer
内部重发，最终 request outcome 也可能不确定。本方案不保证原始 transport error 一定
到达 Proxy，也不解决 transaction-level exactly-once。

## StreamingNode 设计

### Per-term PK version index

每个 PChannel term 创建一个 registry，并按 vchannel 拆分可变索引：

```text
registry[vchannel].pkLastWriteTS[pk] = lastCommitTS
```

CAS 冲突规则：

```text
conflict iff pkLastWriteTS[vchannel, pk] > readTS
```

新 term 创建新 index，旧 term index 不复用。因为 Proxy 在解析当前 term 后才分配
`readTS`，新 index 只需要覆盖本 term 中 `readTS` 之后发生的写入，不需要等待一个
完整 TTL window。

### Retention window

PK index 使用固定 TTL 和 PChannel 级共享字节预算，默认值为 30 秒和
640,000,000 estimated bytes。每个 entry 按 128 bytes 固定结构开销加 VarChar PK
实际长度保守计费，因此默认预算约等价于 5M 个 Int64 PK entry。每个 vchannel 独立持有
map、expiration heap、`retainedSinceTS` 和 `lastMissedWriteTS`，Update、Verify 只获取目标
vchannel 的锁；不同 vchannel 通过原子操作共享字节预算。

Update、Verify 和 TimeTick advance 根据当前 TS 增量淘汰过期 entry，然后执行以下检查：

- `readTS < retainedSinceTS` 时 fail closed；
- `commitTS < readTS` 时返回 unrecoverable internal error；
- read-to-commit 物理时间超过 TTL 时返回 retryable CAS error；
- 已存在 PK 的版本更新不增加 entry 数；
- 新 distinct PK 无法获得字节预算时不再分配 entry，并将对应 vchannel 的
  `lastMissedWriteTS` 推进到该写入 TS；
- `lastMissedWriteTS` 仍处于有效 read window 时，该 vchannel 的 CAS 全部 fail closed，
  其他 vchannel 不受影响。

每次遗漏写入都会延长 incomplete window。只有最后一次遗漏写入早于
`currentTS - TTL` 后，`lastMissedWriteTS` 才会清零并恢复 CAS。TimeTick 即使在没有新
DML 时也会推进淘汰，因此索引不会因空闲而长期保留过期 entry。持续超过预算时，普通
写入继续，但发生遗漏的 vchannel CAS 保持 fail closed；entry 淘汰时原子归还字节预算。

### Collection fence index

无法按 PK 表达的 wide mutation 使用：

```text
fenceTS[vchannel, collection] = lastWideMutationCommitTS
```

如果 `fenceTS > readTS`，partial update 必须重新查询。当前只覆盖：

- `TruncateCollection`；
- shard 兼容接受、但当前无法取得 schema 的 legacy Insert。

普通 Insert 通过 ShardManager 的 `GetPrimaryKeyDescriptor` 获取不可变的 PK field ID 和
data type，再从 `FieldsData` 精确提取 PK。Descriptor 在 schema recovery、CreateCollection
和 AlterCollection 时预计算，热路径不 clone 完整 schema，也不在 partial-update
interceptor 中维护第二份 schema cache。没有携带 schema version、且 shard 为兼容旧
Proxy 而允许写入的 legacy Insert，如果当前没有可用 schema，则更新 collection fence。
DropCollection、DropPartition 和 AlterCollection 依赖现有 exclusive lock、collection
lifecycle 和 schema mismatch 路径，不进入 collection fence。

Fence 在当前 PChannel term 的 collection 生命周期内保留，不按 TTL 淘汰。
`DropCollection` 成功写入后释放目标 vchannel 的 PK map、expiration heap、retention 和
遗漏写入状态，归还共享 byte budget，并删除 incomplete-transaction fence 和对应
`(vchannel, collection)` fence。WAL term 关闭时整体释放该 term 的全部 proof state。

### Transaction state

运行时 interceptor 顺序是：

```text
redo -> lock -> replicate -> timetick -> shard -> partialupdate -> WAL
```

lock 是最外层并发边界，因此它持有的 vchannel 锁覆盖所有内层处理。shard 在
partial-update tracking 前保留既有 schema compatibility 和 typed mismatch 语义。
TimeTick 在进入 partial-update 前完成 `RequestCommitAndWait`；partial-update 返回成功后，
TimeTick 才调用 `CommitDone()`。只有明确发生在 WAL append 前的 admission reject 才调用
`RejectCommit()`；其他 inner error 仍按既有行为调用 `CommitDone()`，因为 WAL append error
不能证明 `CommitTxn` 未持久化。

该约束只定义 SN 内部 TxnSession 的状态迁移，不改变 resumable producer 的 transport
重发策略，也不保证 inner append error 会原样到达 Proxy。

Partial-update append interceptor 在 body append 成功后记录：

- 当前 WAL lifecycle 是否观察到 BeginTxn；
- CAS Insert chunk 的 PK；
- transaction 中普通 Insert/Delete 的精确 PK；
- optional `PartialUpdateCAS` metadata；
- optional Truncate collection fence。

处理 CommitTxn 时，interceptor 从 runtime `pendingTxn` 取得稳定 write set。只有当前
生命周期观察到 BeginTxn，write set 才被视为完整。本地 CAS 缺少完整 proof 时直接返回
retryable error；普通或 replicated transaction 仍按原语义提交，并在 write set 不完整时
发布 vchannel 级保守 fence。完整的本地 CAS 再校验 marker、term、read window、PK
versions、vchannel fence 和 collection fence。CommitTxn append 成功后使用 commit
TimeTick 发布 write set 或保守 fence；reject、append error、rollback 和 expiration 不发布。
Admission reject 使用进程内 marker 与 WAL append error 区分，不新增 wire error code。

## Recovery 和迁移

### WAL open

新 WAL owner 创建全新的 per-term index，不执行 cold warm-up。Partial-update builder
不读取 `InitialRecoverSnapshot.TxnBuffer`，也不查询历史 schema。

TxnManager 完全保留既有 WAL recovery 行为，recovered session 可以继续接收 body、Commit
或 Rollback。Partial-update interceptor 不读取 `recoveredSessions`，也不修改
`RecoverDone`：

- 当前 interceptor 生命周期观察到 BeginTxn 时，后续成功 body 构成完整 runtime write set；
- 只观察到 body suffix 或 CommitTxn 时，write set 被视为不完整；
- 本地 CAS CommitTxn 缺少完整 write set 时不进入 WAL，返回 retryable CAS error；
- 普通或 replicated CommitTxn 缺少完整 write set时仍正常提交，成功后写入
  `incompleteTxnFence[vchannel] = commitTS`；
- 后续 CAS 如果 `readTS < incompleteTxnFence`，必须重新查询。retry 的新 `readTS` 晚于
  fence，因此可以观察已恢复事务的提交结果。

该策略不改变普通 producer、TxnBuffer、LastConfirmedMessageID 或 CDC 的恢复语义，也不
依赖历史 schema。vchannel fence 只保守影响与恢复事务并发的 partial update。

### Replication

Replicated transaction 已经在 primary 完成 admission，secondary 不再使用 primary
`readTS` 做 CAS。CDC 从 BeginTxn 完整重放时发布精确 PK/fence index；只恢复到 body suffix
或 CommitTxn 时继续原有提交并发布 vchannel 保守 fence。Secondary promotion 会产生新
term，并按相同规则创建新的 per-term index。

### Import 和 RestoreSnapshot

Import、RestoreSnapshot 和 backfill 的数据可见性不完全由目标 VChannel WAL 表达，
当前版本不为这些操作增加 collection fence、持久 gate 或 Proxy 侧并发门禁。因此，
它们与 partial update 并发执行不在本方案的正确性保证范围内。

## 失败语义

| 场景 | 结果 |
|------|------|
| 同一 PK 的并发 partial update | 一个 commit，另一个由 SN 返回 retryable CAS error；Proxy 只重试 `REPLACE` |
| 普通 DML 在 `readTS` 后修改同一 PK | SN 返回 retryable CAS error；相对更新投影为 non-retriable client error |
| Truncate 在 `readTS` 后提交 | collection fence conflict |
| Import、RestoreSnapshot 或 backfill 与 partial update 并发 | 不在当前正确性保证范围内 |
| PChannel term 改变 | term mismatch，Proxy 重新执行 attempt |
| read window 过期或 capacity 导致索引 incomplete | retryable CAS error |
| recovered 普通或 replicated transaction 提交 | 保持原事务恢复语义；write set 不完整时发布 vchannel fence |
| recovered 本地 CAS transaction 提交 | 返回 retryable CAS error，Proxy 重建 attempt |
| malformed internal CAS metadata | unrecoverable streaming error |
| transaction message 发送后的断流、超时或响应丢失 | 继承现有 resumable producer 语义；结果可能不确定，transaction-level exactly-once 不在本方案范围内 |

CAS error 对被拒绝的单个 vchannel transaction 是确定性 abort：其 `CommitTxn` 没有
写入。Proxy 只对可安全重建的 `REPLACE` 从 term resolution 阶段自动重试；相对更新可能已经在
其他 vchannel 提交，因此不能自动重放整个请求。

## 验证要求

1. Proxy 必须按 `terms -> readTS -> query` 顺序执行首次 attempt 和 retry。
2. term 变化后创建全新 PK index，不存在 warm-up 等待。
3. 普通 Insert 在 shard schema 校验后提取 Int64/Varchar PK；schema mismatch 保持 typed
   error，合法 schema-less legacy Insert 使用 collection fence。
4. Drop/Alter 不更新 collection fence；Truncate 和 schema-less legacy Insert 更新
   fence；Import 和 RestoreSnapshot 不进入 proof。
5. DropCollection 成功后清理目标 vchannel 的全部 proof state 并归还 PK byte budget；
   append 失败时保留原状态。
6. 普通写入先持有 vchannel 读锁并阻塞 WAL append 时，CAS commit 必须等待该写入完成
   publication，再检测 PK 或 fence conflict。
7. 两个 CAS transaction 使用相同 `readTS` 修改同一 PK 时，只能有一个写入
   `CommitTxn`。
8. 同一 vchannel 的 CAS commit 串行，不同 vchannel 的 CAS commit 可以并行；PChannel
   exclusive 操作必须等待所有 shared holder。
9. CAS 先持有 vchannel 写锁时，后续普通写只能在 CommitTxn append 和 index publication
   完成后进入。
10. 普通和 replicated recovered transaction 必须保持既有提交语义；没有观察到当前
    lifecycle BeginTxn 时，成功提交后必须发布 vchannel 保守 fence。
11. recovered 本地 CAS 缺少完整 runtime proof 时必须返回 retryable CAS error；完整
    replicated CAS 跳过源 term/readTS proof，不完整 replay 使用 vchannel fence。
12. CAS reject 不写 `CommitTxn`、不调用 `CommitDone()`，并清理 transaction state。
13. retry 保持 task ID/BeginTs 稳定，并恢复原始 payload、函数输出和结果计数。
14. PK version entry 的估算内存不超过 PChannel 字节预算；遗漏写入退出 TTL 前，只有对应
    vchannel 的 CAS fail closed。
15. TimeTick 在没有新 DML 时也能推进 PK version 淘汰和 incomplete 恢复。

## 性能和容量

新增成本包括：

- Proxy partial update 每个 attempt 执行一次 query；
- SN 对普通 Insert/Delete 提取 PK 并更新近期 index；
- CAS CommitTxn 在同一 vchannel 的写锁内完成 TimeTick 分配、validation、WAL append 和
  index publication；
- 同一 vchannel 的所有 CAS commit 串行，即使 PK 不相交；不同 vchannel 保持并行；
- 慢 WAL append 会延长对应 vchannel 的写锁持有时间，但不会阻塞同一 PChannel 上其他
  vchannel；
- Truncate 更新一个 collection fence entry。

字节预算至少覆盖一个 TTL window 内的 distinct PK 写入量：

```text
required bytes ~= sum(128 + varchar PK bytes) within TTL
```

默认 640,000,000 bytes 和 30 秒 TTL 对 Int64 PK 约覆盖 5M entries，即 166k distinct
PK/s。超过预算时新 distinct PK 不再进入 index，发生遗漏的 vchannel CAS fail closed；
普通写入和 TimeTick 继续推进 retention。最后一次遗漏写入退出 TTL window 后 CAS 自动恢复。

当前验证覆盖并发正确性、错误链、race 和新增代码覆盖率，但尚未执行生产规模 benchmark。
以下判断仍是待验证假设：同一 vchannel 的 CAS commit 排队不会造成不可接受的 p99
append latency；慢 WAL append 持有写锁时，对同 vchannel 普通 DML 的阻塞可控；全量
DML PK 提取、heap 更新以及默认字节预算的 CPU 和内存成本在目标写入速率下可接受。
发布前应覆盖低冲突、高冲突和慢 WAL 三类场景，并记录锁等待时间、commit latency、
普通 DML throughput、index entry 数、estimated bytes 和 budget miss。

## 回滚

Partial update CAS 是 streaming partial update 的默认行为，不提供独立配置开关。
回滚代码后 partial update 恢复 legacy read-merge-write 语义，并重新暴露 lost-update
风险。

## 开放问题

- vchannel 写锁在高 CAS 密度和慢 WAL 下的 p99 latency 与普通 DML 吞吐影响。
- 高 distinct-PK churn 和长 VarChar PK 下默认字节预算是否足够。
- 全量 DML PK extraction 对吞吐的影响。
- 后续如何协调 Import、RestoreSnapshot、backfill 等独立数据可见性流程与
  partial update。
- 是否需要在 Streaming transaction 层引入 request token 和持久化结果去重，以支持
  BeginTxn、body 和 CommitTxn 的 exactly-once outcome。
