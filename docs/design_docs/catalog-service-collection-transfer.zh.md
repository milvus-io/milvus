# Milvus Catalog Service Collection Transfer 设计文档

状态：基于当前 RootCoord metadata transfer demo 和多 agent 评审整理的设计草案。

日期：2026-08-03

## 摘要

Milvus Catalog Service 的目标不是把 etcd 或 TiKV 包成一个 KV 服务，而是提供稳定的 Milvus metadata control plane。Collection transfer 的本质是把一张 collection 的 ownership 从一个 Milvus namespace 切换到另一个 namespace，因此应该通过语义化的 Catalog API 和可恢复的管控工作流完成，而不是复制底层 KV key。

当前 demo 已经证明了基本链路：

- 两个 Milvus standalone 进程分别作为 `milvus1` 和 `milvus2` 两个 namespace。
- 两个 namespace 的 RootCoord metadata 都放在 TiKV，但使用不同 namespace root 隔离。
- Catalog Service 使用 TiKV 作为 metadata backend。
- Transfer 请求把 `milvus1` 的 collection 转移到 `milvus2`。
- Source RootCoord 在 Milvus 进程不停机的情况下，对该 collection 做 drain/fence。
- Source deactivation 后 invalidate source cache。
- Target RootCoord 把已转移的 metadata apply 到 live state，并 invalidate target cache。
- Transfer 后 source describe/insert 失败。
- Transfer 后 target describe collection 和 alias 成功。

这说明方向成立，但当前仍是 engineering preview，不是 production GA。生产化还必须补 durable fencing、owner/visibility state、reconciler recovery、多实例安全、治理审批、审计、安全、可观测性，以及 offline transfer 与 online transfer 的明确边界。

## 目标

生产目标是在多个 Milvus namespace 之间安全、可观测、可恢复地转移 collection metadata ownership。

第一阶段 GA 应限定为 offline 或 drained collection metadata transfer：

- Source collection 在 ownership 切换前必须 drain/fence。
- Target collection 在 activation 前不能对用户可见。
- 任意时刻最多只有一个 namespace 能接受该 collection 的用户写入。
- Catalog metadata mutation 必须 durable 且可恢复。
- Operator 可以查看状态、pause、resume、在安全阶段 abort，或在不确定阶段 roll forward。
- 权限、审批和审计是一等能力。

## 第一阶段非目标

第一阶段 GA 不应承诺在线数据面迁移。

以下能力不属于第一阶段：

- Active DML catch-up。
- WAL barrier promotion。
- Active loaded collection 的 DataCoord segment/binlog/index metadata 迁移。
- QueryCoord load-state 或 replica 迁移。
- QueryNode loaded segment handoff。
- 跨 region object movement。
- Transfer 过程中 search/query 完全无感连续。

这些应该作为 offline metadata transfer GA 之后的独立 online transfer 项目。

## 核心原则

Catalog Service 应该暴露语义化 metadata API，而不是 raw KV API。

Open-source Milvus 应继续保持 etcd 作为默认 metadata implementation。Cloud 或 managed deployment 可以使用 shared Catalog Service，后端可以是 TiKV、Oxia 或其他存储。TiKV 是接口下面的 backend implementation，不是产品边界。

Catalog 只拥有 persistent metadata，不拥有所有 Milvus runtime state。

RootCoord 或 namespace-level runtime authority 拥有本 namespace 的 live state：lock、gate、cache invalidation、in-flight operation drain、live MetaTable mutation。

Catalog Service 不能直接 fanout 到每个 Proxy、DataCoord、QueryCoord、DataNode、QueryNode、StreamingNode。它应该通知 namespace authority，由 namespace authority 在 Milvus 内部完成 fanout。

## 架构

整体分三层：

```text
Governance / Transfer Manager
  权限、审批、dry-run、workflow、retry、recovery、audit、operator API

Catalog Service
  semantic metadata API、durable owner state、transfer job state、CAS、backend abstraction

Milvus Namespace Runtime
  RootCoord 或 Namespace Control Plane Gateway，负责 namespace 内部 fanout
```

当前 demo 里，Transfer Manager 还在 Catalog Service 进程内，RootCoord 作为 namespace authority。

生产上第一版可以仍然同进程部署，但逻辑边界必须清楚。否则 Catalog Service 会从稳定 metadata interface 膨胀成无限边界的云管控业务系统。

## 职责边界

### Catalog Service

Catalog Service 是 durable metadata authority。

职责：

- 保存语义化 metadata。
- 保存 transfer job。
- 保存 idempotency record。
- 分配或校验 collection owner epoch。
- 维护 owner record 和 visibility record。
- 执行条件化 metadata update。
- 屏蔽 etcd/TiKV/Oxia 等 backend 差异。
- 向 Milvus 和 Transfer Manager 暴露 metadata API。

Catalog Service 不应该：

- 直接 invalidate Proxy cache。
- 直接 drain DataNode 或 QueryNode。
- 直接管理 QueryCoord load state。
- 直接理解每个 runtime node 的拓扑。
- 把 raw KV mutation 暴露成 public contract。

### Transfer Manager

Transfer Manager 是 workflow coordinator。

职责：

- 校验 transfer intent。
- 执行 dry-run。
- 强制执行 governance policy。
- 推进 transfer 状态机。
- 调用 source 和 target namespace authority。
- 重试幂等步骤。
- 发现 stuck job 和 commit uncertain job。
- 根据 durable facts 决定 abort 或 roll forward。
- 发布事件、指标、trace 和审计。

MVP 可以把 Transfer Manager 嵌在 Catalog Service 里，但它应该作为独立模块和 API 边界存在。

### RootCoord / Namespace Runtime

RootCoord 是 RootCoord metadata transfer 第一阶段的 namespace authority。

职责：

- Freeze collection-level metadata operation。
- Drain 已经进入 RootCoord 的 in-flight operation。
- Transfer 开始后拒绝 source 上该 collection 的用户操作。
- Deactivate source live metadata。
- Apply target live metadata。
- Invalidate collection name、collection id、alias 的 Proxy cache。
- 从 durable marker 恢复 transfer fence，且必须在 RootCoord 对外 healthy 前恢复。

长期可以把 RootCoord transfer RPC 抽象成 Namespace Runtime Service。该服务负责协调 RootCoord、DataCoord、QueryCoord、StreamingCoord、Proxy 和 worker node。

### Proxy、DataCoord、QueryCoord、Node

这些组件不应该成为 Catalog Service 的直接通知目标。

它们应该接受本 namespace control plane 的指令：

- Proxy 负责 client request routing 和本地 metadata cache。
- DataCoord 负责 segment、binlog、import、compaction、index 相关数据面 metadata。
- QueryCoord 负责 load state、replica、resource group、target、shard leader。
- Node 只执行自己的本地 runtime state。

这样可以避免 Catalog Service 变成全局 runtime orchestrator。

## 为什么 Catalog Service 要通知 RootCoord

Collection transfer 改变的是 runtime ownership，不只是 durable metadata。

如果 Catalog Service 只改 TiKV，系统可能出现 split-brain：

```text
TiKV 认为 collection 已经属于 target namespace
Source RootCoord 内存里仍然有 collection
Source Proxy cache 仍可能把请求路由到旧 owner
Target RootCoord 内存里还没加载 collection
```

因此 Catalog Service 必须协调 RootCoord：

```text
Catalog Service -> source RootCoord: prepare, fence, drain
Catalog Service -> Catalog backend: move durable metadata
Catalog Service -> source RootCoord: deactivate live metadata and expire caches
Catalog Service -> target RootCoord: apply live metadata and expire caches
```

这不是说 Catalog Service 未来要通知所有 node。RootCoord 只是当前 metadata scope 下的 namespace authority。

## 为什么不是 raw KV copy

Raw KV copy 会把 transfer 正确性绑定到底层 key layout。

问题：

- 绕过 RootCoord catalog 语义。
- 绕过 alias index 和 model validation。
- 无法 drain live operation。
- 无法正确 invalidate Proxy cache。
- 难以处理 metadata schema evolution。
- 把 TiKV/etcd key layout 变成 API contract。

推荐模型是 logical model transfer：

```text
读取 source collection semantic model
校验 target conflict
stage 或 create target semantic model
remove 或 mark source semantic model moved out
通过 namespace runtime activate target
```

当前 demo 使用 source drop + target create，是为了兼容现有 per-namespace RootCoordCatalog 模型。生产应该演进到 owner record + visibility state，而不是只依赖 delete/create 的时序。

## Namespace 和 RootCoord 发现

当前 demo 使用静态路由：

```text
milvus1 -> source RootCoord address
milvus2 -> target RootCoord address
```

这只适合 demo。

生产需要动态 namespace authority discovery：

```text
namespace -> Milvus cluster
Milvus cluster -> current RootCoord leader
RootCoord leader -> authenticated gRPC channel
```

推荐发现来源：

- Catalog Registry session record。
- 过渡期复用 Milvus 现有 session/discovery metadata。
- Kubernetes service/endpoints 加 Milvus leader/session state。
- Cloud control-plane cluster registry。

RootCoord 注册信息至少包含：

```text
namespace
role
endpoint
leader_epoch
lease/session_revision
version/capabilities
health_state
```

Catalog Service 必须使用 fencing token 拒绝 stale leader。

## 一致性模型

生产里的核心记录应该是 collection owner record，而不是“目标 KV 下是否有这批 key”。

owner record 至少包含：

```text
collection_id
db_id
db_name
collection_name
source_namespace
target_namespace
owner_namespace
owner_epoch
transfer_id
transfer_epoch
visibility_state
metadata_revision
request_hash
created_at
updated_at
```

owner epoch 是 collection 级 fencing token。它和 Catalog Service 进程 leader epoch 不同。Catalog Service leader epoch 只能证明谁可以推进 job，owner epoch 才能证明哪个 namespace 可以 mutate collection。

正确性目标是 single-owner visibility：

- 任意时刻最多一个 namespace 可以接受该 collection 写入。
- Target durable metadata 可以提前存在，但必须 hidden。
- Source ownership 被移除前必须先 fence。
- Source drop 之后不能悄悄 abort，默认必须 roll forward。
- Cache invalidation 只是加速收敛，不是 correctness barrier。
- 即使 cache invalidation 延迟，RootCoord/Proxy 也必须通过 owner/fence state 拒绝旧 owner 操作。

## Visibility State

推荐的生产 visibility state：

```text
ACTIVE
SOURCE_FENCED
TARGET_HIDDEN
TARGET_VISIBLE
TRANSFERRED_OUT
```

`ACTIVE`：collection 在 owner namespace 内可见可写。

`SOURCE_FENCED`：source namespace 已 freeze 并 drain，新用户操作失败。如果 persistent ownership 还没有越过不可回滚点，该阶段仍可 abort。

`TARGET_HIDDEN`：target catalog 中已经有 staged collection bundle，但用户不可见、不可使用。

`TARGET_VISIBLE`：target RootCoord 已 apply，target Proxy 可以 resolve。

`TRANSFERRED_OUT`：source RootCoord 对该 owner epoch 永久拒绝，并 invalidate source cache。

## Transfer 状态机

要区分 workflow progress 和 ownership facts。Job state 描述运维进度，visibility state 描述读写权限事实。

Workflow state：

```text
PENDING
PREPARED
SOURCE_DROPPED
CATALOG_MOVED
SOURCE_DEACTIVATED
TARGET_APPLIED
DONE
ABORTED
FAILED
COMMIT_UNCERTAIN
PAUSED_OPERATOR_REQUIRED
```

主路径：

```text
PENDING
  -> PREPARED / SOURCE_FENCED
  -> SOURCE_DROPPED
  -> CATALOG_MOVED / TARGET_HIDDEN
  -> SOURCE_DEACTIVATED / TRANSFERRED_OUT
  -> TARGET_APPLIED / TARGET_VISIBLE
  -> DONE
```

`ABORTED` 只能发生在不可回滚点之前。

source persistent metadata 被移除，或 owner 切离 source 之后，默认恢复方向必须是 roll forward。

`COMMIT_UNCERTAIN` 是一等状态，用于处理 backend commit 不确定、进程 crash 在 commit 边界、durable facts 不一致等情况。

## 生产 Workflow

推荐生产流程：

```text
1. 创建 transfer request
2. 执行 dry-run validation
3. 校验 source 和 target authorization
4. 必要时完成审批
5. 分配 transfer epoch 和 owner epoch
6. 创建 durable owner/transfer marker
7. 请求 source namespace authority prepare
8. Source authority fence 并 drain collection
9. 校验 source snapshot 未变化
10. Preflight target namespace
11. Stage target metadata as hidden
12. 原子推进 owner/job state，越过不可回滚点
13. Deactivate source live metadata 并 invalidate source cache
14. Apply target live metadata
15. Mark target visible
16. Finalize job as done
17. 每个 phase 发 audit 和 metrics
```

当前 demo 简化为：

```text
prepare source
drop source metadata
create target metadata
deactivate source live state
apply target live state
mark done
```

它能证明架构形状，但不能作为 GA 语义。

## 故障恢复

生产恢复模型应该是 idempotent + roll-forward by default。

每个外部副作用都必须能安全重试：

- 同一个 transfer id/epoch 的 prepare 重试后保持 fenced。
- Target staging 如果发现等价 metadata 已存在，则视为成功。
- Source deactivation 重试后保持 transferred-out。
- Target apply 如果 live metadata 已经等价装载，则视为成功。
- Cache invalidation 可以重试。

Rollback 只在 source persistent ownership 尚未移除前安全。

Source drop 或 owner switch 之后，默认恢复方向应该是 roll forward，除非 operator 能明确证明 rollback 安全。

Reconciler 是生产必需组件：

```text
扫描 non-terminal job
使用 durable lease/CAS claim job step
读取 source facts、target facts、owner record、runtime state
决定 retry、advance、pause、abort 或 roll forward
记录 phase event 和 retry metadata
超过 SLO 后告警
```

关键恢复场景：

```text
PREPARED 且 source 仍 fenced:
  retry preflight，或在没有 target side effect 时 abort

PREPARED 但 source collection 已不存在:
  不能盲目 abort；需要检查 source tombstone 或进入 COMMIT_UNCERTAIN

SOURCE_DROPPED 且 target missing:
  使用 job 保存的 collection snapshot 继续创建 target hidden metadata

CATALOG_MOVED 且 target hidden exists:
  retry source deactivate

SOURCE_DEACTIVATED 且 target not visible:
  retry target apply

TARGET_APPLIED 但 job not DONE:
  verify owner/visibility 后 finalize
```

## Durable Atomicity 要求

最重要的 GA 修复是 source drop atomicity。

下面这些必须是一个 durable conditional transition：

```text
source persistent ownership removed or tombstoned
owner record advanced
job state advanced
source transfer marker updated
```

如果 backend 暂时不能原子更新所有相关记录，workflow 必须把这个边界显式建模为 `COMMIT_UNCERTAIN`，再由 reconciler 读取事实后处理。

设计必须避免这种状态：

```text
source metadata 已经消失
target metadata 不存在
job 仍然停在 PREPARED
reconciler 误以为可以 abort
```

## 多实例 Fencing

Production Catalog Service 必须支持多实例。

不能依赖进程内锁。

需要 durable unique record：

```text
transfer_id -> request_hash
source namespace/db/collection -> transfer_id
target namespace/db/collection name -> transfer_id
target namespace/db/alias -> transfer_id
collection_id -> owner_epoch
```

所有状态推进都必须基于 expected job version、owner epoch、metadata revision 做条件更新。

同一时刻只能有一个 worker claim 某个 job step。Stale worker 用旧 version 或旧 epoch 推进状态时必须失败。

## RootCoord RPC Contract

RootCoord transfer RPC 不能只相信调用方传来的 transfer id。

生产 RootCoord 必须校验：

- Caller service identity 被允许。
- Namespace 与本 RootCoord namespace 匹配。
- Transfer id 和 epoch 匹配 durable marker。
- 当前 phase 对当前 visibility state 合法。
- Collection id/name/alias 匹配 durable snapshot。
- Leader/session epoch 新鲜。

RootCoord 启动时必须先从 durable owner/transfer marker 恢复 transfer gate，再对外报告 healthy。

## Cache Invalidation 语义

Cache invalidation 是必须的，但不是 correctness boundary。

Source invalidation 应覆盖：

```text
collection name
collection id
aliases
DML stream mapping
```

Target invalidation 应覆盖：

```text
collection name
collection id
aliases
negative cache entries
```

如果 cache invalidation 部分失败，transfer 不应该静默成功。应该 retry、标记 degraded phase，或告警 operator。即使 Proxy cache 仍 stale，source RootCoord owner/fence check 也必须拒绝旧 owner 写入。

## Governance 和权限

Collection transfer 是高风险跨 namespace 操作，需要治理层。

最小 transfer privilege：

```text
TransferCollectionOut
TransferCollectionIn
ApproveTransferOut
ApproveTransferIn
ViewTransfer
OperateTransfer
ResolveTransfer
AdminOverrideTransfer
```

必须做双边授权：

- Source side 授权 collection release、drain、fence、deactivation。
- Target side 授权 collection acceptance、命名、quota、RBAC materialization、ownership。

同一 owner 内部迁移时，一个 principal 可以同时满足两边权限。跨 tenant、跨 org、跨 billing boundary 时，必须要求两侧独立审批。

## RBAC Transfer Policy

不能盲目复制 source namespace 的全部 RBAC。

推荐 policy：

```text
NONE
COLLECTION_GRANTS_ONLY
EXPLICIT_MAPPING
```

`NONE`：collection 到 target 后不复制 collection grant，由 target admin 重新授权。

`COLLECTION_GRANTS_ONLY`：只复制精确作用于该 collection 的 grant，并且要校验 principal 在 target namespace 存在。

`EXPLICIT_MAPPING`：使用 operator 提供的 identity/role mapping。

Namespace-level 和 global role 不应自动复制。

## 安全

生产要求：

- Admin client、Transfer Manager、Catalog Service、RootCoord 之间使用 mTLS。
- 每个组件有 service identity。
- RootCoord 只允许 trusted Catalog/Transfer Manager 调用 transfer admin RPC。
- Namespace isolation 和 policy check。
- Request signing 或 request hash，用于 idempotency 和 audit。
- Backend encryption 和 secret management。
- Rate limit 和 quota。
- Break-glass admin override 必须带原因、TTL 和审计。

mTLS 证明“谁在调用”。RBAC 和 approval 证明“为什么允许调用”。审计必须记录两者。

## 审计

Audit 应该 append-only 且可查询。

必须记录的事件：

```text
transfer requested
dry-run evaluated
policy denied
approval requested
approval approved
approval rejected
source prepared
source fenced
source drained
source snapshot validated
target preflight passed
target conflict detected
target staged hidden
source deactivated
target applied
target visible
transfer done
transfer failed
transfer paused
transfer resumed
transfer aborted
commit uncertain
operator repaired
admin override used
rbac materialized
cache invalidated
stale write rejected
```

每条事件应包含：

```text
transfer_id
operation_id
request_hash
actor user
actor service identity
approver
source namespace
target namespace
db
collection name
collection id
old state
new state
owner epoch
metadata revision
decision reason
error class
trace id
client address
timestamp
```

普通日志不等于审计。Audit 必须 durable、可查询、抗篡改。

## 可观测性

Transfer 是低频高风险操作。观测重点不是 QPS，而是 phase progress、stuck job、recoverability。

推荐 metrics：

```text
catalog_transfer_jobs_total{state}
catalog_transfer_active_jobs{state}
catalog_transfer_phase_duration_seconds{phase}
catalog_transfer_phase_failures_total{phase,error_class}
catalog_transfer_retry_total{phase,error_class}
catalog_transfer_fence_age_seconds
catalog_transfer_drain_duration_seconds
catalog_transfer_stale_write_rejected_total{side}
catalog_transfer_cache_invalidation_total{side,result}
catalog_transfer_idempotency_replay_total
catalog_transfer_approval_pending_seconds
catalog_transfer_commit_uncertain_total
catalog_transfer_backend_cas_conflict_total
catalog_transfer_admin_override_total
```

不要把 collection name 作为 Prometheus label。Transfer id 和 collection id 应进入 trace、log 和 audit event。

Tracing 应该把 transfer 当成长 workflow，而不是单个 RPC。每个 phase 都应该有 span，并串联 Catalog Service、Transfer Manager、RootCoord、backend KV、cache invalidation 路径。

## Admin API

生产 API surface 应包括：

```text
DryRunTransfer
CreateTransferRequest
ApproveTransfer
RejectTransfer
StartApprovedTransfer
GetTransfer
ListTransfers
WatchTransferEvents
PauseTransfer
ResumeTransfer
AbortTransfer
ResolveCommitUncertain
AdminOverride
```

`StartTransfer` 应该 enqueue 或提交已批准 workflow，不应该长期作为同步跑完整 workflow 的唯一执行模型。

## 业界对比

业界模式支持这种分层。

AWS Glue Data Catalog 是 centralized metadata catalog。Engine 使用 metadata API，而不是直接改底层 catalog store。

AWS Lake Formation 是 catalog 之上的 governance layer，负责权限、共享和审计。Milvus collection transfer 如果涉及跨 namespace/tenant，也需要类似的治理平面。

Apache Iceberg 强调 table metadata versioning、snapshot consistency 和 commit protocol。Milvus transfer 应借鉴 request hash、revision、epoch、CAS、idempotency、commit-uncertain 处理。

Hive Metastore 是 semantic metastore，底层可由数据库实现。Engine 调 HMS API，不直接读写 HMS backend table。生产治理通常由 Ranger、Sentry 或外部平台补齐。这说明 Catalog Service 不应该承载所有审批业务。

对应到 Milvus：

```text
Catalog Service:
  对标 Glue / HMS，提供 semantic metadata API

Transfer Manager:
  对标事务性 workflow coordinator

Governance layer:
  对标 Lake Formation / Ranger，负责 policy、authorization、audit

Owner epoch / revision / snapshot:
  借鉴 Iceberg 的事务和版本控制思想
```

关键不是让 Catalog Service 管每个 runtime worker，而是 Catalog 暴露语义化 API，runtime control plane 管自己的内部状态。

## 当前 Demo 覆盖范围

当前 demo 已验证：

- TiKV-backed RootCoord metadata。
- 两个 Milvus namespace。
- Catalog Service transfer API。
- 静态 namespace-to-RootCoord routing。
- Source prepare/drain/fence。
- Source metadata removal。
- Target metadata creation。
- Source live metadata deactivation。
- Source cache invalidation。
- Target live metadata apply。
- Target cache invalidation。
- Transfer 后 source writer 被拒绝。
- Transfer 后 target collection 和 alias 可见。

当前 demo 未验证：

- Shared object storage 下的数据可访问性。
- Target load/search/query。
- DataCoord metadata migration。
- QueryCoord load-state migration。
- Streaming/WAL barrier。
- RootCoord restart recovery。
- Catalog Service multi-instance recovery。
- Dynamic RootCoord discovery。
- mTLS/authz/audit。
- RBAC transfer policy。

## Production GA 路线图

### Phase 0: Engineering Preview

目标：证明 RootCoord metadata transfer 架构形状。

要求：

- TiKV-backed demo。
- Durable transfer job state。
- Source fence/drain。
- Source 和 target cache invalidation。
- Target live metadata apply。
- 简单 duplicate request 幂等。
- 两个 Milvus smoke test。

该阶段不声明生产可用。

### Phase 1: Controlled Alpha

目标：让 workflow 在非生产或受控环境里可控、可观测。

要求：

- Dry-run。
- Request hash 和 idempotency ledger。
- Catalog 分配 transfer epoch。
- 基础 RBAC。
- mTLS 和 service identity。
- Audit event。
- Metrics 和 tracing。
- Pause、resume、安全阶段 abort。
- Operator runbook。

范围仍限定为 drained/unloaded collection transfer。

### Phase 2: Private Beta

目标：让失败恢复和 operator control 接近生产。

要求：

- Durable owner record。
- Durable source fence。
- Target hidden state。
- Background reconciler。
- 多实例 job claim 和 fencing。
- Commit-uncertain handling。
- 双边审批。
- Quota 和 change window。
- 故障注入：Catalog Service restart、Transfer Manager restart、RootCoord leader switch、backend CAS conflict、cache invalidation failure、duplicate request、partial timeout。

只有配套 on-call 和 repair runbook 后，才允许小范围生产灰度。

### Phase 3: Production GA for Offline Metadata Transfer

目标：生产支持 drained/unloaded collection metadata transfer。

要求：

- SLO 和 alerting。
- Audit retention。
- Backup 和 restore。
- Multi-tenant isolation。
- Rate limit。
- Namespace allowlist。
- Dynamic RootCoord discovery。
- Compatibility 和 capability negotiation。
- Rolling upgrade support。
- Formal rollback/roll-forward matrix。
- 完整 security review。

GA 范围仍是 offline metadata transfer。

### Phase 4: Online Transfer and Broader Catalog GA

目标：支持 active collection transfer 和更广泛的 Milvus catalog migration。

要求：

- DML catch-up。
- WAL barrier。
- DataCoord segment/binlog/index metadata transfer。
- QueryCoord load-state 和 channel ownership transfer。
- QueryNode loaded segment handling。
- Cross-version schema/index compatibility。
- Target load/search/query 完整 e2e。

这应该是 offline metadata transfer GA 后的独立项目，不应塞进第一版 GA。

## Open Questions

- Collection id 跨 namespace 是否必须保留，还是 target 重新分配并通过 owner mapping 映射？
- Owner record 应放在 global Catalog root、tenant root，还是 per-namespace root 加 global index？
- 精确定义哪个 phase 是不可回滚点？
- Target hidden metadata 应复用现有 RootCoord collection state，还是作为独立 staging bundle？
- Collection-level RBAC principal 如何跨 namespace 映射？
- Offline metadata GA 最小需要哪些 DataCoord/QueryCoord validation？
- 第一批生产范围应限定为 same tenant、same region、same object store、unloaded collection 吗？

## GA Readiness Checklist

- Durable source fence 在 RootCoord healthy 前恢复。
- Target hidden state 防止 RootCoord restart 后提前可见。
- Source drop 和 job transition 原子化，或显式进入 commit-uncertain。
- Reconciler 可以恢复所有 non-terminal state。
- 多实例 Catalog Service 不能重复 transfer 同一 collection/name/alias。
- RootCoord transfer RPC 需要认证过的 Catalog/Transfer Manager identity。
- 所有 transfer RPC 校验 durable marker 和 owner epoch。
- Proxy stale cache 不能绕过 source owner/fence check。
- RBAC policy 显式且被审计。
- Dry-run 和 approval API 存在。
- Audit event durable 且可查询。
- Metrics、trace 和 alert 存在。
- Runbook 覆盖 failure 和 repair。
- Chaos test 覆盖 crash 和 timeout 边界。
- E2E 验证 source rejection 和 target visibility。
- 任何超出 metadata transfer 的声明，都必须 e2e 验证 target load/search/query。
