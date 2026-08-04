# Milvus Catalog Service Production Redesign Plan

状态：production 级重新设计与落地规划。

日期：2026-08-04

## 结论

当前 demo 证明了一个方向：Catalog Service 可以编排 RootCoord，把 collection metadata ownership 从一个 namespace 转到另一个 namespace。但 demo 的实现方式不能直接推进到 production。

Production 设计必须从“同步 RPC + source delete + target create”升级为：

```text
Governance / Transfer Manager
  -> Catalog Service durable owner/job state
  -> Namespace Runtime authority
      -> RootCoord / Proxy / DataCoord / QueryCoord / Streaming internal fanout
```

核心改造目标：

```text
owner record
durable source fence
target hidden state
reconciler
proxy fail-closed
multi-instance fencing
mTLS / authz / audit
dry-run / approval / repair
```

没有这些，不能对外宣称 production、GA、online transfer 或自助 transfer。

## Production Scope

第一版 production GA 只支持 offline metadata transfer。

允许范围：

- Same tenant。
- Same region。
- Same object store。
- Drained or unloaded collection。
- RootCoord metadata ownership transfer。
- Operator-driven transfer。
- Target 可见 metadata，但不承诺自动 load/search/query 连续性。

明确不承诺：

- Active DML catch-up。
- Search/query continuous availability。
- DataCoord segment/binlog/index ownership handoff。
- QueryCoord load-state handoff。
- Streaming/WAL barrier。
- RBAC 自动全量继承。
- Cross-tenant self-service transfer。

## Product Boundary

Catalog Service 不是用户直接调用的“搬表 API”。

生产边界应为：

```text
User / Operator
  -> Governance API / Transfer Manager
      -> Catalog Service
      -> Namespace Runtime
```

用户或运维不应该直接调用裸 `StartCollectionTransfer`。生产 API 应拆成：

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

Catalog Service 应保持为 metadata 和 durable state authority；Transfer Manager 才是 workflow coordinator；Governance layer 负责权限、审批、审计和 operator policy。

## Target Architecture

### Control Plane Layers

```text
Governance Layer
  RBAC, approval, policy, audit, tenant boundary, risk control

Transfer Manager
  workflow, reconcile, retry, rollback/roll-forward, operator action

Catalog Service
  semantic metadata API, owner record, job state, CAS, backend adapter

Namespace Runtime Service
  per-namespace authority, RootCoord/Proxy/DataCoord/QueryCoord internal fanout
```

当前可以先让 Transfer Manager 和 Catalog Service 同进程部署，但代码和 API 必须分层。

### Namespace Runtime

Catalog Service 不应该继续扩展成直接通知每个 coord/node。

正确模式：

```text
Catalog / Transfer Manager
  -> source Namespace Runtime PrepareTransfer
  -> target Namespace Runtime Stage/ApplyTransfer

Namespace Runtime internally:
  RootCoord fence/apply/deactivate
  Proxy owner_epoch block and cache eviction
  DataCoord validation or barrier
  QueryCoord unloaded/load-state validation
  Streaming/WAL validation where needed
```

第一阶段 Namespace Runtime 可以由 RootCoord 实现。后续再抽象成独立 gateway。

## Core Data Model

### Owner Record

每个 transferable collection 必须有 durable owner record。

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

owner_epoch 是 collection 级 fencing token。它不能由 CLI 传入，必须由 Catalog/Transfer Manager 分配。

### Visibility State

```text
ACTIVE
SOURCE_FENCED
TARGET_HIDDEN
TARGET_VISIBLE
TRANSFERRED_OUT
```

规则：

- `ACTIVE`: owner namespace 可读写。
- `SOURCE_FENCED`: source 已冻结，新的用户操作失败。
- `TARGET_HIDDEN`: target durable metadata 已 staged，但用户不可见。
- `TARGET_VISIBLE`: target live metadata 已 apply，target 可见。
- `TRANSFERRED_OUT`: source 永久拒绝该 collection。

### Transfer Job

job state 描述 workflow，不等同于 visibility。

```text
PENDING
VALIDATED
APPROVED
PREPARED
SOURCE_DROPPED
TARGET_STAGED
SOURCE_DEACTIVATED
TARGET_APPLIED
DONE
ABORTED
FAILED_RETRYABLE
COMMIT_UNCERTAIN
PAUSED_OPERATOR_REQUIRED
```

必须支持 append-only phase event，不能只覆盖最终状态。

### Durable Unique Records

防止并发 transfer：

```text
transfer_id -> request_hash
source_namespace/db_id/collection_id -> transfer_id
target_namespace/db_id/collection_name -> transfer_id
target_namespace/db_id/alias -> transfer_id
collection_id -> owner_epoch
```

所有状态推进必须带 expected version / revision / owner_epoch。

## Production Workflow

### Main Path

```text
1. User creates transfer request.
2. Governance runs authorization and policy precheck.
3. Dry-run validates source, target, DB, alias, quota, version, collection state.
4. Source and target approvals are recorded.
5. Transfer Manager allocates transfer_epoch and owner_epoch.
6. Catalog creates durable owner/transfer marker.
7. Source Namespace Runtime prepares transfer.
8. Source Runtime blocks Proxy DML/DQL and drains RootCoord in-flight operations.
9. Transfer Manager verifies source snapshot and metadata revision.
10. Target metadata is staged as TARGET_HIDDEN.
11. Source ownership is tombstoned or switched with job state in one conditional transition.
12. Source Runtime deactivates live metadata and evicts caches.
13. Target Runtime verifies hidden bundle and applies live metadata.
14. Catalog marks target visible and source transferred-out.
15. Transfer Manager finalizes job as DONE.
16. Audit and metrics are emitted for each phase.
```

### Correctness Rules

```text
At most one namespace can write the collection.
Target hidden metadata cannot be user visible.
Source drop after point-of-no-return must roll forward.
Cache invalidation is not the correctness barrier.
Proxy and RootCoord must fail-closed on stale owner_epoch.
RootCoord must restore durable fence before healthy.
```

## Reconciler Design

Production must have a background reconciler.

Responsibilities:

```text
scan non-terminal jobs
claim job step with durable lease/CAS
read owner record, source catalog, target catalog, runtime state
decide retry / advance / pause / roll-forward / abort
emit phase event
alert stuck job
```

Recovery examples:

```text
PREPARED + source still fenced + no target side effect:
  retry preflight or abort safely

PREPARED + source metadata gone:
  enter COMMIT_UNCERTAIN, inspect tombstone, then roll forward

SOURCE_DROPPED + target missing:
  use saved snapshot to stage target hidden

TARGET_STAGED + source still active:
  retry source deactivate; do not make target visible

SOURCE_DEACTIVATED + target not visible:
  retry target apply

TARGET_APPLIED + job not DONE:
  verify owner/visibility and finalize
```

## Runtime Fence Design

### Source Runtime

Source must fail-closed at multiple layers:

```text
RootCoord DDL/metadata read gate
Proxy DML/DQL request gate
Proxy meta cache owner_epoch validation
DataCoord write/flush/import/compaction admission validation
QueryCoord load/search/query metadata validation
```

For offline GA, QueryCoord/DataCoord can reject transfer unless collection is unloaded/drained. But the rejection must be explicit and validated.

### Proxy Fence

Proxy must not rely only on cache invalidation.

Needed:

```text
owner_epoch in cached collection meta
transfer block table keyed by collection_id/name/alias
fail-closed if owner_epoch stale or collection is transferring
evict DML stream mapping on source transferred-out
evict negative cache on target apply
ack or observable result for transfer cache command
```

### RootCoord Restart

RootCoord startup sequence must be:

```text
load durable transfer/owner markers
restore transfer gates
load visible collections only
filter TARGET_HIDDEN
mark healthy
```

## Security and Governance

### Authentication

Required:

```text
mTLS between admin client, Transfer Manager, Catalog Service, Namespace Runtime
service identity for each component
RootCoord allowlist for Transfer Manager identity
leader/session epoch in Runtime RPC
```

### Authorization

Required privileges:

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

Transfer requires source and target authorization:

```text
source approves release/fence/deactivate
target approves receive/name/quota/RBAC materialization
```

### RBAC Policy

Default policy:

```text
NONE
```

Supported policies:

```text
NONE
COLLECTION_GRANTS_ONLY
EXPLICIT_MAPPING
```

Namespace-level and global grants must not be automatically copied.

### Audit

Audit must be append-only.

Required event fields:

```text
transfer_id
operation_id
request_hash
actor_user
actor_service_identity
approver
source_namespace
target_namespace
db
collection_name
collection_id
old_state
new_state
owner_epoch
metadata_revision
decision_reason
error_class
trace_id
client_address
timestamp
```

Ordinary logs are not audit.

## Operational Design

### Deployment

Production deployment needs:

```text
Helm/K8s manifests
PDB
HPA
resource requests/limits
readiness/liveness probes
network policy
certificate/secret integration
backend connectivity health checks
```

### Observability

Metrics:

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

Alerts:

```text
job stuck beyond SLO
fence age too high
commit uncertain exists
reconciler stopped
cache invalidation failure
CAS conflict spike
unauthorized transfer RPC
admin override used
```

### Runbook

Must document:

```text
how to dry-run
how to approve/reject
how to inspect source fenced
how to inspect target hidden/visible
how to resolve commit uncertain
when abort is safe
when only roll-forward is allowed
how to repair stuck job
how to restore after backup
```

## Backend Requirements

Backend abstraction must support:

```text
conditional put
conditional update
multi-key CAS for owner/job/marker
idempotency record
list non-terminal jobs
list owner records by namespace
append-only audit events
txn size limits and latency SLO
```

TiKV and etcd implementations need conformance tests for:

```text
CAS conflict
commit uncertain
large collection metadata
many aliases
many partitions
concurrent transfer locks
namespace path isolation
```

## Testing and Verification

### Unit Tests

Required:

```text
owner record CAS
visibility transitions
request hash idempotency
target hidden filtering
source tombstone recovery
multi-transfer unique locks
transfer_id / namespace validation
RBAC policy validation
audit event append
```

### Integration Tests

Required:

```text
source RootCoord restart after prepare
target RootCoord restart after target hidden
Catalog Service crash after source drop
Catalog Service crash after target staged
duplicate StartApprovedTransfer
two different transfer_id for same source collection
target alias conflict
DropDatabase interleaving
stale Proxy cache insert/search/query
cache invalidation partial failure
```

### E2E Tests

Offline GA E2E must prove:

```text
source DML/DQL fail after prepare/fence
source describe/list/alias fail after transferred-out
target hidden is not visible before apply
target visible after apply
reconciler recovers every non-terminal state
restart/failover preserves fence
unauthorized transfer is rejected
audit and metrics are emitted
```

Any online transfer claim must additionally prove:

```text
target load
target search/query
DataCoord metadata consistency
QueryCoord load-state consistency
WAL/DML catch-up
```

## Implementation Roadmap

### Milestone 0: Freeze Current Demo Scope

Output:

```text
label current implementation Engineering Preview
document non-production limitations
keep TiKV demo and e2e
do not expose as public API
```

Exit criteria:

```text
demo passes
design doc published
known P0 risks tracked
```

### Milestone 1: Durable Owner and Visibility Core

Build:

```text
owner record
visibility state
target hidden staging
source tombstone
request hash
server-allocated transfer_epoch / owner_epoch
state machine with COMMIT_UNCERTAIN
```

Exit criteria:

```text
source drop cannot leave PREPARED job with missing metadata
target hidden never user visible
same transfer request is idempotent
conflicting transfer request is rejected
```

### Milestone 2: Reconciler and Multi-Instance Safety

Build:

```text
non-terminal job listing
durable job step claim
retry/backoff
stuck detection
multi-CS CAS fencing
owner/name/alias unique locks
operator status events
```

Exit criteria:

```text
CS crash at every phase recovers
two CS instances cannot double-transfer
stuck jobs alert
operator can inspect next action
```

### Milestone 3: Runtime Fail-Closed

Build:

```text
RootCoord durable fence restore
Proxy owner_epoch validation
Proxy transfer block/evict command
DataCoord/QueryCoord offline validation gates
DropDatabase/DDL transfer reference checks
```

Exit criteria:

```text
source stale Proxy insert/search/query fail
RootCoord restart preserves fence
target restart does not expose hidden collection
offline collection transfer remains unloaded unless explicitly loaded
```

### Milestone 4: Governance and Security

Build:

```text
mTLS
service identity
source/target transfer privileges
dry-run
approval workflow
RBAC transfer policy
append-only audit
admin override with TTL/reason
```

Exit criteria:

```text
unauthorized CatalogService request rejected
unauthorized RootCoord transfer RPC rejected
source and target approvals enforced
audit event query proves full transfer chain
```

### Milestone 5: Production Operations

Build:

```text
K8s deployment assets
health checks
metrics/traces/alerts
runbook
backup/restore guidance
chaos tests
capacity benchmark
TiKV/etcd conformance tests
```

Exit criteria:

```text
HA deployment survives instance restart
operator can repair commit uncertain
alerts fire for stuck/fenced jobs
capacity baseline exists
```

### Milestone 6: Offline Metadata Transfer GA

Scope:

```text
same tenant
same region
same object store
drained/unloaded collection
metadata ownership transfer
operator-driven workflow
```

Exit criteria:

```text
all P0/P1 risks closed
security review passed
chaos suite passed
runbook approved
SLO and alerting defined
docs clearly say not online transfer
```

## Explicit Non-GA Items

Do not include in first GA:

```text
online DML transfer
cross-tenant self-service
automatic global RBAC copy
QueryCoord load-state handoff
DataCoord active segment ownership handoff
Streaming/WAL ownership handoff
transparent search/query continuity
```

These should be planned as the next major project after offline metadata transfer GA.

## Boss-Level Summary

Production 方案不是把当前 demo 补几个 if 判断后上线。需要把 transfer 从“同步搬 metadata”升级为“可治理、可恢复、带 owner epoch 的跨 namespace ownership workflow”。

正确的生产边界是：

```text
Catalog Service 管 durable metadata 和 owner state
Transfer Manager 管 workflow/recovery
Governance 管权限审批审计
Namespace Runtime 管本集群 runtime fence/cache/barrier
```

第一版 GA 应只承诺 offline metadata transfer。Online transfer 和数据面 handoff 必须另立项目。
