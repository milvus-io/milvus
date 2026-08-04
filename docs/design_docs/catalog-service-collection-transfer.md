# Milvus Catalog Service Collection Transfer Design

Status: design draft based on the current RootCoord metadata transfer demo and multi-agent review.

Date: 2026-08-03

## Executive Summary

Milvus Catalog Service should be the shared metadata control plane for Milvus namespaces. Collection transfer should be implemented as a control-plane workflow that changes collection ownership from one namespace to another, not as raw KV prefix copying.

The current demo has proven the basic shape:

- Two Milvus standalone processes act as two namespaces.
- Both namespaces use TiKV for RootCoord metadata, with isolated namespace roots.
- Catalog Service uses TiKV as the metadata backend.
- A transfer request moves a collection from `milvus1` to `milvus2`.
- Source RootCoord drains and fences the collection without stopping the Milvus process.
- Source cache is invalidated after deactivation.
- Target RootCoord applies the transferred metadata into live state and invalidates target cache.
- Source describe/insert fail after transfer.
- Target describe and alias resolution succeed after transfer.

This is an engineering preview, not production GA. The production design must add durable fencing, owner/visibility state, reconciler recovery, multi-instance safety, governance, audit, security, observability, and a clear offline-vs-online transfer boundary.

## Goals

The production goal is to support safe, observable, and recoverable collection metadata ownership transfer between Milvus namespaces through Catalog Service.

The first GA scope should be offline or drained collection metadata transfer:

- Source collection is drained/fenced before ownership changes.
- Target collection is not visible before activation.
- At any point, at most one namespace can accept user writes for the collection.
- Catalog metadata changes are durable and recoverable.
- Operators can inspect, pause, resume, abort where safe, or roll forward after uncertain commits.
- Permissions, approvals, and audit events are first-class.

## Non-Goals for First GA

The first GA should not claim online data-plane migration.

Out of scope for first GA:

- Active DML catch-up.
- WAL barrier promotion.
- DataCoord segment/binlog/index metadata transfer for active loaded data.
- QueryCoord load-state or replica migration.
- QueryNode loaded segment handoff.
- Cross-region object movement.
- Transparent live search/query continuity during transfer.

These should be a separate online-transfer project after offline metadata transfer is production-grade.

## Core Principles

Catalog Service should expose semantic metadata APIs, not raw KV APIs.

Open-source Milvus should keep etcd as the default implementation of the metadata interface. Cloud or managed deployments can use a shared Catalog Service backed by TiKV, Oxia, or another backend. TiKV is a backend implementation, not the product boundary.

Catalog owns persistent metadata. It should not own every Milvus runtime state.

RootCoord or a namespace-level runtime authority owns live namespace state: locks, gates, cache invalidation, in-flight operation drain, and live MetaTable mutation.

Catalog Service must not fan out directly to every Proxy, DataCoord, QueryCoord, DataNode, QueryNode, or StreamingNode. It should notify the namespace authority. That authority should perform internal fanout using Milvus runtime protocols.

## Architecture

The architecture has three logical layers.

```text
Governance / Transfer Manager
  permissions, approval, dry-run, workflow, retry, recovery, audit, operator APIs

Catalog Service
  semantic metadata APIs, durable owner state, transfer job state, CAS, backend abstraction

Milvus Namespace Runtime
  RootCoord or Namespace Control Plane Gateway, Proxy/DataCoord/QueryCoord internal fanout
```

For the current demo, Transfer Manager is implemented inside Catalog Service and RootCoord acts as the namespace authority.

Production can keep this deployment shape initially, but the logical boundary should remain explicit. Otherwise Catalog Service will grow from a stable metadata interface into an unbounded cloud management system.

## Role Boundaries

### Catalog Service

Catalog Service is the durable metadata authority.

Responsibilities:

- Store semantic metadata.
- Store transfer jobs.
- Store idempotency records.
- Allocate or validate collection owner epochs.
- Maintain owner and visibility records.
- Perform conditional metadata updates.
- Provide backend abstraction over etcd/TiKV/Oxia.
- Expose metadata APIs to Milvus and control-plane APIs to Transfer Manager.

Catalog Service should not:

- Directly invalidate Proxy caches.
- Directly drain DataNode or QueryNode.
- Directly manage QueryCoord load state.
- Directly understand every runtime node topology.
- Expose raw KV mutation as its public contract.

### Transfer Manager

Transfer Manager is the workflow coordinator.

Responsibilities:

- Validate transfer intent.
- Run dry-run checks.
- Enforce governance policy.
- Drive the transfer state machine.
- Call source and target namespace authorities.
- Retry idempotent steps.
- Detect stuck or uncertain jobs.
- Decide abort vs roll-forward based on durable facts.
- Publish events, metrics, traces, and audit records.

Transfer Manager may be embedded in Catalog Service for MVP, but should remain a distinct module and API boundary.

### RootCoord / Namespace Runtime

RootCoord is the first namespace authority for RootCoord metadata transfer.

Responsibilities:

- Freeze collection-level metadata operations.
- Drain in-flight collection operations.
- Reject source operations after transfer starts.
- Deactivate source live metadata.
- Apply target live metadata.
- Invalidate Proxy cache for collection name, collection id, and aliases.
- Restore transfer fences after restart from durable markers.

Long term, RootCoord may become part of a broader Namespace Runtime Service. That service would coordinate RootCoord, DataCoord, QueryCoord, StreamingCoord, Proxy, and worker nodes inside one namespace.

### Proxy, DataCoord, QueryCoord, Nodes

Proxy, DataCoord, QueryCoord, and nodes should not be direct Catalog Service targets.

They should receive instructions from their own namespace control plane:

- Proxy handles client request routing and local metadata cache.
- DataCoord handles segment, binlog, import, compaction, and index-related data metadata.
- QueryCoord handles load state, replicas, resource groups, targets, and shard leaders.
- Nodes execute local runtime state only.

This prevents Catalog Service from becoming a global runtime orchestrator.

## Why Catalog Service Notifies RootCoord

Collection transfer changes runtime ownership, not just durable metadata.

If Catalog Service only changes TiKV, the system can split:

```text
TiKV says collection belongs to target namespace.
Source RootCoord memory still has the collection.
Source Proxy cache may still route requests to the old owner.
Target RootCoord memory may not have loaded the collection.
```

Therefore Catalog Service must coordinate with RootCoord:

```text
Catalog Service -> source RootCoord: prepare, fence, drain
Catalog Service -> Catalog backend: move durable metadata
Catalog Service -> source RootCoord: deactivate live metadata and expire caches
Catalog Service -> target RootCoord: apply live metadata and expire caches
```

This is not intended to evolve into Catalog Service notifying every node. RootCoord is the current namespace authority for this metadata scope.

## Why Not Raw KV Copy

Raw KV copy would couple transfer correctness to internal key layout.

Problems:

- It bypasses RootCoord catalog semantics.
- It bypasses alias indexes and model validation.
- It cannot drain live operations.
- It cannot invalidate Proxy caches correctly.
- It cannot handle schema evolution cleanly.
- It makes TiKV/etcd key layout part of the API contract.

The recommended model is logical transfer:

```text
Read source collection semantic model.
Validate target conflicts.
Stage or create target semantic model.
Remove or mark source semantic model moved out.
Activate target through namespace runtime.
```

The current demo uses source drop plus target create to stay compatible with existing per-namespace RootCoordCatalog behavior. Production should evolve to owner/visibility records rather than relying only on delete/create timing.

## Namespace and RootCoord Discovery

The current demo uses static routing:

```text
milvus1 -> source RootCoord address
milvus2 -> target RootCoord address
```

This is acceptable only for demo.

Production needs dynamic namespace authority discovery:

```text
namespace -> Milvus cluster
Milvus cluster -> current RootCoord leader
RootCoord leader -> authenticated gRPC channel
```

Recommended discovery sources:

- Catalog Registry session records.
- Existing Milvus session/discovery metadata during transition.
- Kubernetes service/endpoints plus Milvus leader/session state.
- Cloud control-plane cluster registry.

RootCoord registration should carry:

- namespace
- role
- endpoint
- leader epoch
- lease/session revision
- version/capabilities
- health state

Catalog Service must reject stale leaders using fencing tokens.

## Consistency Model

The core production record should be a collection owner record.

Minimum fields:

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

Owner epoch is collection-level fencing. It is separate from Catalog Service process leadership. A Catalog Service leader epoch only proves who may advance jobs; owner epoch proves which namespace may mutate a collection.

The correctness target is single-owner visibility:

- At most one namespace can accept writes for a collection.
- Target durable metadata may exist before activation, but it must be hidden.
- Source must be fenced before source ownership is removed.
- Source drop after the point of no return must roll forward, not silently abort.
- Cache invalidation accelerates convergence but is not the correctness barrier.
- RootCoord and Proxy must enforce owner/fence state even if cache invalidation is delayed.

## Visibility States

Recommended production visibility states:

```text
ACTIVE
SOURCE_FENCED
TARGET_HIDDEN
TARGET_VISIBLE
TRANSFERRED_OUT
```

`ACTIVE` means the collection is visible and writable in the owner namespace.

`SOURCE_FENCED` means the source namespace has frozen the collection and drained in-flight operations. New user operations fail. This phase can still be aborted if persistent ownership has not crossed the point of no return.

`TARGET_HIDDEN` means the target catalog has a staged collection bundle, but users cannot see or use it.

`TARGET_VISIBLE` means target RootCoord has applied the collection and target Proxy can resolve it.

`TRANSFERRED_OUT` means source RootCoord permanently rejects the collection for that owner epoch and invalidates source caches.

## Transfer State Machine

Use separate states for workflow progress and ownership facts.

Workflow state:

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

Main path:

```text
PENDING
  -> PREPARED / SOURCE_FENCED
  -> SOURCE_DROPPED
  -> CATALOG_MOVED / TARGET_HIDDEN
  -> SOURCE_DEACTIVATED / TRANSFERRED_OUT
  -> TARGET_APPLIED / TARGET_VISIBLE
  -> DONE
```

`ABORTED` should only be allowed before the point of no return.

After source persistent metadata is removed or ownership is changed away from source, default recovery must be roll-forward.

`COMMIT_UNCERTAIN` is required for ambiguous backend commits, process crashes at commit boundaries, and cases where durable facts disagree.

## Production Workflow

Recommended production flow:

```text
1. Create transfer request.
2. Run dry-run validation.
3. Enforce source and target authorization.
4. Obtain approval if required.
5. Allocate transfer epoch and owner epoch.
6. Create durable owner/transfer marker.
7. Ask source namespace authority to prepare.
8. Source authority fences and drains collection.
9. Verify source snapshot has not changed.
10. Preflight target namespace.
11. Stage target metadata as hidden.
12. Atomically advance owner/job state past the point of no return.
13. Deactivate source live metadata and invalidate source cache.
14. Apply target live metadata.
15. Mark target visible.
16. Finalize job as done.
17. Emit audit and metrics for every phase.
```

For the current demo, steps are simplified:

```text
prepare source
drop source metadata
create target metadata
deactivate source live state
apply target live state
mark done
```

This is enough to prove the shape, but not enough for GA.

## Failure Recovery

The recovery model should be idempotent and roll-forward by default after the point of no return.

Every external action must be safe to retry:

- Prepare with the same transfer id and epoch keeps the collection fenced.
- Target staging with equivalent metadata is treated as success.
- Source deactivation with the same transfer id is treated as success.
- Target apply with equivalent live metadata is treated as success.
- Cache invalidation can be retried.

Rollback is only safe before source persistent ownership is removed.

After source drop or owner switch, recovery should roll forward unless an operator explicitly proves rollback is safe.

Required reconciler behavior:

```text
Scan non-terminal jobs.
Claim one job step using durable lease/CAS.
Read durable source facts, target facts, owner record, and runtime state.
Decide retry, advance, pause, abort, or roll forward.
Emit phase events and update retry metadata.
Alert when a job remains stuck beyond SLO.
```

Important recovery cases:

```text
PREPARED and source still fenced:
  retry preflight or abort if no durable target side effect exists.

PREPARED but source collection is gone:
  do not abort blindly; inspect source tombstone or enter COMMIT_UNCERTAIN.

SOURCE_DROPPED and target missing:
  use saved collection snapshot to create target hidden metadata.

CATALOG_MOVED and target hidden exists:
  retry source deactivate.

SOURCE_DEACTIVATED and target not visible:
  retry target apply.

TARGET_APPLIED but job not DONE:
  verify owner/visibility and finalize.
```

## Durable Atomicity Requirements

The most important GA fix is source drop atomicity.

The following must be one durable conditional transition:

```text
source persistent ownership removed or tombstoned
owner record advanced
job state advanced
source transfer marker updated
```

If the backend cannot atomically update all involved records, the workflow must explicitly model the boundary as `COMMIT_UNCERTAIN` and reconcile from facts.

The design should avoid a state where:

```text
source metadata is gone
target metadata is absent
job still says PREPARED
reconciler thinks abort is safe
```

## Multi-Instance Fencing

Production Catalog Service must support multiple instances.

Do not rely on process-local locks.

Required durable locks or unique records:

```text
transfer_id -> request_hash
source namespace/db/collection -> transfer_id
target namespace/db/collection name -> transfer_id
target namespace/db/alias -> transfer_id
collection_id -> owner_epoch
```

All state transitions should be conditional on expected job version, owner epoch, and metadata revision.

Only one worker can claim a job step at a time. A stale worker must fail when it tries to advance a state with an old version or epoch.

## RootCoord RPC Contract

RootCoord transfer RPC should not be trusted based only on caller-supplied transfer id.

Production RootCoord should verify:

- Caller service identity is allowed.
- Namespace matches the RootCoord namespace.
- Transfer id and epoch match the durable marker.
- Requested phase is legal for current visibility state.
- Collection id/name/alias match the durable snapshot.
- Leader/session epoch is fresh.

RootCoord should restore transfer gates on startup from durable owner/transfer markers before reporting healthy.

## Cache Invalidation Semantics

Cache invalidation is required, but it is not the correctness boundary.

Source invalidation should cover:

- collection name
- collection id
- aliases
- DML stream mapping

Target invalidation should cover:

- collection name
- collection id
- aliases
- negative cache entries

If invalidation partially fails, the transfer should not silently succeed. It should either retry, mark a degraded phase, or alert operators. User writes must still be rejected by source RootCoord owner/fence checks even if a stale Proxy cache remains.

## Governance and Permissions

Collection transfer is a high-risk cross-namespace operation and needs governance above the raw metadata API.

Minimum transfer privileges:

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

Dual authorization is required:

- Source side authorizes collection release, drain, fence, and deactivation.
- Target side authorizes collection acceptance, naming, quota, RBAC materialization, and ownership.

For same-owner internal moves, one principal may satisfy both sides. For cross-tenant, cross-org, or cross-billing-boundary moves, require independent approvals.

## RBAC Transfer Policy

Do not blindly copy all source namespace RBAC.

Recommended policy options:

```text
NONE
COLLECTION_GRANTS_ONLY
EXPLICIT_MAPPING
```

`NONE` means the collection arrives with no copied collection grants. Target admins must grant access.

`COLLECTION_GRANTS_ONLY` copies only grants scoped exactly to the transferred collection, after validating that principals exist in the target namespace.

`EXPLICIT_MAPPING` uses an operator-provided identity and role mapping.

Namespace-level and global roles should not be copied automatically.

## Security

Production requirements:

- mTLS between admin clients, Transfer Manager, Catalog Service, and RootCoord.
- Service identity for every component.
- RootCoord allowlist for transfer admin RPC callers.
- Namespace isolation and policy checks.
- Request signing or request hash for idempotency and audit.
- Backend encryption and secret management.
- Rate limit and quota.
- Break-glass admin override with reason, TTL, and audit.

mTLS proves who called. RBAC and approval prove why the caller is allowed to act. Audit must record both.

## Audit

Audit should be append-only and queryable.

Required events:

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

Each event should include:

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

Regular logs are not enough. Audit must be durable, queryable, and tamper-resistant.

## Observability

Transfer is low-frequency and high-risk. Observability should focus on phase progress, stuck jobs, and recoverability.

Recommended metrics:

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

Avoid collection name as a Prometheus label. Use transfer id and collection id in traces, logs, and audit events instead.

Tracing should model transfer as a long-running workflow. Each phase should create a span and propagate trace context through Catalog Service, Transfer Manager, RootCoord, backend KV, and cache invalidation paths.

## Admin APIs

Production API surface should include:

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

`StartTransfer` should enqueue or submit an approved workflow. It should not synchronously perform the full long-running transfer as the only execution model.

## Industry Comparison

The industry pattern supports this separation.

AWS Glue Data Catalog is a centralized metadata catalog. Engines use metadata APIs; they do not directly mutate the underlying catalog store.

AWS Lake Formation is a governance layer above the catalog. It handles permissions, sharing, and audit. Milvus collection transfer similarly needs policy, approval, and audit above the metadata service.

Apache Iceberg emphasizes table metadata versioning, snapshot consistency, and commit protocols. Milvus transfer should borrow the same discipline: request hash, revision, epoch, CAS, idempotency, and commit-uncertain handling.

Hive Metastore is a semantic metastore backed by a database. Engines use HMS APIs, not raw backend tables. In production, governance is often added by external policy systems. Milvus should similarly keep Catalog Service focused on semantic metadata and put transfer governance in a dedicated layer.

The key lesson is not that Catalog Service should control every runtime worker. The lesson is that catalog systems expose semantic APIs and rely on engine/runtime control planes to manage their own internal execution state.

## Current Demo Scope

The current demo validates:

- TiKV-backed RootCoord metadata.
- Two Milvus namespaces.
- Catalog Service transfer API.
- Static namespace-to-RootCoord routing.
- Source prepare/drain/fence.
- Source metadata removal.
- Target metadata creation.
- Source live metadata deactivation.
- Source cache invalidation.
- Target live metadata apply.
- Target cache invalidation.
- Source writer rejection after transfer.
- Target describe and alias visibility.

The demo does not validate:

- Shared object storage data accessibility.
- Target load/search/query.
- DataCoord metadata migration.
- QueryCoord load-state migration.
- Streaming/WAL barriers.
- RootCoord restart recovery.
- Catalog Service multi-instance recovery.
- Dynamic RootCoord discovery.
- mTLS/authz/audit.
- RBAC transfer policy.

## Production GA Roadmap

### Phase 0: Engineering Preview

Goal: prove RootCoord metadata transfer shape.

Required:

- TiKV-backed demo.
- Durable transfer job state.
- Source fence/drain.
- Source and target cache invalidation.
- Target live metadata apply.
- Idempotent retry for simple duplicate requests.
- Two-Milvus smoke test.

Not production supported.

### Phase 1: Controlled Alpha

Goal: make the workflow controlled and observable in non-production or tightly controlled environments.

Required:

- Dry-run.
- Request hash and idempotency ledger.
- Catalog-allocated transfer epoch.
- Basic RBAC.
- mTLS and service identity.
- Audit events.
- Metrics and tracing.
- Pause, resume, abort where safe.
- Operator runbook.

Scope remains drained/unloaded collection transfer.

### Phase 2: Private Beta

Goal: make failure recovery and operator control production-like.

Required:

- Durable owner record.
- Durable source fence.
- Target hidden state.
- Background reconciler.
- Multi-instance job claim and fencing.
- Commit-uncertain handling.
- Dual approval.
- Quota and change window enforcement.
- Chaos tests for Catalog Service restart, RootCoord restart, CAS conflict, cache invalidation failure, duplicate requests, and partial timeouts.

Limited production gray release can begin only with on-call and repair runbooks.

### Phase 3: Production GA for Offline Metadata Transfer

Goal: support drained/unloaded collection metadata transfer in production.

Required:

- SLO and alerting.
- Audit retention.
- Backup and restore.
- Multi-tenant isolation.
- Rate limits.
- Namespace allowlist.
- Dynamic RootCoord discovery.
- Compatibility and capability negotiation.
- Rolling upgrade support.
- Formal rollback/roll-forward matrix.
- Full security review.

GA scope remains offline metadata transfer.

### Phase 4: Online Transfer and Broader Catalog GA

Goal: support active collection transfer and broader Milvus catalog migration.

Required:

- DML catch-up.
- WAL barriers.
- DataCoord segment/binlog/index metadata transfer.
- QueryCoord load-state and channel ownership transfer.
- QueryNode loaded segment handling.
- Cross-version schema and index compatibility.
- End-to-end target load/search/query verification.

This should be treated as a separate GA project, not folded into first offline metadata transfer GA.

## Open Questions

- Should collection ids be preserved across namespaces, or should target ids be remapped with an owner mapping?
- Where should owner records live: global Catalog root, per-tenant root, or per-namespace root with global index?
- What is the exact point of no return for rollback?
- Should target hidden metadata be represented inside existing RootCoord collection state, or as a separate transfer staging bundle?
- How should collection-level RBAC principals be mapped across namespaces?
- What is the minimum required DataCoord/QueryCoord validation for offline metadata GA?
- What should be the first production customer scope: same tenant only, same region only, same object store only, unloaded collection only?

## GA Readiness Checklist

- Durable source fence restored before RootCoord becomes healthy.
- Target hidden state prevents early visibility on restart.
- Source drop and job transition are atomic or explicitly reconciled as commit-uncertain.
- Reconciler can resume every non-terminal state.
- Multi-instance Catalog Service cannot double-transfer the same collection/name/alias.
- RootCoord transfer RPCs require authenticated Catalog/Transfer Manager identity.
- All transfer RPCs validate durable markers and owner epochs.
- Proxy stale cache cannot bypass source owner/fence checks.
- RBAC policy is explicit and audited.
- Dry-run and approval APIs exist.
- Audit events are durable and queryable.
- Metrics, traces, and alerts exist.
- Runbooks cover failure and repair.
- Chaos tests cover crash and timeout boundaries.
- E2E verifies source rejection and target visibility.
- For any claim beyond metadata transfer, E2E verifies target load/search/query.
