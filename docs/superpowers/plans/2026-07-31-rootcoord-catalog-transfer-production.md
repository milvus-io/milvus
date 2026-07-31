# RootCoord Catalog Transfer Production Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-shaped RootCoord collection metadata transfer path between Milvus namespaces/clusters, backed by Catalog Service durable workflow state, source fencing/drain, target live `MetaTable` apply, Proxy cache invalidation, idempotency, and operational validation.

**Architecture:** Transfer Manager is the orchestrator and never mutates RootCoord KV directly. Source and target RootCoord own the live transfer protocol through internal/admin RPCs. Catalog Service owns durable semantic metadata, transfer job state, revision/CAS, owner epoch, and idempotency; OSS etcd remains the default local implementation.

**Tech Stack:** Go, protobuf/gRPC, Milvus RootCoord, Milvus Catalog SDK, milvus-catalog routing, etcd/TiKV metastore adapters, Prometheus metrics, Milvus `mlog`, Go tests with `-tags dynamic,test -gcflags="all=-N -l"`.

---

## Scope

Milestone 1 implements production-grade **RootCoord collection metadata transfer for drained/unloaded collections**.

In scope:
- Durable transfer job state and idempotency.
- Collection owner epoch and transfer epoch persisted in Catalog Service.
- Source RootCoord prepare/fence/drain/export/deactivate.
- Target RootCoord prepare/stage/activate/live apply/cache invalidate.
- Target Proxy lazy reload after RootCoord live apply.
- Source stale writes rejected even if Proxy cache invalidation is missed.
- E2E smoke with two Milvus instances and one routed Catalog Service.
- Unit/integration/fault tests for retry and crash recovery.

Out of scope for Milestone 1:
- Active DML catch-up.
- DataCoord segment/binlog/index migration.
- QueryCoord load-state migration.
- Streaming/WAL barrier promotion for live traffic.

Those are Phase 2 because RootCoord metadata alone cannot prove data-plane correctness.

## Worktrees

- Milvus: `/home/shaoting/workspace/milvus/.worktrees/rootcoord-catalog-production-transfer`
- milvus-catalog: `/home/shaoting/.config/superpowers/worktrees/milvus-catalog/rootcoord-catalog-production-transfer`

## File Structure

Milvus repo:
- Modify `pkg/proto/root_coord.proto`: add RootCoord transfer admin RPCs and messages.
- Modify `internal/rootcoord/root_coord.go`: wire RootCoord transfer RPC handlers.
- Create `internal/rootcoord/catalog_transfer.go`: source/target transfer handler implementation.
- Create `internal/rootcoord/catalog_transfer_state.go`: in-process transfer gates and validation helpers.
- Modify `internal/rootcoord/meta_table.go`: add live transfer apply/deactivate methods under `ddLock`.
- Modify `internal/rootcoord/ddl_callbacks.go` and related callback files only if a first-class transfer broadcast message is needed in Milestone 1.
- Modify `internal/rootcoord/expire_cache.go`: add transfer-aware invalidation helper for collection real name, ID, aliases, and RBAC refresh.
- Modify `internal/proxy/meta_cache.go`: add owner epoch awareness only if source stale-write tests prove Proxy-side epoch checks are required for Milestone 1.
- Modify `internal/proxy/task_insert.go`, `internal/proxy/task_query.go`, `internal/proxy/task_search.go`: add transfer fence checks only after tests define the expected behavior.
- Create tests under `internal/rootcoord/catalog_transfer*_test.go` and targeted Proxy tests.

milvus-catalog repo:
- Create `transfer/`: durable transfer job model and state machine.
- Create `idempotency/`: operation idempotency ledger.
- Create `ownership/`: collection owner epoch/revision records.
- Modify `client/router.go` only for production-required typed request envelope support.
- Add proto/service files in the repo location used by the existing generated `catalogpb` workflow once confirmed.
- Add tests under `transfer/`, `ownership/`, `idempotency/`, and routing integration tests.

## Milestone Workflow

### Task 1: Baseline And Interface Inventory

**Files:**
- Read: `internal/metastore/rootcoord_catalog.go`
- Read: `internal/rootcoord/meta_table.go`
- Read: `internal/rootcoord/expire_cache.go`
- Read: `internal/proxy/meta_cache.go`
- Read: `client/router.go` in milvus-catalog
- Create: `docs/superpowers/plans/rootcoord-catalog-transfer-inventory.md`

- [ ] **Step 1: Run current targeted Milvus baseline**

Run:

```bash
cd /home/shaoting/workspace/milvus/.worktrees/rootcoord-catalog-production-transfer
go test -tags dynamic,test -gcflags="all=-N -l" ./internal/rootcoord -run 'TestMetaTable|TestDDLCallbacks|TestExpire' -count=1
```

Expected: existing tests pass or failures are recorded as baseline.

- [ ] **Step 2: Run current milvus-catalog baseline**

Run:

```bash
cd /home/shaoting/.config/superpowers/worktrees/milvus-catalog/rootcoord-catalog-production-transfer
go test ./...
```

Expected: routing/client/migration tests pass.

- [ ] **Step 3: Write inventory doc**

Write `docs/superpowers/plans/rootcoord-catalog-transfer-inventory.md` with exact current entry points:

```markdown
# RootCoord Catalog Transfer Inventory

## RootCoord Live State
- MetaTable owns DB, collection, partition, alias, file-resource cache and locks.
- Direct catalog/KV writes are invisible until reload.

## Proxy Cache
- RootCoord invalidates through ExpireMetaCache.
- Proxy lazy reloads through DescribeCollection and GetShardLeaders.
- Cache invalidation is not a correctness fence.

## Catalog Routing
- Catalog route term fences catalog-service node ownership only.
- Collection transfer requires separate owner epoch.
```

### Task 2: Catalog Transfer Job Model

**Files:**
- Create: `transfer/job.go`
- Create: `transfer/job_test.go`

- [ ] **Step 1: Write failing state-machine tests**

Create tests for:
- duplicate `transfer_id` with same request hash returns existing job
- duplicate `transfer_id` with different request hash returns conflict
- only legal state transitions are allowed
- terminal states cannot advance

Run:

```bash
cd /home/shaoting/.config/superpowers/worktrees/milvus-catalog/rootcoord-catalog-production-transfer
go test ./transfer -run TestTransferJob -count=1
```

Expected: fail because package does not exist.

- [ ] **Step 2: Implement minimal job model**

Define:

```go
type State string

const (
	StateRequested              State = "REQUESTED"
	StateValidating             State = "VALIDATING"
	StateSourceFencing          State = "SOURCE_FENCING"
	StateSourceDraining         State = "SOURCE_DRAINING"
	StateSnapshotExporting      State = "SNAPSHOT_EXPORTING"
	StateTargetReserving        State = "TARGET_RESERVING"
	StateTargetStaging          State = "TARGET_STAGING"
	StateVerifying              State = "VERIFYING"
	StateTargetActivating       State = "TARGET_ACTIVATING"
	StateSourceDeactivating     State = "SOURCE_DEACTIVATING"
	StateCommitted              State = "COMMITTED"
	StateAborted                State = "ABORTED"
	StateFailedRetryable        State = "FAILED_RETRYABLE"
	StatePausedOperatorRequired State = "PAUSED_OPERATOR_REQUIRED"
	StateCommitUncertain        State = "COMMIT_UNCERTAIN"
)

type Job struct {
	TransferID        string
	RequestHash       string
	SourceNamespace   string
	TargetNamespace   string
	DBID              int64
	DBName            string
	CollectionID      int64
	CollectionName    string
	TransferEpoch     int64
	SourceRevision    int64
	TargetRevision    int64
	State             State
	Version           int64
	LastError         string
	CompletedSteps    []string
}
```

- [ ] **Step 3: Verify tests pass**

Run:

```bash
go test ./transfer -count=1
```

Expected: PASS.

### Task 3: Catalog Idempotency Ledger

**Files:**
- Create: `idempotency/ledger.go`
- Create: `idempotency/ledger_test.go`

- [ ] **Step 1: Write failing tests**

Cover:
- same `(namespace, operation_id)` and same payload hash returns saved result
- same key and different payload hash returns conflict
- terminal failed operation can be replayed as same failure

Run:

```bash
cd /home/shaoting/.config/superpowers/worktrees/milvus-catalog/rootcoord-catalog-production-transfer
go test ./idempotency -count=1
```

Expected: fail because package does not exist.

- [ ] **Step 2: Implement in-memory interface and model**

Create a backend-neutral interface first:

```go
type Record struct {
	Namespace    string
	OperationID  string
	PayloadHash  string
	ResultHash   string
	StatusCode   string
	ErrorMessage string
}

type Ledger interface {
	Begin(ctx context.Context, rec Record) (Record, bool, error)
	Complete(ctx context.Context, namespace, operationID, resultHash, statusCode, errorMessage string) error
}
```

Implement an in-memory implementation for conformance tests. Add persistent backend in later task.

### Task 4: Collection Ownership Epoch

**Files:**
- Create: `ownership/collection_owner.go`
- Create: `ownership/collection_owner_test.go`

- [ ] **Step 1: Write failing CAS tests**

Cover:
- active owner can be frozen by expected revision
- stale expected revision is rejected
- source mutation after frozen epoch is rejected
- target activation requires expected transfer epoch

Run:

```bash
cd /home/shaoting/.config/superpowers/worktrees/milvus-catalog/rootcoord-catalog-production-transfer
go test ./ownership -count=1
```

Expected: fail because package does not exist.

- [ ] **Step 2: Implement ownership model**

Define:

```go
type CollectionState string

const (
	CollectionActive          CollectionState = "ACTIVE"
	CollectionTransferringOut CollectionState = "TRANSFERRING_OUT"
	CollectionTransferringIn  CollectionState = "TRANSFERRING_IN"
	CollectionTransferredOut  CollectionState = "TRANSFERRED_OUT"
)

type CollectionOwner struct {
	Namespace    string
	CollectionID int64
	OwnerCluster string
	OwnerEpoch   int64
	TransferID   string
	State         CollectionState
	Revision      int64
}
```

Expose `CanMutate(ctx MutationContext, owner CollectionOwner) error`.

### Task 5: Catalog Service Transfer Store

**Files:**
- Create: `transfer/store.go`
- Create: `transfer/store_test.go`

- [ ] **Step 1: Write failing store conformance tests**

Cover:
- `CreateJob`
- `GetJob`
- `ListJobs`
- `CompareAndSwapJob`
- source/target index uniqueness

Run:

```bash
cd /home/shaoting/.config/superpowers/worktrees/milvus-catalog/rootcoord-catalog-production-transfer
go test ./transfer -run TestStore -count=1
```

Expected: fail until store exists.

- [ ] **Step 2: Implement in-memory store**

Implement the interface:

```go
type Store interface {
	CreateJob(ctx context.Context, job *Job) (*Job, error)
	GetJob(ctx context.Context, transferID string) (*Job, error)
	ListJobs(ctx context.Context, states ...State) ([]*Job, error)
	CompareAndSwapJob(ctx context.Context, transferID string, fromVersion int64, next *Job) error
}
```

Persistent etcd/TiKV-backed store follows after API stabilizes.

### Task 6: RootCoord Transfer Proto

**Files:**
- Modify: `pkg/proto/root_coord.proto`
- Generated: Go proto files through repository generator
- Test: compile-only proto generation check

- [ ] **Step 1: Add compile guard test or generation check**

Run before modification:

```bash
cd /home/shaoting/workspace/milvus/.worktrees/rootcoord-catalog-production-transfer
make generated-proto-go
```

Expected: baseline generation succeeds or generator prerequisite failure is recorded.

- [ ] **Step 2: Add RootCoord admin RPCs**

Add messages and service methods for:
- `PrepareCollectionTransferOut`
- `DrainCollectionTransferOut`
- `ExportCollectionTransferSnapshot`
- `DeactivateTransferredCollection`
- `AbortCollectionTransferOut`
- `PrepareCollectionTransferIn`
- `StageCollectionTransferBundle`
- `VerifyCollectionTransferBundle`
- `ActivateTransferredCollection`
- `AbortCollectionTransferIn`
- `GetCollectionTransferStatus`

The request messages must include `transfer_id`, `operation_id`, `source_namespace`, `target_namespace`, `db_name`, `collection_name`, `collection_id`, `transfer_epoch`, and `expected_revision`.

### Task 7: RootCoord In-Process Transfer Gate

**Files:**
- Create: `internal/rootcoord/catalog_transfer_state.go`
- Create: `internal/rootcoord/catalog_transfer_state_test.go`

- [ ] **Step 1: Write failing gate tests**

Cover:
- ACTIVE collection accepts normal operations
- TRANSFERRING_OUT rejects source create/alter/drop/insert/search/load gate checks
- stale epoch rejects
- matching transfer epoch allows internal transfer operation

Run:

```bash
cd /home/shaoting/workspace/milvus/.worktrees/rootcoord-catalog-production-transfer
go test -tags dynamic,test -gcflags="all=-N -l" ./internal/rootcoord -run TestTransferGate -count=1
```

Expected: fail because gate does not exist.

- [ ] **Step 2: Implement gate**

Add a narrow internal type:

```go
type transferGate struct {
	mu sync.RWMutex
	collections map[int64]transferGateEntry
}

type transferGateEntry struct {
	transferID string
	epoch int64
	state string
}
```

Expose:
- `Freeze(collectionID int64, transferID string, epoch int64) error`
- `FreezeWithDrain(collectionID int64, transferID string, epoch int64, timeout time.Duration) error`
- `BeginUserOperation(collectionID int64, epoch int64) (done func(), err error)`
- `AllowUserOperation(collectionID int64, epoch int64) error`
- `AllowTransferOperation(collectionID int64, transferID string, epoch int64, expected transferCollectionState) error`
- `Deactivate(collectionID int64, transferID string, epoch int64) error`
- `Abort(collectionID int64, transferID string, epoch int64) error`
- `Restore(collectionID int64, entry transferGateEntry)`

### Task 8: RootCoord Live Target Apply

**Files:**
- Modify: `internal/rootcoord/meta_table.go`
- Create: `internal/rootcoord/catalog_transfer_meta_test.go`

- [ ] **Step 1: Write failing live apply tests**

Cover:
- applying a transferred collection inserts into `collID2Meta`
- names and aliases resolve after apply
- partition name index is built
- file-resource refcount increments
- applying duplicate transfer is idempotent
- apply does not require `reload()`

Run:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" ./internal/rootcoord -run TestMetaTableTransferApply -count=1
```

Expected: fail until methods exist.

- [ ] **Step 2: Implement `ApplyTransferredCollection`**

Add an `IMetaTable` method only if tests require mocking. Otherwise keep method concrete on `*MetaTable` first to limit interface blast radius.

Implementation must acquire `ddLock`, update:
- `collID2Meta`
- `names`
- `aliases`
- `partitionName2ID`
- `fileResourceRefCnt`
- `generalCnt`
- RootCoord collection/partition metrics
- `channel.StaticPChannelStatsManager`

### Task 9: Transfer-Aware Cache Invalidation

**Files:**
- Modify: `internal/rootcoord/expire_cache.go`
- Create: `internal/rootcoord/catalog_transfer_cache_test.go`
- Modify tests in `internal/proxy` if required

- [ ] **Step 1: Write failing invalidation tests**

Cover:
- source invalidation sends real collection name and collection ID
- source invalidation also invalidates aliases
- target invalidation clears negative cache by name
- RBAC refresh is called when collection grants are materialized

Run:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" ./internal/rootcoord -run TestTransferCacheInvalidation -count=1
```

Expected: fail until helper exists.

- [ ] **Step 2: Implement helper**

Add:

```go
func (c *Core) ExpireTransferCollectionCaches(ctx context.Context, dbName string, coll *model.Collection, aliases []string, ts typeutil.Timestamp, msgType commonpb.MsgType) error
```

It should call existing `ExpireMetaCache` for real name and every alias, with collection ID when known.

### Task 10: RootCoord Transfer RPC Handlers

**Files:**
- Create: `internal/rootcoord/catalog_transfer.go`
- Modify: `internal/rootcoord/root_coord.go`
- Create: `internal/rootcoord/catalog_transfer_rpc_test.go`

- [ ] **Step 1: Write failing RPC tests**

Cover:
- prepare out freezes source and returns transfer epoch
- drain out reports in-flight zero for Milestone 1 gate
- export snapshot returns semantic bundle
- prepare in reserves target state but does not expose to normal describe
- activate in live applies and invalidates caches
- deactivate out marks source transferred out and invalidates caches

Run:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" ./internal/rootcoord -run TestCatalogTransferRPC -count=1
```

Expected: fail until handlers exist.

- [ ] **Step 2: Implement handlers**

The implementation must not call KV directly. It must use RootCoord `meta`, allocators, broker, proxy manager, and Catalog SDK abstractions.

### Task 11: Catalog Service Remote RootCoordCatalog v2

**Files:**
- Create or modify catalog proto package after locating current generation flow
- Create: server package for transfer job endpoints
- Create: client package wrapping routing client
- Add conformance tests

- [ ] **Step 1: Write conformance tests**

The same tests must run against:
- local in-memory store
- routed service store

Cover create/get/CAS/idempotency/owner epoch.

- [ ] **Step 2: Implement service and client**

Use existing `routing.Coordinator`, `client.Router`, and `nsmeta` only for catalog-node routing. Use typed proto fields for transfer epoch and operation IDs.

### Task 12: Transfer Manager Workflow

**Files:**
- Create: `cmd/catalog-transfer-manager`
- Create: `internal/catalogtransfer/workflow.go`
- Create: `internal/catalogtransfer/workflow_test.go`

- [ ] **Step 1: Write failing workflow tests**

Use fake source RootCoord, fake target RootCoord, and fake Catalog transfer store.

Cover:
- happy path reaches COMMITTED
- retry after crash resumes from last completed phase
- abort before target live releases source fence
- commit uncertain requires operator resolution

- [ ] **Step 2: Implement workflow**

Workflow sequence:

```text
CreateTransferJob
PrepareCollectionTransferOut
DrainCollectionTransferOut
ExportCollectionTransferSnapshot
PrepareCollectionTransferIn
StageCollectionTransferBundle
VerifyCollectionTransferBundle
ActivateTransferredCollection
DeactivateTransferredCollection
FinalizeTransferJob
```

### Task 13: Two-Milvus E2E

**Files:**
- Create: `tests/integration/catalog_transfer/`
- Create: docker compose override files only under test directory
- Create: Python smoke or Go integration test

- [ ] **Step 1: Write E2E smoke**

Test:
- Milvus1 create collection and insert data if unloaded/drained scope allows.
- Milvus1 drain/fence transfer.
- Transfer selected collections to Milvus2 namespace.
- Milvus1 write/read rejected after fence.
- Milvus2 `DescribeCollection` succeeds without restart.
- Milvus2 Proxy lazy cache reload works.

- [ ] **Step 2: Run E2E**

Run:

```bash
cd /home/shaoting/workspace/milvus/.worktrees/rootcoord-catalog-production-transfer
pytest tests/integration/catalog_transfer -q
```

Expected: PASS.

### Task 14: Operational Readiness

**Files:**
- Modify metrics definitions where Milvus keeps RootCoord metrics
- Add catalog service metrics
- Create: `docs/catalog_transfer_runbook.md`

- [ ] **Step 1: Add metrics tests**

Metrics:
- `catalog_transfer_jobs_total{state}`
- `catalog_transfer_phase_duration_seconds{phase}`
- `catalog_transfer_retry_total{phase,error}`
- `catalog_transfer_fence_age_seconds`
- `catalog_transfer_cache_invalidation_total{side,result}`
- `catalog_transfer_idempotency_replay_total`

- [ ] **Step 2: Add runbook**

Runbook must include:
- start transfer
- inspect state
- pause/resume
- abort before target live
- resolve commit uncertain
- verify target live
- verify source fenced
- rollback limitations

## Final Verification

Run:

```bash
cd /home/shaoting/.config/superpowers/worktrees/milvus-catalog/rootcoord-catalog-production-transfer
go test ./...

cd /home/shaoting/workspace/milvus/.worktrees/rootcoord-catalog-production-transfer
go test -tags dynamic,test -gcflags="all=-N -l" ./internal/rootcoord ./internal/proxy
pytest tests/integration/catalog_transfer -q
```

Expected:
- all unit tests pass
- E2E two-Milvus smoke passes
- no target restart in transfer path
- no direct RootCoord KV mutation in Transfer Manager

## Self-Review

Spec coverage:
- Catalog Service included.
- RootCoord live state included.
- Source fence/drain included.
- Target live cache apply included.
- Proxy invalidate/lazy reload included.
- RBAC default policy included through cache invalidation task and workflow scope.
- Data-plane online transfer explicitly out of Milestone 1.

Placeholder scan:
- No `TBD` markers.
- No task says "implement later".
- Each task has concrete files and verification commands.

Type consistency:
- Transfer job uses `transfer_id`, `operation_id`, `transfer_epoch`, `expected_revision` consistently.
- Collection owner epoch is separate from catalog-service routing term.
