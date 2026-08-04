# Catalog Service Production Transfer Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把当前 RootCoord Catalog Service collection transfer POC 改造成生产级、可恢复、可运维的 offline metadata transfer 能力。

**Architecture:** Catalog Service 只做语义 metadata 和 durable state authority，不退化为 raw KV API。Transfer workflow 由 durable job/reconciler 推进；source/target namespace runtime 执行 RootCoord/Proxy/DataCoord/QueryCoord 的 fail-closed fence、cache/drain 和 apply。OSS 默认 etcd 路径保持兼容，TiKV 是 Catalog Service backend 的一个实现。

**Tech Stack:** Go, protobuf/gRPC, Milvus RootCoord/Proxy metastore, TiKV/etcd `kv.TxnKV`, Milvus `metastore.RootCoordCatalog`, Prometheus metrics, `mlog`, Go unit/integration/e2e tests.

---

## 代码审查结论

当前代码可以 demo，但不能按 production GA 解释。

### 当前真实 workflow

入口是 `internal/catalogservice/transfer_manager.go` 的 `TransferManager.StartCollectionTransfer`。它不是异步 job，而是一个同步 saga：

```text
load/create transfer job
read source collection snapshot
source RootCoord CatalogTransferPrepare
source catalog DropCollection
target catalog CreateCollection + CreateAlias
source RootCoord CatalogTransferDeactivate
target RootCoord CatalogTransferApply
mark DONE
```

持久 job 在 `internal/catalogservice/transfer_job_store.go`，只按 `transfer_id` 存一条 JSON，通过 `predicates.KeyNotExists` 和 `predicates.ValueEqual` 做 CAS。`cmd/catalogservice/main.go` 使用静态 `--rootcoord-routes` 和 insecure gRPC。`pkg/proto/catalog_service.proto` 只有 `StartCollectionTransfer` / `GetCollectionTransfer` 两个 RPC。

### 当前 RootCoord/Proxy runtime

`internal/rootcoord/catalog_transfer_state.go` 的 `transferGate` 是进程内 map，状态只有 `TRANSFERRING_OUT` 和 `TRANSFERRED_OUT`。`CatalogTransferPrepare` drain 后 freeze，`CatalogTransferDeactivate` 从 live `MetaTable` 删除并 invalidate Proxy cache，`CatalogTransferApply` 校验 target durable catalog 后把 collection 加入 target live `MetaTable`。

Proxy 没有 transfer/owner 概念。`internal/proxy/task_insert.go`、`task_delete.go`、`task_search.go`、`task_query.go`、`task.go` 继续信任 `globalMetaCache`、`channelsMgr` 和 Query/Data plane routing。`ExpireTransferMetaCache` 是 cache eviction，不是 correctness barrier。source transfer-out 也没有专门清理 DML stream，`Proxy.InvalidateCollectionMetaCache` 只有 DropCollection 分支会 `chMgr.removeDMLStream(collectionID)`。

### 当前 KV 原子性

`pkg/kv/predicates/predicate.go` 只有 `ValueEqual` 和 `KeyNotExists`。`internal/kv/tikv/txn_tikv.go` 的 `MultiSaveAndRemove` 支持二者，但 `MultiSaveAndRemoveWithPrefix` 没有正确处理 `PredTargetExists`。`internal/kv/mem/mem_kv.go` 的 `MultiSaveAndRemoveWithPrefix` 遇到 predicate 直接返回不支持。

最大 P0 是 `source catalog DropCollection` 和 `job advance SOURCE_DROPPED` 不原子：如果 source drop 成功后 Catalog Service 崩溃，job 仍停在 `PREPARED`，下一次 retry 会重新读 source 并失败，而不是基于 job snapshot 继续 roll-forward。

## Production 边界

第一版 production 只交付 offline metadata transfer：

- same tenant
- same region
- same object store
- collection 已 drain 或 unloaded
- transfer 期间 source fail-closed
- target 在 durable hidden/staged 后显式 activate
- 不承诺 active DML catch-up
- 不承诺 query/search 连续可用
- 不迁移 DataCoord segment/binlog/index ownership
- 不自动继承全量 RBAC，第一版采用 policy-driven grant mapping

## 文件结构

Catalog Service:

- Modify: `pkg/proto/catalog_service.proto`
- Modify: `internal/catalogservice/server.go`
- Modify: `internal/catalogservice/transfer_manager.go`
- Modify: `internal/catalogservice/transfer_job_store.go`
- Create: `internal/catalogservice/transfer_reconciler.go`
- Create: `internal/catalogservice/transfer_owner_store.go`
- Create: `internal/catalogservice/transfer_visibility_store.go`
- Create: `internal/catalogservice/transfer_audit.go`
- Create: `internal/catalogservice/transfer_policy.go`
- Modify: `internal/catalogservice/rootcoord_transfer_client.go`
- Modify: `cmd/catalogservice/main.go`
- Modify: `cmd/catalogtransferctl/main.go`

Milvus metastore / KV:

- Modify: `pkg/kv/predicates/predicate.go`
- Modify: `internal/kv/tikv/txn_tikv.go`
- Modify: `internal/kv/mem/mem_kv.go`
- Modify: `internal/metastore/rootcoord_catalog.go`
- Modify: `internal/metastore/kv/rootcoord/update.go`
- Create: `internal/metastore/kv/rootcoord/transfer.go`

RootCoord runtime:

- Modify: `pkg/proto/root_coord.proto`
- Modify: `internal/rootcoord/root_coord.go`
- Modify: `internal/rootcoord/catalog_transfer.go`
- Modify: `internal/rootcoord/catalog_transfer_state.go`
- Create: `internal/rootcoord/catalog_transfer_fence_store.go`
- Modify: `internal/rootcoord/meta_table.go`
- Modify: `internal/rootcoord/expire_cache.go`
- Modify: RootCoord DDL/read task files already touched by the POC.

Proxy runtime:

- Modify: `internal/proxy/meta_cache.go`
- Modify: `internal/proxy/impl.go`
- Modify: `internal/proxy/channels_mgr.go`
- Modify: `internal/proxy/task_insert.go`
- Modify: `internal/proxy/task_insert_streaming.go`
- Modify: `internal/proxy/task_delete.go`
- Modify: `internal/proxy/task_search.go`
- Modify: `internal/proxy/task_query.go`
- Modify: `internal/proxy/task.go`

Ops / demo:

- Modify: `scripts/catalog_transfer_demo.sh`
- Modify: `scripts/catalog_transfer_e2e.py`
- Modify: `scripts/catalog_transfer_e2e_test.py`
- Create: `deployments/catalogservice/README.md`
- Create: `docs/runbooks/catalog-service-transfer.zh.md`

## Task 0: Freeze POC Baseline

**Files:**
- Read: `internal/catalogservice/transfer_manager.go`
- Read: `internal/rootcoord/catalog_transfer.go`
- Read: `internal/proxy/meta_cache.go`
- Create: `docs/superpowers/plans/catalog-transfer-current-inventory.zh.md`

- [ ] **Step 1: Run current focused baseline**

Run:

```bash
cd /home/shaoting/workspace/milvus/.worktrees/rootcoord-catalog-production-transfer
go test -tags dynamic,test -gcflags="all=-N -l" ./internal/catalogservice ./internal/rootcoord -run 'TestTransfer|TestCatalogTransfer' -count=1
```

Expected: PASS in a machine with C++ deps. If `milvus_core.pc` or `rocksdb.pc` is missing, record it as environment failure, not code failure.

- [ ] **Step 2: Write inventory doc**

Create `docs/superpowers/plans/catalog-transfer-current-inventory.zh.md` with this content:

```markdown
# Catalog Transfer Current Inventory

## Catalog Service
- StartCollectionTransfer is synchronous.
- Job CAS is transfer_id scoped only.
- No list/claim/reconciler/lease/owner_epoch.

## RootCoord
- transferGate is in-memory.
- Prepare freezes RootCoord metadata paths only.
- Deactivate removes live MetaTable and invalidates Proxy cache.
- Apply loads target live MetaTable from already persisted target catalog.

## Proxy
- No owner_epoch or transfer_state in collectionInfo.
- Data plane trusts globalMetaCache and channelsMgr.
- Cache invalidate is eviction, not a correctness fence.
```

- [ ] **Step 3: Commit baseline doc**

Run:

```bash
git add docs/superpowers/plans/catalog-transfer-current-inventory.zh.md
git commit -m "docs: record catalog transfer poc baseline"
```

Expected: commit succeeds.

## Task 1: Fix KV Predicate Consistency

**Files:**
- Modify: `internal/kv/tikv/txn_tikv.go`
- Modify: `internal/kv/mem/mem_kv.go`
- Test: `internal/kv/mem/mem_kv_test.go`
- Test: `internal/kv/tikv/txn_tikv_test.go`

- [ ] **Step 1: Add mem test for prefix transaction predicates**

Add this test to `internal/kv/mem/mem_kv_test.go`:

```go
func TestMultiSaveAndRemoveWithPrefixPredicates(t *testing.T) {
	kv := NewMemoryKV()
	ctx := context.Background()

	require.NoError(t, kv.Save(ctx, "a/1", "old"))
	require.NoError(t, kv.MultiSaveAndRemoveWithPrefix(
		ctx,
		map[string]string{"b/1": "new"},
		[]string{"a"},
		predicates.ValueEqual("a/1", "old"),
		predicates.KeyNotExists("lock/1"),
	))

	value, err := kv.Load(ctx, "b/1")
	require.NoError(t, err)
	require.Equal(t, "new", value)
	_, err = kv.Load(ctx, "a/1")
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)

	require.Error(t, kv.MultiSaveAndRemoveWithPrefix(
		ctx,
		map[string]string{"c/1": "bad"},
		nil,
		predicates.KeyNotExists("b/1"),
	))
}
```

- [ ] **Step 2: Run failing mem test**

Run:

```bash
go test ./internal/kv/mem -run TestMultiSaveAndRemoveWithPrefixPredicates -count=1
```

Expected before implementation: FAIL with predicate unsupported.

- [ ] **Step 3: Implement mem prefix predicates**

In `internal/kv/mem/mem_kv.go`, make `MultiSaveAndRemoveWithPrefix` evaluate predicates exactly like `MultiSaveAndRemove` before deleting prefixes:

```go
for _, pred := range preds {
	item := kv.tree.Get(memoryKVItem{key: pred.Key()})
	switch pred.Target() {
	case predicates.PredTargetValue:
		if item == nil || !pred.IsTrue(item.(memoryKVItem).value.String()) {
			return merr.WrapErrIoFailedReason("failed to meet predicate")
		}
	case predicates.PredTargetExists:
		if !pred.IsTrue(item != nil) {
			return merr.WrapErrIoFailedReason("failed to meet predicate")
		}
	default:
		return merr.WrapErrParameterInvalid("valid predicate target", pred.Key())
	}
}
```

- [ ] **Step 4: Add TiKV predicate unit/integration coverage**

Add a TiKV test mirroring the mem test in `internal/kv/tikv/txn_tikv_test.go` near existing transaction tests. Use the existing TiKV test fixture in that file and assert `KeyNotExists` succeeds for absent keys and fails for present keys.

- [ ] **Step 5: Implement TiKV prefix predicate switch**

In `internal/kv/tikv/txn_tikv.go`, make `MultiSaveAndRemoveWithPrefix` predicate handling match `MultiSaveAndRemove`:

```go
for _, pred := range preds {
	key := kv.GetPath(pred.Key())
	val, err := txn.Get(ctx, []byte(key))
	switch pred.Target() {
	case predicates.PredTargetValue:
		if err != nil {
			if errors.Is(err, tikverr.ErrNotExist) {
				return markPredicateNotMet(merr.WrapErrIoFailedReason(
					fmt.Sprintf("failed to read predicate target (%s:%v) for MultiSaveAndRemoveWithPrefix", pred.Key(), pred.TargetValue()),
					err.Error(),
				))
			}
			return merr.WrapErrIoFailedReason(
				fmt.Sprintf("failed to read predicate target (%s:%v) for MultiSaveAndRemoveWithPrefix", pred.Key(), pred.TargetValue()),
				err.Error(),
			)
		}
		if !pred.IsTrue(val.Value) {
			return markPredicateNotMet(merr.WrapErrIoFailedReason("failed to meet predicate", fmt.Sprintf("key=%s, value=%v", pred.Key(), pred.TargetValue())))
		}
	case predicates.PredTargetExists:
		if err != nil && !errors.Is(err, tikverr.ErrNotExist) {
			return merr.WrapErrIoFailedReason(
				fmt.Sprintf("failed to read predicate target (%s:%v) for MultiSaveAndRemoveWithPrefix", pred.Key(), pred.TargetValue()),
				err.Error(),
			)
		}
		if !pred.IsTrue(err == nil) {
			return markPredicateNotMet(merr.WrapErrIoFailedReason("failed to meet predicate", fmt.Sprintf("key=%s, exists=%t", pred.Key(), err == nil)))
		}
	default:
		return merr.WrapErrParameterInvalid("valid predicate target", fmt.Sprintf("%d", pred.Target()))
	}
}
```

- [ ] **Step 6: Verify KV**

Run:

```bash
go test ./internal/kv/mem -run TestMultiSaveAndRemoveWithPrefixPredicates -count=1
go test ./internal/kv/tikv -run TestMultiSaveAndRemoveWithPrefixPredicates -count=1
```

Expected: PASS.

## Task 2: Production Catalog Service API

**Files:**
- Modify: `pkg/proto/catalog_service.proto`
- Modify: `scripts/generate_proto.sh`
- Modify generated files under `pkg/proto/catalogpb/`
- Test: `internal/catalogservice/server_transfer_test.go`

- [ ] **Step 1: Replace synchronous API shape**

Change `CatalogService` in `pkg/proto/catalog_service.proto` to this production control surface:

```proto
service CatalogService {
  rpc DryRunCollectionTransfer(DryRunCollectionTransferRequest) returns (DryRunCollectionTransferResponse) {}
  rpc CreateCollectionTransfer(CreateCollectionTransferRequest) returns (CreateCollectionTransferResponse) {}
  rpc ApproveCollectionTransfer(ApproveCollectionTransferRequest) returns (ApproveCollectionTransferResponse) {}
  rpc StartCollectionTransfer(StartCollectionTransferRequest) returns (StartCollectionTransferResponse) {}
  rpc GetCollectionTransfer(GetCollectionTransferRequest) returns (GetCollectionTransferResponse) {}
  rpc ListCollectionTransfers(ListCollectionTransfersRequest) returns (ListCollectionTransfersResponse) {}
  rpc CancelCollectionTransfer(CancelCollectionTransferRequest) returns (CancelCollectionTransferResponse) {}
  rpc ResumeCollectionTransfer(ResumeCollectionTransferRequest) returns (ResumeCollectionTransferResponse) {}
}
```

Keep `StartCollectionTransfer` for compatibility, but change its semantic to enqueue/resume an approved job and return quickly.

- [ ] **Step 2: Add production job state messages**

Add these states:

```proto
enum CollectionTransferState {
  COLLECTION_TRANSFER_STATE_UNSPECIFIED = 0;
  COLLECTION_TRANSFER_STATE_REQUESTED = 1;
  COLLECTION_TRANSFER_STATE_DRY_RUN_VALIDATED = 2;
  COLLECTION_TRANSFER_STATE_APPROVED = 3;
  COLLECTION_TRANSFER_STATE_SOURCE_FENCING = 4;
  COLLECTION_TRANSFER_STATE_SOURCE_DRAINED = 5;
  COLLECTION_TRANSFER_STATE_TARGET_STAGED = 6;
  COLLECTION_TRANSFER_STATE_SOURCE_DEACTIVATED = 7;
  COLLECTION_TRANSFER_STATE_TARGET_ACTIVATED = 8;
  COLLECTION_TRANSFER_STATE_DONE = 9;
  COLLECTION_TRANSFER_STATE_FAILED_RETRYABLE = 10;
  COLLECTION_TRANSFER_STATE_COMMIT_UNCERTAIN = 11;
  COLLECTION_TRANSFER_STATE_PAUSED_OPERATOR_REQUIRED = 12;
  COLLECTION_TRANSFER_STATE_ABORTED = 13;
}

message CollectionTransferJob {
  string transfer_id = 1;
  string request_hash = 2;
  string source_namespace = 3;
  string target_namespace = 4;
  string db_name = 5;
  string collection_name = 6;
  int64 collection_id = 7;
  int64 owner_epoch = 8;
  int64 claim_epoch = 9;
  CollectionTransferState state = 10;
  string last_error = 11;
  bool recoverable = 12;
  uint64 created_at = 13;
  uint64 updated_at = 14;
  repeated string completed_steps = 15;
}
```

- [ ] **Step 3: Add server tests**

In `internal/catalogservice/server_transfer_test.go`, add tests:

```go
func TestServerCreateCollectionTransferReturnsJobWithoutRunningWorkflow(t *testing.T) {
	mgr := NewMockCollectionTransferManager(t)
	mgr.EXPECT().CreateCollectionTransfer(mock.Anything, mock.Anything).Return(&TransferJob{
		TransferID: "t1",
		State:      TransferStateRequested,
	}, nil)

	server := NewServer(mgr)
	resp, err := server.CreateCollectionTransfer(context.Background(), &catalogpb.CreateCollectionTransferRequest{
		TransferId:      "t1",
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DbName:          "default",
		CollectionName:  "c1",
	})

	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetCode())
	require.Equal(t, catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_REQUESTED, resp.GetJob().GetState())
}
```

- [ ] **Step 4: Generate proto**

Run:

```bash
make generated-proto-without-cpp
```

Expected: generated `pkg/proto/catalogpb/*` and root service stubs compile.

## Task 3: Durable Transfer Job Store With Claim

**Files:**
- Modify: `internal/catalogservice/transfer_job_store.go`
- Modify: `internal/catalogservice/transfer_manager.go`
- Test: `internal/catalogservice/transfer_job_store_test.go`

- [ ] **Step 1: Define store interface**

Replace `TransferJobStore` with this interface:

```go
type TransferJobStore interface {
	Get(ctx context.Context, transferID string) (*TransferJob, error)
	List(ctx context.Context, filter TransferJobFilter) ([]*TransferJob, error)
	Create(ctx context.Context, job *TransferJob) error
	CompareAndSave(ctx context.Context, expected *TransferJob, job *TransferJob) error
	Claim(ctx context.Context, transferID string, workerID string, now time.Time, ttl time.Duration) (*TransferJob, error)
	Heartbeat(ctx context.Context, transferID string, workerID string, claimEpoch int64, now time.Time, ttl time.Duration) error
	Release(ctx context.Context, transferID string, workerID string, claimEpoch int64) error
}

type TransferJobFilter struct {
	States []TransferState
	Limit  int
}
```

Extend `TransferJob`:

```go
WorkerID           string
ClaimEpoch         int64
LeaseExpireUnixSec int64
Attempt            int64
CreatedAtUnixSec   int64
UpdatedAtUnixSec   int64
LastStep           string
CompletedSteps     []string
```

- [ ] **Step 2: Add claim tests**

Add tests in `internal/catalogservice/transfer_job_store_test.go`:

```go
func TestKVTransferJobStoreClaimRejectsActiveLease(t *testing.T) {
	store := newTestKVTransferJobStore(t)
	ctx := context.Background()
	require.NoError(t, store.Create(ctx, &TransferJob{TransferID: "t1", State: TransferStateApproved}))

	first, err := store.Claim(ctx, "t1", "worker-a", time.Unix(100, 0), time.Minute)
	require.NoError(t, err)
	require.Equal(t, "worker-a", first.WorkerID)

	_, err = store.Claim(ctx, "t1", "worker-b", time.Unix(110, 0), time.Minute)
	require.ErrorIs(t, err, errTransferJobModified)
}

func TestKVTransferJobStoreClaimAllowsExpiredLease(t *testing.T) {
	store := newTestKVTransferJobStore(t)
	ctx := context.Background()
	require.NoError(t, store.Create(ctx, &TransferJob{TransferID: "t1", State: TransferStateApproved}))
	_, err := store.Claim(ctx, "t1", "worker-a", time.Unix(100, 0), time.Second)
	require.NoError(t, err)

	second, err := store.Claim(ctx, "t1", "worker-b", time.Unix(102, 0), time.Second)
	require.NoError(t, err)
	require.Equal(t, "worker-b", second.WorkerID)
	require.Equal(t, int64(2), second.ClaimEpoch)
}
```

- [ ] **Step 3: Implement KV claim**

Use `Get` + mutate copy + `CompareAndSave`. Claim succeeds when:

```go
job.WorkerID == "" || job.LeaseExpireUnixSec <= now.Unix() || job.WorkerID == workerID
```

Claim mutation:

```go
next.WorkerID = workerID
next.ClaimEpoch++
next.LeaseExpireUnixSec = now.Add(ttl).Unix()
next.UpdatedAtUnixSec = now.Unix()
```

On CAS mismatch, return `errTransferJobModified`.

- [ ] **Step 4: Verify job store**

Run:

```bash
go test ./internal/catalogservice -run 'TestKVTransferJobStoreClaim|TestKVTransferJobStoreCompareAndSave' -count=1
```

Expected: PASS.

## Task 4: Owner And Visibility Records

**Files:**
- Create: `internal/catalogservice/transfer_owner_store.go`
- Create: `internal/catalogservice/transfer_visibility_store.go`
- Modify: `internal/catalogservice/transfer_manager.go`
- Test: `internal/catalogservice/transfer_owner_store_test.go`

- [ ] **Step 1: Define owner model**

Create `internal/catalogservice/transfer_owner_store.go`:

```go
type CollectionOwnerState string

const (
	CollectionOwnerActive         CollectionOwnerState = "ACTIVE"
	CollectionOwnerSourceFenced   CollectionOwnerState = "SOURCE_FENCED"
	CollectionOwnerTargetHidden   CollectionOwnerState = "TARGET_HIDDEN"
	CollectionOwnerTargetVisible  CollectionOwnerState = "TARGET_VISIBLE"
	CollectionOwnerTransferredOut CollectionOwnerState = "TRANSFERRED_OUT"
)

type CollectionOwnerRecord struct {
	TenantID          string
	SourceNamespace  string
	TargetNamespace  string
	OwnerNamespace   string
	DBID             int64
	DBName           string
	CollectionID     int64
	CollectionName   string
	OwnerEpoch       int64
	TransferID       string
	State            CollectionOwnerState
	Version          int64
	UpdatedAtUnixSec int64
}

type CollectionOwnerStore interface {
	Get(ctx context.Context, namespace string, dbID int64, collectionID int64) (*CollectionOwnerRecord, error)
	CompareAndSave(ctx context.Context, expected *CollectionOwnerRecord, next *CollectionOwnerRecord) error
	ReserveSource(ctx context.Context, req OwnerReservationRequest) (*CollectionOwnerRecord, error)
	ReserveTargetHidden(ctx context.Context, req OwnerReservationRequest) (*CollectionOwnerRecord, error)
	ActivateTarget(ctx context.Context, transferID string, ownerEpoch int64) error
	MarkSourceTransferredOut(ctx context.Context, transferID string, ownerEpoch int64) error
}
```

- [ ] **Step 2: Add owner reservation tests**

Add tests:

```go
func TestOwnerStoreRejectsConcurrentSourceReservation(t *testing.T) {
	store := newTestOwnerStore(t)
	ctx := context.Background()
	_, err := store.ReserveSource(ctx, OwnerReservationRequest{
		TransferID: "t1", SourceNamespace: "milvus1", DBID: 1, CollectionID: 100, OwnerEpoch: 10,
	})
	require.NoError(t, err)

	_, err = store.ReserveSource(ctx, OwnerReservationRequest{
		TransferID: "t2", SourceNamespace: "milvus1", DBID: 1, CollectionID: 100, OwnerEpoch: 11,
	})
	require.ErrorIs(t, err, errTransferJobModified)
}
```

- [ ] **Step 3: Store keys**

Use these exact keys under the Catalog Service job prefix:

```text
owners/source/<source_namespace>/<db_id>/<collection_id>
owners/name/<target_namespace>/<db_id>/<collection_name>
owners/id/<target_namespace>/<db_id>/<collection_id>
owners/transfer/<transfer_id>
```

All reservation writes must be one `MultiSaveAndRemove` with `KeyNotExists` predicates for the unique keys.

- [ ] **Step 4: Verify owner store**

Run:

```bash
go test ./internal/catalogservice -run TestOwnerStore -count=1
```

Expected: PASS.

## Task 5: Reconciler And Step Runner

**Files:**
- Create: `internal/catalogservice/transfer_reconciler.go`
- Modify: `internal/catalogservice/transfer_manager.go`
- Test: `internal/catalogservice/transfer_reconciler_test.go`
- Test: `internal/catalogservice/transfer_manager_test.go`

- [ ] **Step 1: Split manager responsibilities**

Make `TransferManager.StartCollectionTransfer` enqueue only:

```go
func (m *TransferManager) StartCollectionTransfer(ctx context.Context, req StartCollectionTransferRequest) (*StartCollectionTransferResponse, error) {
	job, err := m.createOrLoadApprovedJob(ctx, req)
	if err != nil {
		return nil, err
	}
	m.wakeReconciler(job.TransferID)
	return &StartCollectionTransferResponse{
		TransferID: job.TransferID,
		State:      job.State,
		CollectionID: job.CollectionID,
	}, nil
}
```

Move the existing synchronous side effects into:

```go
func (m *TransferManager) runTransferStep(ctx context.Context, job *TransferJob) (*TransferJob, error)
```

- [ ] **Step 2: Add source-drop crash regression**

Add this test:

```go
func TestTransferManagerRecoversAfterSourceDropBeforeJobAdvance(t *testing.T) {
	ctx := context.Background()
	env := newTransferManagerCrashTestEnv(t)
	job := env.createPreparedJobWithSnapshot("transfer-1")

	env.sourceCatalog.DropSucceedsOnce()
	env.jobStore.FailNextCompareAndSave()
	_, err := env.manager.runTransferStep(ctx, job)
	require.Error(t, err)

	reloaded, err := env.jobStore.Get(ctx, "transfer-1")
	require.NoError(t, err)
	require.Equal(t, TransferStatePrepared, reloaded.State)
	require.NotNil(t, reloaded.Collection)

	env.sourceCatalog.CollectionAlreadyDropped()
	next, err := env.manager.runTransferStep(ctx, reloaded)
	require.NoError(t, err)
	require.Equal(t, TransferStateSourceDropped, next.State)
	require.True(t, env.targetCatalog.CreateNotCalled())
}
```

Implementation rule: when state is `PREPARED` and source collection is already missing but `job.Collection` snapshot exists and owner record is `SOURCE_FENCED`, advance to `SOURCE_DROPPED` instead of aborting.

- [ ] **Step 3: Add reconciler claim tests**

Add tests:

```go
func TestTransferReconcilerClaimsAndRunsOnlyOneWorker(t *testing.T) {
	store := newTestKVTransferJobStore(t)
	job := &TransferJob{TransferID: "t1", State: TransferStateApproved}
	require.NoError(t, store.Create(context.Background(), job))

	r1 := NewTransferReconciler("worker-a", store, fakeStepRunner{})
	r2 := NewTransferReconciler("worker-b", store, fakeStepRunner{})

	require.NoError(t, r1.Tick(context.Background()))
	require.NoError(t, r2.Tick(context.Background()))

	ran := fakeStepRunnerRunCount("t1")
	require.Equal(t, 1, ran)
}
```

- [ ] **Step 4: Implement reconciler**

`transfer_reconciler.go` responsibilities:

```go
type TransferReconciler struct {
	workerID string
	store    TransferJobStore
	runner   TransferStepRunner
	leaseTTL time.Duration
}

type TransferStepRunner interface {
	RunTransferStep(ctx context.Context, job *TransferJob) (*TransferJob, error)
}
```

`Tick` lists non-terminal jobs, claims one job, runs one state transition, heartbeats before long RPCs, and releases terminal jobs.

- [ ] **Step 5: Verify reconciler**

Run:

```bash
go test ./internal/catalogservice -run 'TestTransferReconciler|TestTransferManagerRecoversAfterSourceDrop' -count=1
```

Expected: PASS.

## Task 6: Transfer-Specific RootCoordCatalog Operations

**Files:**
- Modify: `internal/metastore/rootcoord_catalog.go`
- Create: `internal/metastore/kv/rootcoord/transfer.go`
- Modify: `internal/metastore/mocks/mock_rootcoord_catalog.go`
- Test: `internal/metastore/kv/rootcoord/transfer_test.go`

- [ ] **Step 1: Add interface**

Extend `metastore.RootCoordCatalog`:

```go
DropCollectionForTransfer(ctx context.Context, coll *model.Collection, transferID string, ownerEpoch int64, ts typeutil.Timestamp) error
CreateCollectionForTransfer(ctx context.Context, coll *model.Collection, transferID string, ownerEpoch int64, ts typeutil.Timestamp) error
```

- [ ] **Step 2: Add tests**

In `internal/metastore/kv/rootcoord/transfer_test.go`:

```go
func TestCatalogDropCollectionForTransferRequiresExpectedSnapshot(t *testing.T) {
	catalog, kv := newTestRootCoordCatalog(t)
	coll := testCollectionForTransfer()
	require.NoError(t, catalog.CreateCollection(context.Background(), coll, 100))

	modified := coll.Clone()
	modified.SchemaVersion++
	err := catalog.DropCollectionForTransfer(context.Background(), modified, "t1", 10, 101)
	require.Error(t, err)

	got, err := catalog.GetCollectionByID(context.Background(), coll.DBID, typeutil.MaxTimestamp, coll.CollectionID)
	require.NoError(t, err)
	require.Equal(t, coll.CollectionID, got.CollectionID)
	require.NotNil(t, kv)
}

func TestCatalogCreateCollectionForTransferIsAbsentOrSameSnapshot(t *testing.T) {
	catalog, _ := newTestRootCoordCatalog(t)
	coll := testCollectionForTransfer()

	require.NoError(t, catalog.CreateCollectionForTransfer(context.Background(), coll, "t1", 10, 100))
	require.NoError(t, catalog.CreateCollectionForTransfer(context.Background(), coll.Clone(), "t1", 10, 101))

	conflict := coll.Clone()
	conflict.Name = "conflict"
	require.Error(t, catalog.CreateCollectionForTransfer(context.Background(), conflict, "t2", 11, 102))
}
```

- [ ] **Step 3: Implement with predicates**

`DropCollectionForTransfer` reads the current collection value key and requires `ValueEqual(collectionKey, expectedValue)` before removing child keys and collection key. `CreateCollectionForTransfer` requires collection key absent or equal to the same marshaled snapshot. If the same snapshot exists, return success without overwriting children unless child reconciliation is required by the same method.

- [ ] **Step 4: Regenerate mocks**

Run:

```bash
make generate-mockery-metastore
```

Expected: mock interface includes the two transfer methods.

## Task 7: Durable RootCoord Transfer Fence

**Files:**
- Modify: `pkg/proto/root_coord.proto`
- Modify generated `pkg/proto/rootcoordpb/*`
- Modify: `internal/rootcoord/catalog_transfer_state.go`
- Create: `internal/rootcoord/catalog_transfer_fence_store.go`
- Modify: `internal/rootcoord/catalog_transfer.go`
- Modify: `internal/rootcoord/root_coord.go`
- Test: `internal/rootcoord/catalog_transfer_state_test.go`
- Test: `internal/rootcoord/catalog_transfer_test.go`

- [ ] **Step 1: Add fence record**

Create `internal/rootcoord/catalog_transfer_fence_store.go`:

```go
type TransferFenceState string

const (
	TransferFenceSourceFenced   TransferFenceState = "SOURCE_FENCED"
	TransferFenceTransferredOut TransferFenceState = "TRANSFERRED_OUT"
)

type TransferFenceRecord struct {
	TransferID   string
	OwnerEpoch   int64
	CollectionID int64
	DBName       string
	CollectionName string
	State        TransferFenceState
	Version      int64
}

type TransferFenceStore interface {
	SaveFence(ctx context.Context, expected *TransferFenceRecord, next *TransferFenceRecord) error
	GetFence(ctx context.Context, collectionID int64) (*TransferFenceRecord, error)
	ListFences(ctx context.Context) ([]*TransferFenceRecord, error)
	RemoveFence(ctx context.Context, collectionID int64, transferID string, ownerEpoch int64) error
}
```

- [ ] **Step 2: Restore gate on RootCoord start**

In `internal/rootcoord/root_coord.go`, after `MetaTable` initialization and before RootCoord becomes healthy:

```go
func (c *Core) restoreCatalogTransferFences(ctx context.Context) error {
	records, err := c.transferFenceStore.ListFences(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		c.transferGate.Restore(record.CollectionID, transferGateEntry{
			transferID: record.TransferID,
			epoch:      record.OwnerEpoch,
			state:      transferFenceStateToGateState(record.State),
		})
	}
	return nil
}
```

- [ ] **Step 3: Persist before in-memory gate**

In `CatalogTransferPrepare`, write `SOURCE_FENCED` durable fence before `FreezeWithDrain`. In `CatalogTransferDeactivate`, write `TRANSFERRED_OUT` before `transferGate.Deactivate`. In `CatalogTransferAbort`, remove durable fence only when state is still `SOURCE_FENCED`.

- [ ] **Step 4: Add restart tests**

Add tests:

```go
func TestCatalogTransferPrepareFenceRestoredAfterRootCoordRestart(t *testing.T) {
	store := newMemoryTransferFenceStore()
	core1 := newTestCoreWithTransferFenceStore(t, store)
	status, err := core1.CatalogTransferPrepare(context.Background(), &rootcoordpb.CatalogTransferPrepareRequest{
		TransferId: "t1", TransferEpoch: 10, CollectionId: 100, DbName: "default", CollectionName: "c1", DrainTimeoutMs: 1000,
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.GetCode())

	core2 := newTestCoreWithTransferFenceStore(t, store)
	require.NoError(t, core2.restoreCatalogTransferFences(context.Background()))
	require.ErrorIs(t, core2.transferGate.AllowUserOperation(100, 0), errCollectionTransferring)
}
```

- [ ] **Step 5: Verify RootCoord fence**

Run:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" ./internal/rootcoord -run 'TestCatalogTransfer.*Restart|TestTransferGate' -count=1
```

Expected: PASS.

## Task 8: Target Hidden Visibility

**Files:**
- Modify: `internal/catalogservice/transfer_manager.go`
- Modify: `internal/rootcoord/catalog_transfer.go`
- Modify: `internal/rootcoord/meta_table.go`
- Modify: `internal/rootcoord/describe_collection_task.go`
- Modify: `internal/rootcoord/show_collection_task.go`
- Modify: `internal/rootcoord/has_collection_task.go`
- Test: `internal/rootcoord/catalog_transfer_meta_test.go`

- [ ] **Step 1: Add hidden target state**

Target durable metadata must be created as hidden/staged before user-visible apply. Use owner record `TARGET_HIDDEN` as the visibility authority. Do not expose target collection from RootCoord read paths until `CatalogTransferApply` has activated it.

- [ ] **Step 2: Add read filtering test**

```go
func TestTargetHiddenCollectionNotVisibleUntilApply(t *testing.T) {
	core := newTestCoreWithOwnerStore(t)
	core.ownerStore.Set(&CollectionOwnerRecord{
		OwnerNamespace: "milvus2",
		CollectionID: 100,
		CollectionName: "c1",
		State: CollectionOwnerTargetHidden,
	})
	core.meta.MustApplyCollectionLive(testCollectionForTransfer())

	resp, err := core.DescribeCollection(context.Background(), &milvuspb.DescribeCollectionRequest{
		DbName: "default", CollectionName: "c1",
	})
	require.NoError(t, err)
	require.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetCode())
}
```

- [ ] **Step 3: Apply activation**

`CatalogTransferApply` must transition owner record from `TARGET_HIDDEN` to `TARGET_VISIBLE` with expected `transfer_id` and `owner_epoch`, then load `MetaTable`, then invalidate target Proxy cache. If live apply succeeds but owner activation status is unknown, job becomes `COMMIT_UNCERTAIN`.

- [ ] **Step 4: Verify visibility**

Run:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" ./internal/rootcoord -run 'TestTargetHidden|TestCoreCatalogTransferApply' -count=1
```

Expected: PASS.

## Task 9: Proxy Fail-Closed Ownership Checks

**Files:**
- Modify: `internal/proxy/meta_cache.go`
- Modify: `internal/proxy/impl.go`
- Modify: `internal/proxy/channels_mgr.go`
- Modify: `internal/proxy/task_insert.go`
- Modify: `internal/proxy/task_insert_streaming.go`
- Modify: `internal/proxy/task_delete.go`
- Modify: `internal/proxy/task_search.go`
- Modify: `internal/proxy/task_query.go`
- Modify: `internal/proxy/task.go`
- Test: `internal/proxy/meta_cache_test.go`
- Test: `internal/proxy/impl_test.go`
- Test: `internal/proxy/task_insert_test.go`
- Test: `internal/proxy/task_search_test.go`
- Test: `internal/proxy/task_query_test.go`
- Test: `internal/proxy/task_delete_test.go`

- [ ] **Step 1: Extend collectionInfo**

Add fields in `internal/proxy/meta_cache.go`:

```go
ownerNamespace string
ownerEpoch     int64
transferState  string
```

Populate them from `DescribeCollectionResponse` after extending RootCoord proto response or response properties.

- [ ] **Step 2: Add cache stale test**

```go
func TestMetaCacheRejectsTransferredOutCollectionInfo(t *testing.T) {
	cache := newTestMetaCache(t)
	cache.putCollectionInfo(&collectionInfo{
		collID: 100,
		dbName: "default",
		schema: newTestSchemaInfo("c1"),
		ownerEpoch: 10,
		transferState: "TRANSFERRED_OUT",
	})

	_, err := cache.GetCollectionInfo(context.Background(), "default", "c1", 100)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
}
```

- [ ] **Step 3: Add transfer-out invalidate branch**

In `internal/proxy/impl.go`, add a specific msg type or flag for transfer-out. It must:

```go
globalMetaCache.InvalidateCollectionMeta(ctx, dbName, collectionName, collectionID, false)
node.chMgr.removeDMLStream(collectionID)
node.shardClientMgr.InvalidateShardLeaderCache([]int64{collectionID})
```

- [ ] **Step 4: Add task fail-fast tests**

For insert/search/query/delete PreExecute, add one test per task: cached collection has `transferState=TRANSFERRED_OUT`, operation returns collection not found or transfer-fenced error before allocating row IDs, opening DML stream, or calling QueryCoord.

- [ ] **Step 5: Verify Proxy**

Run:

```bash
go test ./internal/proxy -run 'Test.*Transfer|TestMetaCacheRejectsTransferredOut' -count=1
```

Expected: PASS.

## Task 10: Governance, Auth, Audit

**Files:**
- Create: `internal/catalogservice/transfer_policy.go`
- Create: `internal/catalogservice/transfer_audit.go`
- Modify: `internal/catalogservice/server.go`
- Modify: `cmd/catalogservice/main.go`
- Test: `internal/catalogservice/transfer_policy_test.go`
- Test: `internal/catalogservice/transfer_audit_test.go`

- [ ] **Step 1: Add policy interface**

Create:

```go
type TransferPrincipal struct {
	TenantID string
	User    string
	Roles   []string
}

type TransferPolicy interface {
	AuthorizeCreate(ctx context.Context, principal TransferPrincipal, req StartCollectionTransferRequest) error
	AuthorizeApprove(ctx context.Context, principal TransferPrincipal, transferID string) error
	AuthorizeCancel(ctx context.Context, principal TransferPrincipal, transferID string) error
}
```

First production policy:

```text
same tenant only
source namespace and target namespace both belong to tenant
principal has CatalogTransferAdmin or platform operator role
RBAC grants are not blindly copied; dry-run reports grant mapping plan
```

- [ ] **Step 2: Add audit event**

Create:

```go
type TransferAuditEvent struct {
	EventID      string
	TransferID   string
	Principal    TransferPrincipal
	Action       string
	BeforeState  TransferState
	AfterState   TransferState
	Decision     string
	Reason       string
	CreatedAtUnixSec int64
}
```

Audit every create/dry-run/approve/start/cancel/resume/admin-override.

- [ ] **Step 3: Server auth tests**

Add tests that unauthenticated requests are rejected and unauthorized cross-tenant transfer returns permission denied before creating a job.

- [ ] **Step 4: Verify governance**

Run:

```bash
go test ./internal/catalogservice -run 'TestTransferPolicy|TestTransferAudit|TestServer.*Authorize' -count=1
```

Expected: PASS.

## Task 11: Dynamic Namespace Runtime Resolver

**Files:**
- Modify: `cmd/catalogservice/main.go`
- Create: `internal/catalogservice/namespace_runtime_resolver.go`
- Test: `internal/catalogservice/namespace_runtime_resolver_test.go`

- [ ] **Step 1: Replace static route as production default**

Keep `--rootcoord-routes` only for demo/debug. Add resolver interface:

```go
type NamespaceRuntimeResolver interface {
	ResolveRootCoord(ctx context.Context, namespace string) (TransferRootCoord, error)
	ResolveRuntime(ctx context.Context, namespace string) (*NamespaceRuntimeEndpoint, error)
}

type NamespaceRuntimeEndpoint struct {
	Namespace string
	RootCoordAddress string
	LeaderEpoch int64
	CertSAN string
	UpdatedAtUnixSec int64
}
```

- [ ] **Step 2: Add resolver tests**

Tests cover namespace not found, stale leader epoch rejected, static route fallback only when explicitly enabled.

- [ ] **Step 3: Wire mTLS config**

`cmd/catalogservice/main.go` must accept:

```text
--tls-cert
--tls-key
--tls-ca
--require-client-cert
--runtime-resolver=static|catalog-registry
```

Production manifests must use TLS; demo may use insecure with explicit `--allow-insecure-demo`.

## Task 12: Ops, Metrics, Runbook

**Files:**
- Modify: `internal/catalogservice/transfer_manager.go`
- Modify: `internal/catalogservice/transfer_reconciler.go`
- Create: `docs/runbooks/catalog-service-transfer.zh.md`
- Create: `deployments/catalogservice/README.md`
- Modify: `scripts/catalog_transfer_demo.sh`
- Modify: `scripts/catalog_transfer_e2e.py`

- [ ] **Step 1: Add metrics**

Expose:

```text
catalog_transfer_jobs_total{state}
catalog_transfer_phase_duration_seconds{phase,result}
catalog_transfer_reconcile_attempts_total{result}
catalog_transfer_claim_conflicts_total
catalog_transfer_commit_uncertain_total
catalog_transfer_runtime_rpc_duration_seconds{namespace,method,result}
catalog_transfer_proxy_fence_reject_total{namespace,operation}
```

- [ ] **Step 2: Add runbook**

`docs/runbooks/catalog-service-transfer.zh.md` must include:

```markdown
# Catalog Service Transfer Runbook

## 前置检查
- tenant/region/object store 相同
- collection 已停止写入
- source Proxy fence metrics 无持续通过
- target DB exists and DBID mapping valid

## 状态处理
- FAILED_RETRYABLE: reconciler 自动 retry
- COMMIT_UNCERTAIN: operator 运行 verify 后选择 roll-forward 或 mark-done
- PAUSED_OPERATOR_REQUIRED: 需要审批或手工修复冲突

## 回滚边界
- SOURCE_FENCED 前可 abort
- TARGET_STAGED 后默认 roll-forward
- SOURCE_DEACTIVATED 后不支持自动 rollback
```

- [ ] **Step 3: Upgrade e2e**

`scripts/catalog_transfer_e2e.py` must verify:

```text
milvus1 insert loop starts
transfer dry-run passes
source fence starts and insert is rejected without stopping Milvus process
source cache invalidated and DML stream removed
target hidden is not visible before apply
target apply makes collection visible
milvus2 describe/search path refreshes cache from RootCoord
job is DONE
```

- [ ] **Step 4: Verify e2e**

Run:

```bash
bash scripts/catalog_transfer_demo.sh --backend tikv --strict-production-checks
python3 -m unittest scripts/catalog_transfer_e2e_test.py
```

Expected: transfer completes through Catalog Service backed by TiKV and strict checks pass.

## GA Gate

不能进入 production GA，除非以下全部满足：

- Source fence survives RootCoord restart.
- Source transfer-out survives Proxy stale cache and clears DML stream.
- Target hidden metadata is not user visible before activation.
- Source drop/job advance crash can roll-forward from durable snapshot.
- Multi Catalog Service instances cannot execute duplicate side effects.
- Reconciler resumes every non-terminal state after restart.
- All transfer RPCs have mTLS/authz/audit.
- `StartCollectionTransfer` no longer blocks on full workflow.
- TiKV backend passes KV predicate and transfer crash tests.
- E2E uses two real Milvus processes and TiKV, not container stop as gate.

## Review Notes From Parallel Agents

Catalog Service review:

- Current `StartCollectionTransfer` is synchronous.
- Job store has no list/claim/reconciler.
- Client-supplied epoch/timestamp is unsafe.
- Static insecure RootCoord routes are demo-only.

RootCoord/Proxy review:

- `transferGate` is in-memory only.
- RootCoord guard covers metadata paths, not already-resolved Proxy data plane.
- Transfer invalidate is not a correctness barrier.
- Source transfer-out must clear DML stream and shard/cache routing.

KV/metastore review:

- TiKV supports needed base transaction for single `MultiSaveAndRemove`, but predicate semantics are inconsistent in prefix transaction.
- Current `DropCollection`/`CreateCollection` are ordinary overwrite/delete operations, not transfer-specific CAS.
- `source DropCollection` and job state advance are the highest crash-consistency risk.
