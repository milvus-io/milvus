# DataCoord Backfill Delta-Based Manifest Commit

- **Created:** 2026-09-15
- **Status:** Draft
- **Component:** DataCoord / StorageV3 / Backfill
- **Related work:**
  - [DataCoord Segment-Scoped Manifest Commit Framework](./20260817-datacoord-segment-manifest-commit.md) (the commit framework this design routes backfill into)
  - [Milvus Storage Manifest Format](./20260226-manifest-format.md) (manifest / transaction semantics)
  - [StorageV3 Manifest Index Publication](./20260811-storagev3-manifest-index-publication.md) (index entries carried by manifest revisions)

## Summary

`CommitBackfillResult` today publishes a Spark-produced, externally pre-built
StorageV3 manifest **version** by advancing `SegmentInfo.manifest_path` through
`UpdateManifestVersion` — the "version adoption" bypass documented as a known
limitation of the segment manifest commit framework. This design replaces the
V3 backfill commit with a **delta-based commit**: the backfill result carries
the backfill's **operations** (file lists + op type, `add` / `replace`) instead
of a pre-baked version, and DataCoord applies them to the segment's **current**
manifest through `meta.CommitSegmentManifests`, generating the new revision
under the per-segment manifest lock.

Two explicit design decisions accompany the change:

1. **The backfill commit does NOT go through the broadcast framework.**
   `CommitBackfillResult` calls the commit framework synchronously and directly;
   it no longer emits a `BatchUpdateManifestMessageV2` and no longer relies on
   the WAL control channel or a broadcaster resource-key serialization.
2. **In a global cluster, the primary and standby DataCoords each perform the
   backfill commit independently** (the convention is fixed in this document).
   The commit is therefore a per-cluster catalog operation that must be safe,
   idempotent, and self-contained when executed separately on each cluster's
   own metadata.

## Problem

### Version adoption is a known bypass

The current V3 path (`internal/datacoord/services_commit_backfill.go`):

```text
CommitBackfillResult
  -> classifyBackfillSegments        // validate entry.Version > currentVer
  -> broadcast BatchUpdateManifestMessageV2 (WAL control channel)
  -> batchUpdateManifestV2AckCallback
      -> UpdateManifestVersion(segID, entry.Version)   // meta.go
```

Spark builds the new manifest revision itself (against a manifest version it
snapshot at job start) and reports only the resulting `version`. DataCoord
adopts the pointer under **version monotonicity alone** — it never verifies the
base the revision was built from. The segment-manifest-commit design doc
records the consequence:

> A revision built from base N and adopted after a commit published N+1 wins on
> version alone, and the storage layer's overwrite resolver does not merge, so
> the newer revision's content is silently dropped — index entries included.
> Until adoption carries the base version the revision was built from and
> rejects a stale one (follow-up), no index build or GC retraction may overlap
> a backfill window on the same segment.

`entry.Version > currentVer` only blocks rollback; it cannot block a forward
version that was built from a stale base, which silently discards concurrent
stats / index commits.

### Broadcast is the wrong transport for backfill commit

The broadcast pipeline is bound to one cluster's streaming WAL (control
channel) and a broadcaster resource-key guard. It serializes the commit against
collection DDL in the same cluster and applies the update asynchronously in the
ack callback. This is per-cluster infrastructure: it cannot express a commit
that must be applied independently on both the primary and the standby of a
global cluster, and it couples the backfill commit to the streaming layer's
availability. The backfill commit is a catalog + manifest mutation that the
DataCoord can execute directly; the broadcast indirection adds an ack window
without adding a cross-cluster guarantee.

## Goals

1. Backfill commits are **delta-based**: the result carries `add` / `replace`
   operations (file lists) and DataCoord builds the new manifest revision from
   the segment's current in-lock pointer via
   `meta.CommitSegmentManifests` (`ManifestMutationCommitUpdates`).
2. **Legacy results remain supported** with a source-version fence: a
   pre-baked-version result carries the base version it was built from, and the
   commit is rejected when the current manifest version differs (stats / index
   advanced it). Delta results need no source comparison.
3. The backfill commit **does not use the broadcast framework** — it is a
   direct, synchronous catalog + manifest operation.
4. **Global cluster convention:** primary and standby DataCoords each commit
   the backfill result against their own metadata; the commit is idempotent and
   self-contained so a separate execution on either cluster is safe.
5. Per-segment failures are surfaced through the existing
   `CommitBackfillResultResponse.SegmentStatuses` contract.

## Non-goals

- Migrating the public `BatchUpdateManifest` RPC
  (`internal/datacoord/services.go`) or its ack callback
  (`ddl_callbacks_batch_update_manifest.go`) off the broadcast path. That path
  (the "batch DDL" adoption side of the same known bypass) is out of scope and
  keeps its current transport.
- Removing `UpdateManifestVersion`; it still serves the public RPC path.
- Changing the V2 backfill commit (column-group upsert via
  `UpdateSegmentColumnGroupsOperator`), apart from invoking it directly instead
  of through the broadcast callback.
- A cross-cluster distributed transaction for object storage + etcd.

## Design

### 1. Result JSON: dual-mode `BackfillSegment`

`BackfillSegment` (`internal/datacoord/backfill_result.go`) supports two V3
forms selected in this order: `IsV2()` -> delta (ops present) -> legacy.

```go
type BackfillSegment struct {
    // legacy V3: pre-baked version + the base version it was built from
    Version        int64    `json:"version"`
    SourceVersion  int64    `json:"sourceVersion"`
    ManifestPaths  []string `json:"manifestPaths"` // retained for diagnostics
    // delta V3: operations, mutually exclusive with Version
    Ops            []BackfillManifestOp `json:"ops"`
    // V2 fields unchanged
    StorageVersion *int64                  `json:"storage_version,omitempty"`
    ColumnGroups   []BackfillV2ColumnGroup `json:"column_groups,omitempty"`
}

type BackfillManifestOp struct {
    Type     string               // "add" | "replace"
    Columns  []string             // Milvus field IDs as column names
    Format   string               // "parquet"
    Files    []BackfillManifestFile // Path / StartIndex / EndIndex / Properties
    RowCount int64
}
```

`SourceVersion` exists **only for legacy compatibility**: it pins the base a
pre-baked version was built from. Delta results do not compare it.

### 2. Delta commit: ops -> structured manifest mutation

Each op maps into `packed.ManifestUpdates`
(`internal/storagev2/packed/manifest_commit.go`) and is committed as a
`ManifestMutationCommitUpdates`:

| op | `packed.ManifestUpdates` mapping | loon action |
|---|---|---|
| `add` | `ColumnGroups` (single-field groups) | `loon_transaction_add_column_group` |
| `replace` | `DropColumns([target field IDs])` + `ColumnGroups` | `loon_transaction_drop_column` then add |

`replace` is the atomic per-column overwrite the Spark job already performs
today (`MilvusLoonWriter.scala`, `addfield` commit type: `dropColumn(fieldId)`
then `addColumnGroups` in one transaction). The pinned milvus-storage already
exposes `loon_transaction_drop_column`
(`internal/core/output/include/milvus-storage/ffi_c.h`), which removes the
column from every column group, deletes empty groups, and auto-drops that
column's indexes; it is a no-op when the column is absent (idempotent). The C++
transaction applies drops before add-column-group validation, so drop+add in
one `CommitSegmentManifests` revision is atomic.

**Required FFI gap:** `packed.ManifestUpdates` does not expose `DropColumns`
today (the C FFI exists; the Go layer never calls it). The design adds
`DropColumns []string` to `ManifestUpdates` and stages it via
`loon_transaction_drop_column` in `applyManifestUpdates`, **before** the
column-group adds, matching the C++ ordering contract.

Column names are Milvus field-ID strings — the same convention the Spark job
and `ColumnGroupEntry.Columns` use — so `DropColumns` and the added group
reference the same identity.

Every op file path is validated to be an **absolute** path under the target
segment's `<base>/_data/` directory (prefix + `path.Clean(p) == p`): milvus-storage
only relativizes column-group files under `<base>/_data/` (`ToRelative` with
`kDataPath`), so a bare `<base>/x` would be read back from
`<base>/_data/<base>/x`, and a `..` traversal would normalize to another
segment's files and never get collected. This mirrors the V2 path normalization
(which rebuilds paths from the segment ID) for the delta form.

### 3. CommitBackfillResult: no broadcast, direct framework call

`CommitBackfillResult` (`services_commit_backfill.go`) drops all broadcast
machinery (`broadcastBackfillBatch`, WAL control channel,
`StartBroadcastWithResourceKeys`, `maxItemsPerBroadcast` batching semantics are
kept only as a local batch size for the framework call) and applies commits
synchronously:

```text
CommitBackfillResult
  health check
  loadBackfillResult
  classifyBackfillSegments
    V2       -> UpdateSegmentColumnGroupsOperator
    V3 delta -> SegmentManifestCommit{ ManifestMutationCommitUpdates, Updates: opsToManifestUpdates }
    V3 legacy-> SegmentManifestCommit{ ManifestMutationNoop,
                 ManifestPath: Marshal(base, Version),
                 ExpectedManifest: Marshal(base, SourceVersion) }
  describe collection + checkBackfillSchemaVersion (fast-fail)
  for each batch:
    checkBackfillSchemaVersion (re-check; narrows the DDL window)
    V2 batch  -> meta.UpdateSegmentsInfo(operators...)
    V3 batch  -> meta.CommitSegmentManifests(commits...)
    record per-segment status
  aggregate CommitBackfillResultResponse
```

- **Delta mode** sets no `ExpectedManifest`: the revision is generated from the
  pointer current under the per-segment manifest lock and rebases onto any
  concurrent stats/index commit, exactly like the schema-bump materialization
  path (`compaction_task_bump_schema_version.go`).
- **Legacy mode** pins `ExpectedManifest` to `Marshal(base, SourceVersion)`.
  `CommitSegmentManifests` enforces the CAS inside the manifest lock
  (`prepareSegmentManifest` / `publishSegmentManifestOperator`), closing the
  TOCTOU between the pre-check and lock acquisition that a bare
  `UpdateManifestVersion` cannot. `SourceVersion == 0` skips the fence
  (best-effort, preserving today's behavior for results produced before the
  field existed).
- **Replace routing:** every delta commit whose `DropColumns` is non-empty is
  dispatched through the per-segment `CommitSegmentManifest` (the single-commit
  path), never the batch. The batch publishes segment records only and cannot
  stage the SegmentIndex removal that a column drop requires; the single-commit
  path always derives the index-removal set from the commit's `DropColumns`
  against the CURRENT index definitions (an index created during the commit
  window is still caught) and retracts the records atomically with the new
  pointer.
- `CommitSegmentManifests` is one catalog transaction per batch
  (all-or-nothing per batch, dropped/unhealthy segments skipped benignly),
  preserving the batch atomicity the broadcast ack callback deliberately kept.
  Both V2 and V3 dispatch use bounded batches of `maxBackfillCommitBatch`
  (512), so a large result cannot hold `segMu` (V2) or the manifest locks (V3)
  for the whole commit; per-segment manifest reads and single-segment commits
  run under an `errgroup.SetLimit`-bounded worker pool inside the RPC (no
  goroutine per segment) instead of serially.
- **Lock hygiene in the single-segment commit:** the backfill index-removal set
  is re-resolved from current definitions AFTER manifest I/O but BEFORE taking
  `segMu`, and `fieldIndexLock`'s read side is held from that re-resolution
  THROUGH the publication section (released after `segMu`, before the deferred
  gauge closures that re-enter it). This makes the final check and the pointer
  advance atomic with index creation: a `CreateIndex` for a dropped column either
  completed before the re-resolution (its index ID is in the removal set and its
  records are caught by `verifyNoUnexpectedIndexRecords`) or is blocked until the
  replace publishes (its build then reads the new manifest, so no index built
  from the pre-replace column values can be accepted). The read side is taken
  before `segMu` — never acquired inside it — so a replace commit overlapping a
  `CreateIndex`/`DropIndex` (which holds that lock's write side across an etcd
  round trip) cannot stall every segment reader behind `segMu`.
- **DataView reconciliation:** after any V3 commit succeeds, the handler calls
  `meta.recomputeDataView` (matching the old ack callback and every other
  manifest-commit caller); V2-only results skip it because V2 upserts do not
  touch the manifest.
- The schema-version fence is retained as a fast-fail and re-checked per batch.
  Without the broadcast resource-key serialization, a collection DDL can commit
  in the window between the fence check and the manifest commit; this is
  narrower than the previous async-ack window and is accepted (a collection
  scoped in-process lock is optional hardening, not a requirement).

### 4. Global cluster convention: primary and standby each commit

**Convention (fixed).** In a global cluster where the primary and standby share
object storage but maintain independent etcd metadata, the Spark-produced
result JSON on the shared object storage is committed by **each** cluster's own
DataCoord — the primary commits to the primary's metadata, the standby commits
to the standby's metadata. The commit is executed separately on both clusters
and must be safe under both executions.

This convention is what makes "no broadcast" mandatory rather than merely
preferable: the broadcast/WAL pipeline is per-cluster and cannot express a
commit applied independently to two clusters' metadata. The direct framework
call is a self-contained catalog + manifest operation that runs identically on
either node.

Safety of the second (standby) execution follows from the per-cluster state:

- Delta mode is idempotent: an op whose target column already exists in the
  manifest with exactly the op's file descriptors is recognized as already
  applied and skipped (`add` keyed by column, `replace` keyed by its exact
  replacement group), so a replay on a manifest that already carries the group
  commits nothing. The manifest lock serializes each cluster's own writers.
- Legacy mode is fenced by `SourceVersion`: if the standby's manifest already
  advanced past the source (its own stats/index commits, or metadata replicated
  from the primary), the commit is rejected as stale instead of overwriting.
- Validation (`storage_version`, state, collection/partition membership,
  schema-version fence, and each delta file path lying under the target
  segment's manifest base) is evaluated against the local cluster's metadata.
- **Backfillable-field gate:** every op column (and V2 group field ID) must be a
  live user field that is not the primary key, the partition key, a system
  field, a function output, or the dynamic field — the producer-side "ordinary
  writable collection fields" contract, enforced at the receiver. The partition
  key is a structural rejection: a replace would leave rows in a partition that
  no longer matches their key (re-enabling it needs row re-routing, not a check
  change). The dynamic field (`$meta`) is storage-equivalent to an ordinary JSON
  column, so its rejection is a contract gate rather than a structural one; it
  can be re-enabled by dropping `GetIsDynamic()` from `backfillOpColumnFieldIDs`
  once the producer supports the dynamic-JSON-equivalent semantics.

No cross-cluster coordination, no once-only marker, and no assumption about
which cluster commits first.

## Failure and Retry Semantics

- A failed `CommitSegmentManifests` batch leaves nothing published (the
  prepared revisions are orphaned and invisible); the caller retries on the
  current pointer.
- Replaying a committed delta is idempotent: an op whose target column already
  exists in the manifest with exactly the op's file descriptors is recognized
  as already applied and skipped (`add` keyed by column, `replace` keyed by its
  exact replacement group), so a retry after a lost response neither republishes
  a new revision nor (for an indexed field) re-retracts the rebuilt
  SegmentIndex records. Legacy replay is rejected by the `SourceVersion` CAS
  once the first commit advanced the pointer.
- Two distinct replace results on the same column are ordered by the manifest
  lock, not by version: a replace carries no source-version comparison, so an
  older replace result retried after a newer one was applied re-runs its
  drop+re-add and wins over the newer content. This is accepted: a retried
  result with *identical* files is the only replay case and is skipped; a
  different-file retry is treated as a fresh replace. Producers must not
  re-submit stale replace results against a segment they did not overwrite.
- `CommitSegmentManifests` skips dropped/unhealthy segments as a benign outcome;
  such segments surface in `SegmentStatuses` via the pre-validation diagnostics.

## Compatibility

- V2 column-group results: unchanged behavior, now applied directly.
- V3 legacy results (`version` present, no `ops`): continue to work; results
  carrying `sourceVersion` get the new stale-base rejection. Results without it
  (`sourceVersion == 0`) keep today's best-effort semantics.
- V3 delta results (`ops` present): new commit path.
- Wire/proto: **no changes.** The ops travel only inside the result JSON
  (Go-decoded) and never cross a message bus; `BatchUpdateManifestItem` and
  `messages.proto` are untouched because backfill no longer broadcasts.
- `UpdateManifestVersion`, `ddl_callbacks_batch_update_manifest.go`, and the
  public `BatchUpdateManifest` RPC remain for the batch-DDL adoption path.
- **Receiver-capability gate (open rollout requirement, producer-side):** a
  delta result (non-empty `ops`, no pre-baked `version`) is consumed only by
  DataCoord builds that decode `BackfillSegment.Ops`. An older DataCoord
  ignores the field, sees `Version == 0`, and rejects the segment with
  "missing or invalid manifest version"; the ops are never applied and retrying
  the same result cannot recover it. Spark-milvus MUST gate delta output on an
  explicit receiver-capability signal and keep emitting the legacy form for old
  coordinators; both receivers in a primary/standby pair must support the
  selected form. This PR does not change the Spark producer.

## Verification Plan

- `services_test.go` backfill cases reworked: legacy `version`-monotonic checks
  become `sourceVersion`-CAS checks; delta success path (mock
  `meta.CommitSegmentManifests` / `packed.CommitManifestUpdates`).
- New: legacy `SourceVersion != currentVer` rejection; delta `add` and
  `replace` (drop+add) success; mixed delta/legacy batch; lock-internal CAS
  race (version advanced between pre-check and lock acquisition -> stale);
  idempotent replay; primary/standby independent-commit scenario (same result
  applied to two independent meta states).
- FFI: `packed` `DropColumns` unit test against
  `loon_transaction_drop_column` (drop of existing/absent column, empty-group
  removal, index auto-drop).
- Regression: `go test -tags dynamic,test -gcflags="all=-N -l" -count=1
  ./internal/datacoord/...` and the `packed` module tests.

## Risks / Open Questions

1. `DropColumns` exposes `loon_transaction_drop_column` in the Go layer for the
   first time; the exact behavior of drop on a column group that is also being
   re-added in the same transaction is pinned to the current milvus-storage
   revision (drops apply before add validation) and must be covered by a test.
2. The schema-version fence loses the broadcast resource-key serialization; the
   residual DDL window is documented and optionally hardened with a
   collection-scoped in-process lock.
3. Per-segment status granularity degrades to batch-level for V3 (one
   `CommitSegmentManifests` error fails its batch); acceptable under the
   existing response contract.
4. A backfill replace retires the segment's in-memory `TextStatsLogs` /
   `JsonKeyStats` entries atomically with the pointer, but `drop_column` leaves
   the manifest's `text_index.<fid>` / `json_stats.<fid>` entries in place (V3
   readers resolve stats from the manifest), and a text/JSON stats task already
   in flight on the old column can rebase its old-data result onto the new
   revision. Fully closing this needs a drop-stat capability in milvus-storage;
   tracked with the segment-change-group feature ([issue #53321]) and out of
   scope for this PR.

[issue #53321]: https://github.com/milvus-io/milvus/issues/53321
