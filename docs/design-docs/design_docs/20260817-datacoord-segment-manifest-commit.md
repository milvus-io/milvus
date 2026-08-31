# DataCoord Segment-Scoped Manifest Commit Framework

- **Created:** 2026-08-17
- **Status:** Draft
- **Component:** DataCoord / StorageV3
- **Related work:** StorageV3 manifest index metadata migration

## Summary

StorageV3 uses an immutable, versioned manifest as the source of truth for a
segment's files.  `SegmentInfo.manifest_path` is the durable pointer that makes
one manifest revision visible to the rest of Milvus.

This design introduces one DataCoord-owned commit framework for this pair of
operations.  For a given segment, it serializes the full sequence:

1. read the currently published `SegmentInfo` and manifest;
2. construct and commit the next manifest revision;
3. atomically persist the new `manifest_path` together with the associated
   `SegmentInfo` and/or `SegmentIndex` change; and
4. update the in-memory metadata only after the catalog write succeeds.

The serialization key is `segmentID`.  Different segments remain concurrent.
No caller outside the framework may create a manifest revision that is intended
to update an existing segment, or directly replace that segment's manifest
pointer in etcd.

The framework must be completed on a clean branch based on `master`.  The
ongoing manifest-index migration is intentionally **not** its implementation
branch: after this framework is complete, that work will be rebased/adapted to
use the framework rather than retain its local publication logic.

## Problem

Today the lifecycle is split across several owners:

- an index worker can append its index entry to a manifest and later report the
  resulting path to DataCoord;
- DataCoord GC removes index entries from a manifest, then separately publishes
  a new path and removes files/`SegmentIndex` metadata;
- copy/restore appends copied index entries before a later `UpdateManifest`;
- stats, flush/import, and compaction paths publish manifests through several
  forms of `UpdateManifest`.

`meta.segMu` protects the in-process execution of `UpdateSegmentsInfo`, but it
does not cover the object-storage transaction that created the manifest
revision.  Therefore it is not a segment commit lock.  Two paths can observe
one pointer, create revisions independently, and later publish their results
out of order.  A later etcd write can then point backward to an older revision
or publish a revision that omits a completed change.

The existing `indexMeta.keyLock(BuildID)` has a different responsibility: it
serializes lifecycle changes for one `SegmentIndex` job.  It cannot serialize a
manifest shared by all indexes and stats of a segment.  Conversely, a global
`segMu` held across object storage would serialize unrelated segments and make
network I/O block all metadata updates.

## Goals

1. A single segment has exactly one in-process manifest commit at a time.
2. The segment lock covers manifest revision creation **and** catalog
   publication, not only the etcd write.
3. DataNode writes physical data, index, and stats files, but DataCoord owns the
   commit-time manifest transaction and the etcd publication of the result.
4. A successful visible commit updates all metadata that describes that visible
   manifest in one catalog transaction.
5. Failure is retryable and never publishes a manifest pointer before its
   manifest exists.
6. The framework serializes manifest writes only where *concurrent* writers can
   target the same segment: the post-flush async jobs — stats sort, index build,
   GC, compaction, batch DDL — that operate on an already-flushed segment.
   Single-writer manifest writes are published inline via `UpdateManifest`
   without the keyed lock: the flush of a growing or L0 segment
   (`SaveBinlogPaths`, serialized by the segment's single WAL owner) and the
   finalization of a fresh copy or import target. `UpdateManifest` therefore
   carries no StorageV3 guard; the concurrent paths route through
   `CommitSegmentManifest` by construction because they build a new revision
   rather than record a pre-built pointer.

## Non-goals

- A distributed transaction between object storage and etcd.  It is not
  available and must not be simulated with protobuf-value CAS.
- A distributed per-segment lock across multiple active DataCoord leaders.
  Milvus leadership already permits only the active DataCoord to mutate catalog
  metadata.  Manifest transaction conflict handling remains the protection for
  leader handoff, retries, and unexpected external writers.
- Serializing all segment metadata updates.  Non-manifest updates can continue
  to use `UpdateSegmentsInfo`; only the manifest commit protocol is serialized
  by this new keyed lock.
- Changing the manifest format or the storage transaction ABI beyond the
  structured mutations needed by this framework.

## Ownership Model

```text
DataNode / compactor
  writes immutable artifact files
  returns a structured manifest delta and its expected input manifest
                      |
                      v
DataCoord meta.CommitSegmentManifest(segmentID, request)
  segment-scoped lock
    -> read current SegmentInfo
    -> validate expected base / segment state
    -> execute packed manifest transaction
    -> catalog transaction: SegmentInfo + SegmentIndex/task state
    -> install in-memory result
                      |
                      v
Visible SegmentInfo.manifest_path
```

Where DataCoord builds the revision, the worker must not return a pre-published
manifest revision.  For example, an index task returns its index files and index
metadata, not the result of `AddIndexInfoToManifest`; DataCoord converts it to a
`ManifestIndexInfo` while holding the segment commit lock and invokes the packed
transaction itself.  This holds for every concurrent post-flush path — stats
sort, index build, GC index removal, compaction, batch DDL — because each
targets an already-flushed segment that the others may be advancing at the same
time and so must serialize.  These paths reach the manifest only through
`CommitSegmentManifest`; they never call `UpdateManifest`.

Single-writer manifest writes do not serialize and are published inline via
`UpdateManifest`:

- The flush of a growing or L0 segment (`SaveBinlogPaths`).  A growing segment is
  created *already holding* a `ManifestEarliest` revision (`segment_manager.go`)
  and its manifest is advanced by every sync, but those syncs come from the one
  WAL owner of the segment's VChannel and are applied sequentially — there is no
  concurrent writer.  Stale retries and cross-node handoff are fenced by the
  channel-owner check in `SaveBinlogPaths`, and a re-sent identical pointer is a
  no-op, so no base-match CAS is required.  The flusher returns a complete
  manifest pointer, which DataCoord records directly.
- The finalization of a fresh copy or import target.  It is pre-registered with
  an **empty** manifest path (`snapshot_manager.go`, `import_util.go`) and stays
  `Importing` — invisible to stats/index/compaction, which gate on
  `Flushed`/`Flushing` — until a single `Importing -> Flushed` finalization.  Its
  worker returns a complete manifest pointer, DataCoord does no manifest I/O, and
  no other writer touches it before publication.

Because these paths have no concurrent writer, `UpdateManifest` carries no
StorageV3 guard.  A producer may write data files, but a job that publishes into
the concurrent post-flush window does not select a visible manifest revision
itself; it hands DataCoord the structured entries and DataCoord commits the
revision under the lock.

## API Shape

`meta` owns a keyed lock, initialized with the rest of metadata state:

```go
segmentManifestLocks *lock.KeyLock[int64]
```

The exported surface should use typed requests rather than an arbitrary callback
that can do hidden I/O or re-enter `meta`:

```go
type SegmentManifestCommit struct {
    SegmentID        int64
    ExpectedManifest string // empty only for initial-manifest creation
    Mutation         ManifestMutation
    CatalogMutation  SegmentManifestCatalogMutation
}

func (m *meta) CommitSegmentManifest(
    ctx context.Context,
    commit SegmentManifestCommit,
) error
```

`ManifestMutation` is a closed/typed set, initially including:

- `CreateManifest` — construct the first revision from worker-supplied
  structured entries;
- `AddIndexes` and `DropIndexes`;
- `AddStats`;
- `AppendData` / `ReplaceData` as needed by flush and compaction;
- `PublishPreparedManifest` only as a temporary compatibility adapter.  It must
  validate the base and is removed once every producer returns structured
  entries.

`CatalogMutation` describes the metadata that must become visible with the
manifest pointer.  Examples are completing one `SegmentIndex`, creating copied
target `SegmentIndex` records, updating segment statistics, or changing a
segment state.  It must produce catalog actions, not perform an independent
catalog write.

## Commit Protocol and Lock Order

For one segment, the protocol is:

1. Lock `segmentManifestLocks[segmentID]`.
2. Briefly take `segMu`, clone the current `SegmentInfo`, and release `segMu`.
   Validate segment existence, StorageV3, health/state, and
   `ExpectedManifest` where the operation depends on a specific input.
3. Execute the `packed` transaction using the cloned/current manifest.  The
   packed resolver is `OVERWRITE`: under the segment lock there is no competing
   local writer, while the resolver gives a deterministic latest-manifest
   rebase for retry/leader-handoff races.
4. Reacquire `segMu`, reload the latest `SegmentInfo`, and revalidate segment
   health and `ExpectedManifest`. Apply the new pointer and catalog mutation to
   that latest clone, preserving unrelated ordinary metadata updates that ran
   during manifest I/O.
5. While still holding `segMu`, execute one `catalog.Update` / catalog
   transaction containing the changed `SegmentInfo` and all associated
   `SegmentIndex` records. This matches the existing full-record
   `UpdateSegmentsInfo` consistency model: final catalog publications are
   serialized, while the slower manifest I/O for different segments remains
   concurrent. If the catalog write fails, do not change memory.
6. Install the cloned metadata in memory, then release `segMu` and the segment
   lock.

The lock ordering is always:

```text
segmentManifestLock(segmentID) -> segMu -> indexMeta.keyLock(buildID)
```

`segMu` is never held during object-storage I/O.  A path requiring both segment
and index state follows this order; no code may take a BuildID lock first and
then attempt a segment manifest commit.  Multi-segment operations sort segment
IDs before locking.  Where possible, compaction creates independent target
segments rather than committing two segments under one lock.

## Failure and Recovery Semantics

Object storage and etcd cannot commit atomically.  The ordering is deliberately
one-way:

```text
artifact files -> manifest revision -> etcd/catalog pointer -> memory
```

- If artifact generation fails, no manifest or etcd change is made.
- If manifest creation succeeds but catalog publication fails, the revision is
  orphaned and invisible.  Retrying starts from the still-published pointer;
  orphan cleanup is safe because no `SegmentInfo` references it.
- A retry must be idempotent at the logical mutation level.  Adding an index
  must not create duplicate logical index entries; dropping a deleted logical
  index may remove every matching historical entry.
- A stale expected base or changed segment state is a retry/discard result, not
  an attempt to overwrite the current pointer. Exact `ExpectedManifest`
  conflicts retain a typed retriable error plus an in-process stale marker, so
  task-specific consumers such as Stats can discard obsolete worker output
  without classifying unrelated service-unavailable failures as stale.

Drop requires a durable cleanup state in addition to serialization:

```text
commit manifest without index + mark index cleanup pending
  -> delete index objects (retryable)
  -> remove SegmentIndex metadata / complete cleanup marker
```

The framework prevents an interleaved index/stat commit during these steps, but
it cannot make object deletion and etcd deletion atomic across a process crash.
The pending state makes the remaining cleanup discoverable and idempotent.

## Required Caller Migration

| Current owner/path | Framework migration |
|---|---|
| `task_index.go` / DataNode index task | Return index artifact metadata.  `meta.CommitSegmentManifest(AddIndexes)` creates the revision and atomically completes `SegmentIndex`. |
| `garbage_collector.go` | Use `DropIndexes`; publish pointer and cleanup intent in one catalog transaction, then perform retryable object cleanup. |
| `copy_segment_task.go`, `import_task_import.go` / restore | A copy or import target is a fresh, exclusively owned segment whose worker returns a complete manifest pointer, so DataCoord publishes that first pointer inline via `UpdateManifest`. No `CommitSegmentManifest` serialization is needed for a segment no other writer touches. |
| `task_stats.go` | Text, JSON, and sort stats all use `AddStats`; remove the bare sort `UpdateManifest` path. |
| flush / `SaveBinlogPaths` | No migration. Publishes inline via `UpdateManifest`. A growing or L0 segment is flushed by its single WAL owner and advanced sequentially, so its manifest write has no concurrent writer and needs no `CommitSegmentManifest` serialization even though the manifest advances from `ManifestEarliest`. |
| compaction | Generate output files in DataNode, return output manifest entries, then publish each output segment through the framework. |
| external collection refresh | Deferred from the segment-scoped migration. Keep its existing job-level `UpdateSegmentsInfo` publication until a collection-level generation boundary can atomically switch the complete refresh result and `external_source` / `external_spec`. |
| snapshot/restore and recovery | Read the published pointer normally; concurrent post-flush destination-manifest writes use the framework. |

`UpdateManifest` is the inline publication mechanism for the single-writer
manifest paths: all StorageV1/V2 writes, and the StorageV3 flush
(`SaveBinlogPaths`) and copy/import finalizations, none of which has a concurrent
writer. It carries no StorageV3 guard. The concurrent post-flush paths (stats,
index, GC, compaction, batch DDL) never call `UpdateManifest` — they build a
revision and advance the pointer through `CommitSegmentManifest`. A review-time
grep of every `UpdateManifest(` and every packed manifest mutation is a required
migration gate: any new `UpdateManifest` caller must be a single-writer path.

The current staged implementation intentionally does not migrate external
collection refresh by publishing each returned segment independently. Doing so
would turn one job-level refresh into a partially visible sequence and could
leave a failed job with only some new manifests published. External collection
refresh remains an explicit follow-up and the end-state acceptance criteria
below are not satisfied until that collection-level protocol is implemented.

## Implementation Plan in the Clean Worktree

1. Add the keyed lock and `CommitSegmentManifest` skeleton in `meta.go`, with
   lock-order documentation and focused concurrency tests.
2. Add typed packed mutation adapters and tests for add/drop/stats/create.  The
   storage transaction uses `OVERWRITE`; do not add a protobuf-serialized
   `SegmentInfo` value-equality CAS.
3. Migrate index completion end-to-end: update DataNode result contract, remove
   worker-side index manifest publication, and atomically publish
   `SegmentInfo` plus `SegmentIndex` from `meta`.
4. Migrate GC with the durable cleanup state and crash/retry tests.
5. Migrate stats, including the sort path.
6. Migrate compaction output publication to the framework. Leave flush
   (`SaveBinlogPaths`) and copy/import on inline `UpdateManifest` — they are
   single-writer — and keep `UpdateManifest` free of any StorageV3 guard.
7. Add observability: lock wait/hold duration, commit outcomes, stale-base
   rejections, orphan-manifest count, and cleanup retries.
8. Run the full affected DataCoord/DataNode test matrix in the Milvus builder
   container, with race/fault-injection tests covering etcd failure, stale
   inputs, concurrent index completion, index-vs-GC, stats-vs-index, and
   restart after each drop stage.

## Integration with the Existing Manifest-Index Work

The existing branch should not be incrementally expanded with this framework.
After the clean branch is complete:

1. rebase the manifest-index migration onto the framework branch;
2. replace its local `packed.AddIndexInfosToManifest`,
   `RemoveIndexInfosFromManifest`, and manifest-pointer publication logic with
   `CommitSegmentManifest` actions;
3. preserve the read fallback from legacy `SegmentIndex.IndexFileKeys` to
   manifest entries for migration compatibility; and
4. re-run the full lifecycle audit: build, load, query, copy/restore, snapshot,
   compaction, dropped-segment GC, and recovery.

This ordering avoids stabilizing two competing publication protocols in the
same release.

## Acceptance Criteria

1. There is no StorageV3 manifest mutation or pointer publication outside the
   DataCoord segment commit framework.
2. Two concurrent operations on the same segment cannot publish pointer
   revisions out of order.
3. Operations on different segments do not block one another on manifest I/O.
4. A catalog write failure never updates in-memory metadata and never exposes
   the orphan manifest revision.
5. GC after a crash converges without deleting files referenced by the current
   manifest.
6. Tests demonstrate each required race and failure case rather than only
   successful sequential execution.
