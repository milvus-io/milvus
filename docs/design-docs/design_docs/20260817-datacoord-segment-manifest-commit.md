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
- copy/restore has the worker write the target's index entries into the
  manifest it produces, then publishes that pointer with `UpdateManifest`;
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

- `CommitUpdates` — construct the next revision from the segment's currently
  published pointer and a structured `packed.ManifestUpdates` payload: new data
  files, delta logs, stats, and index add/drop entries;
- `Noop` — publish a prepared manifest path unchanged, only as a temporary
  compatibility adapter.  It must validate the base and is removed once every
  producer returns structured entries.

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
segmentManifestLock(segmentID) -> indexMeta.keyLock(buildID) -> segMu
```

`segMu` is never held during object-storage I/O, and the BuildID key lock is
acquired only after the manifest I/O completes, immediately before the final
`segMu` publication (the pre-I/O `segMu` snapshot is a read lock released
before any I/O or key-lock acquisition). Every other `SegmentIndex` writer
takes `keyLock` and only then reads segment state under `segMu`, so a commit
must take the key lock before - never inside or after - its final `segMu`
section. No code may take a BuildID lock first and
then attempt a segment manifest commit.  Multi-segment operations sort segment
IDs before locking.  Where possible, compaction creates independent target
segments rather than committing two segments under one lock.

One known bypass predates this design and is not closed by it: backfill
adoption and batch DDL advance `manifest_path` through `UpdateManifestVersion`,
outside `segmentManifestLocks`, guarded only by version monotonicity. A
revision built from base N and adopted after a commit published N+1 wins on
version alone, and the storage layer's overwrite resolver does not merge, so
the newer revision's content is silently dropped - index entries included, now
that manifests carry them. Until adoption carries the base version the revision
was built from and rejects a stale one (follow-up), no index build or GC
retraction may overlap a backfill window on the same segment.

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

Drop keeps the same ordering - bytes first, then the metadata naming them:

```text
resolve delete list (meta + manifest)
  -> delete index objects
  -> commit manifest without index + remove SegmentIndex (one catalog transaction)
```

The usual argument for retiring metadata first - "bytes gone but metadata still
claims them is a read failure" - does not apply here. `recycleUnusedSegIndexes`
reaches this path only when the index definition no longer exists or the segment
itself is gone, and only for a task in a terminal state. Nothing consumes the
artifact, so a window where metadata still names deleted files harms no reader,
and the next GC cycle simply repeats the whole step; deleting already-deleted
files is a no-op.

Retiring the metadata first is what would be unsafe. Once the `SegmentIndex`
record is removed, a failed deletion leaks bytes that nothing can re-drive:
`recycleUnusedIndexFilesV1` is meta-driven through `GetDeletedIndexesWithV1Path`,
so a COLLECTION_ROOTED artifact leaks permanently. Only the BUILD_ROOTED layout
would be reclaimed, by `recycleUnusedIndexFilesV0`'s orphan-buildID sweep - and
that is the older layout, so the leak would grow rather than shrink over time.

What the framework still contributes is atomicity between the two metadata
stores: the manifest revision and the `SegmentIndex` removal retire in one
catalog transaction, so they can never disagree about whether the index exists.
Only their ordering relative to the bytes is what changed. Object deletion
remains non-atomic with either, which is why it runs first, where a failure
costs a retry rather than an unreclaimable leak.

## Retiring the etcd SegmentIndex Record

The end state of this migration is that a manifest is the only durable record
of a finished index artifact. `dataCoord.index.writeSegmentIndexToManifest`
(default off) is the seam to that end state. The two stores are **exclusive,
never dual-written**: off is the pure legacy path - every `SegmentIndex`
record goes to etcd and no manifest index entry is produced at all
(`publishIndexToManifest` declines before touching a manifest, and the copy
path ships the worker no target index definitions, so it retracts inherited
entries and writes none); on publishes the record into the segment manifest
and skips the `SegmentIndex` catalog writes - `CreateSegmentIndex`,
`AlterSegmentIndexes`, and the upsert action staged into a manifest commit -
while leaving DataCoord's in-memory index state untouched. Where durable state
lives is what the switch controls; what DataCoord itself observes is not.

The suppression is decided **per segment, not globally**: it requires both the
switch to be on and the segment to be manifest-backed (StorageV3 with a
manifest path). A StorageV1/V2 segment records its indexes nowhere else, so
skipping its write would destroy the only copy rather than defer to another
one, and it keeps persisting regardless of the switch. `indexMeta` answers that
question through `manifestBackedSegment`, wired by `newMeta` once the segment
list exists; a batch that mixes both kinds persists only the records that need
it. Removals are likewise never skipped: the switch stops DataCoord adding
`SegmentIndex` rows to etcd, it never stops it removing them, or a row written
while records still went to etcd would be resurrected by reload after GC had
already collected it.

**The sticky segment marker is what makes the switch safe to flip in both
directions.** `SegmentInfo.manifest_has_index` (field 40) records that a
segment's manifest has carried at least one index entry. It is written in the
same catalog transaction as the entry itself, through exactly three channels:
the single-segment `CommitSegmentManifest` and the batch
`CommitSegmentManifests` fill a framework-owned
`SegmentCatalogMutation.setManifestHasIndex` from the mutation's actual
`Updates.Indexes` (a Noop mutation never sets it - the framework cannot vouch
for a revision it did not build), and the copy sync path - the one publication
that adopts a worker-built manifest outside the framework - appends the
`UpdateManifestHasIndex` operator in the same `UpdateSegmentsInfo` transaction
as the pointer, after the read-back proves the manifest carries entries. The
marker is set-only: retraction never clears it, because a wrong false silently
drops indexes at the next reload while a stale true costs exactly one extra
manifest read at boot.

Reload closes the loop, and is **driven by the marker, not by the switch**.
`meta.reloadSegmentIndexesFromManifests` runs unconditionally at every boot: it
loads every etcd record first (that path never changed), then reads the
manifest of each healthy, non-L0, marked StorageV3 segment and projects the
entries etcd does not already have into `SegmentIndex` records, with etcd
winning on buildID conflict - the same precedence every other manifest-index
consumer uses. Recovery is decided by where the records durably are, never by
where the current configuration would put them. Flipping the switch in either
direction therefore strands nothing: records published while it was on stay
visible after it goes off, and existing etcd rows stay authoritative when it
comes on. A cluster whose segments carry no marker reads nothing, so the
default startup performs zero object-storage reads.

**A marked manifest that cannot be read fails startup.** This is the same
fail-closed contract the etcd path has - a `ListSegmentIndexes` error aborts
`newIndexMeta` and therefore `newMeta` - and the marker is exactly what scopes
it: only a segment proven to carry entries can abort boot. Skipping an
unreadable marked manifest would leave `indexMeta` silently incomplete, and an
incomplete `indexMeta` is not merely "that segment looks unindexed": GC reads
an absent `SegmentIndex` as proof the artifact is garbage.
`recycleUnusedIndexFilesV0`'s `CheckCleanSegmentIndex` miss path removes the
entire buildID prefix with no time tolerance, and the default
`storePathVersion: 0` places index files under exactly the `index_files/`
prefix it walks - so one transient object-store error would delete live index
files the manifest still references. Failing to start is recoverable; that is
not. Startup logs scanned / read / recovered counts, because otherwise a
partial reload is indistinguishable from a cluster that has no indexes.

GC applies the same marker discipline: `resolveManifestIndexRetraction` skips
the manifest read when neither the segment marker nor the record's
`ManifestPublished` flag claims an entry (nothing to retract, so the record is
simply removed), and `getManifestIndexFiles` skips unmarked segments entirely,
so a dropped V3 segment on an all-etcd cluster can never have its recycling
blocked by an unreadable manifest that cannot name index files anyway - the
record-driven side of the sweep covers the same files independently.

The candidate set is deliberately NOT narrowed by which index definitions are
still live. Skipping a segment whose definitions all have records would strand
precisely the entry the system still needs: a manifest entry whose definition is
already dropped has no `SegmentIndex` record by construction, and GC is entirely
record-driven (`GetAllSegIndexes`, `GetDeletedIndexesWithV1Path`), so an entry
with no record is never visited again and its bytes leak for the
COLLECTION_ROOTED layout. The filter is therefore only: healthy, non-L0,
StorageV3, marked.

Every entry read is validated with the same predicate the other manifest
consumers use (`manifestIndexFilePathInfo`) before it becomes a record, because
the reload is the one path that promotes a manifest entry into a record whose
file keys later reach `removeObjectFiles`. A malformed entry fails startup for
the same reason an unreadable manifest does.

The cost is one object read per *marked* segment at startup. That fan-out is
pooled at `dataCoord.index.segmentIndexManifestLoadConcurrency` (default 4096)
rather than `metastore.readConcurrency`: the latter is shared with the
querycoord and rootcoord catalogs and defaults to 32, which is a sane etcd
fan-out and a badly wrong one for object-storage GETs. At 32 in flight a
million-segment cluster would serialize its boot into hours, and since the scan
is fail-closed inside `newMeta` that time is downtime, not background warmup.
Each slot holds a cgo call for the duration of one GET and so pins an OS thread,
which is the ceiling the knob trades against; a throttling object store is the
reason to lower it.

**Lock order.** Gating the etcd write requires knowing whether a segment is
manifest-backed, which resolves through `meta` under `segMu`. Every
`SegmentIndex` writer reaches that gate while holding `indexMeta.keyLock`, so
the global order is `segmentManifestLocks -> indexMeta.keyLock -> segMu`.
`CommitSegmentManifest` therefore acquires the build's key lock before its
first `segMu` acquisition and holds it through publication; acquiring it during
staging, after `segMu`, inverts the order and deadlocks against a concurrent
transition of the same build. `TestCommitSegmentManifestTakesKeyLockBeforeSegMu`
pins this: parked on the key lock, the commit must hold no `segMu`.

Three limits are inherent rather than incidental, and are why the switch is
off-by-default rather than removed:

- Only StorageV3 segments record indexes in a manifest, which is why the switch
  cannot apply to StorageV1/V2 segments at all - the etcd record stays their
  sole copy, and retiring it needs a different mechanism.
- A `SegmentIndex` is also the build *task* record. Its state machine, assigned
  node, failure reason and timestamps have no manifest home, so with the switch
  on, an in-flight or failed build on a manifest-backed segment is memory-only:
  it is lost across a restart and simply reissued. A recovered record is
  `Finished` by construction. The cost is concrete: a deterministically failing
  build re-runs in full after every restart, and its `FailReason` is never
  visible to an operator who did not read the logs before the restart. A
  fake-finished build (too small to train) is likewise not persisted, though
  rebuilding it is free. The same applies to the publish declines in
  `publishIndexToManifest` (dropped index definition, unhealthy segment): the
  legacy `FinishTask` fallback's etcd write is gated too, so those records are
  memory-only and die with their segment or get reissued.
- **The one-way boundary is the binary version, not the config.** While the
  switch stays false no manifest index entry exists anywhere and downgrading
  the DataCoord binary is free. Once it has been on and a build completed,
  rolling back to a binary without marker-driven reload makes the
  manifest-only records invisible, and `recycleUnusedIndexFilesV0`'s miss path
  then deletes their index files as orphans (no time tolerance; under the
  default `storePathVersion: 0` that prefix is exactly where live index files
  land). The boundary is documented, not enforced.

A version-skew guard protects the one cross-binary interaction: with the
switch on, a copy executed by a DataNode that predates manifest index
republication returns a manifest whose index section was never rewritten. The
synced records would be memory-only (their etcd write is gated) and silently
vanish on restart, so `syncVectorScalarIndexes` hard-fails the copy task with
an upgrade hint instead of installing them.

One invariant carries the whole scheme: **a visible StorageV3 segment always
has a non-empty `ManifestPath`.** It holds by construction - `AllocSegment`
mints the pointer in the same `SegmentInfo` literal that sets `StorageVersion`,
and flush, compaction, import and copy all publish `ManifestPath` atomically
with the segment record; there is no in-place V1/V2 to V3 conversion. Nothing
validated it, however, and a violation is silent: the segment would answer "not
manifest-backed", keep writing to etcd, and quietly exempt itself from the
migration. Both the gate and the reload filter now log an error when they see
a visible V3 segment with an empty pointer.

The consumers that decide *whether a segment is indexed* -
`indexMeta.GetSegmentIndexes` / `GetIndexedSegments`, read by the index
inspector, the compaction trigger and segment load - answer from in-memory
records alone, which is the reason the reload exists at all. No DataCoord read
path falls back to the manifest: the reload makes the in-memory records
complete before the server serves, and every publication installs its record in
memory in the same commit, so an absent record means an absent artifact rather
than a record kept elsewhere. Manifest index entries are read only by the
reload, by GC when it retracts one, and by the copy worker.

## Required Caller Migration

| Current owner/path | Framework migration |
|---|---|
| `task_index.go` / DataNode index task | Return index artifact metadata.  `meta.CommitSegmentManifest(AddIndexes)` creates the revision and, via a `SegmentIndexUpsert` catalog mutation, atomically completes `SegmentIndex`. |
| `garbage_collector.go` | Delete the objects first, then use `DropIndexes` with a `SegmentIndexRemove` catalog mutation so the retracting revision's pointer and the `SegmentIndex` removal land in one catalog transaction - the same bytes-first ordering as the legacy path (see Failure and Recovery Semantics). A StorageV2 segment, a dropped segment, or a manifest that never carried the entry follows the same file deletion with a bare `RemoveSegmentIndex` instead of a manifest commit. |
| `copy_segment_task.go`, `import_task_import.go` / restore | A copy or import target is a fresh, exclusively owned segment whose worker returns a complete manifest pointer, so DataCoord publishes that first pointer inline via `UpdateManifest`. No `CommitSegmentManifest` serialization is needed for a segment no other writer touches. Index entries are part of "complete": the copied manifest inherits the source's entries, whose stored paths point outside the segment base and therefore encode the source IDs, so the copy worker retracts them and records the target's own entries in the same transaction. DataCoord supplies the identity it owns (reallocated index IDs, the inherited entry IDs to retract) in the copy request rather than creating a revision of its own. |
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
3. Migrate index completion end-to-end: no worker-side index manifest
   publication and no change to the DataNode result contract - DataCoord builds
   the `ManifestIndexInfo` from the collection schema, the index definition, and
   the index task record, then atomically publishes `SegmentInfo` plus
   `SegmentIndex`.  Implemented; see
   [StorageV3 Manifest Index Publication](20260811-storagev3-manifest-index-publication.md).
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
3. keep `SegmentIndex.IndexFileKeys` as the read path's only file-list source -
   no manifest fallback is needed, since a record without keys is a
   fake-finished build, which publishes no manifest entry either; and
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
