# Transform Start-After TimeTick

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @congqixia
- Design Review: 2026-07-29
- Design Update: 2026-10-09

Status: implemented; the dual-frontier contract below supersedes the
checkpoint-bounded producer. Focused unit and distributed-cluster regressions
cover publication, exact-version recovery, Delete/Upsert, and release/reload.
The broader client L0/L1 E2E campaign is tracked separately.

Each DataView shard persists two version-bound inputs: WAL publication frontier
W (`growing_transform_start_after_timetick`) and selected-base coverage A
(`compacted_transform_start_after_timetick`). Its shared TransformLog start is
F = min(W, A), derived at runtime and not persisted. Channel checkpoint reports
neither generate DataViews nor participate in native frontier calculation.

## 1. Two levels of coverage

### Segment cursor

`C(s, base) = T` means that every relevant Transform through T has already been
included in the exact Segment data version being loaded, or cannot affect its
rows. Loading that base and consuming TransformLog entries strictly after T
must recover all required changes.

C is a continuous-prefix guarantee. A maximum Delete timestamp, Manifest
version, or task scheduling position alone does not prove it. C must be stored
with its base/Manifest/deltalog revision and supplied to the Segment loader.
An old View must not combine an old base with a newer cursor from SegmentMeta.
Indirect Manifest version 0 loading must also freeze a matching base/cursor
pair when resolving metadata.

| Segment source/update | Cursor rule |
|---|---|
| Ordinary WAL-flushed L1 | Earliest effective Insert TimeTick, available as the first data StartPosition |
| Import | CommitImport TimeTick for this business VChannel |
| External-table refresh | CreateCollection TimeTick for this business VChannel; retain it for same-ID Manifest updates and new/replacement Segments |
| Non-L0 compaction | Minimum C of the actual input data versions |
| Split or exact data copy within the same collection | Inherit the source data version's C |
| RestoreSnapshot into a new collection | Target business VChannel RestoreSnapshot TimeTick; source snapshot changes must be included in the restored base |
| L0 compaction into Segment deltalogs | Advance to T only after complete coverage of the required interval through T is durable |
| No matching Delete | May advance after a complete evaluation proves the interval irrelevant |
| Index-only update | Keep C |
| Unknown/legacy coverage | Unknown; do not manufacture a usable cursor |

The earliest Insert itself can be the exclusive cursor: current Delete MVCC
only deletes rows with `insert_ts < delete_ts`. A Delete at or before the
earliest Insert cannot delete any row in that Segment. Transactions use their
effective outer commit time. Imported rows use their per-VChannel commit time.

External collections reject Insert/Delete/Upsert and WAL L1 Flush. Their persisted
W therefore remains the business VChannel creation origin B across refreshes
and recovery. DataCoord stamps refreshed Segment bases with this W before
persisting them. Source-table deletes are part of the refreshed base, not the
target external collection's TransformLog. This rule does not apply to writable
collections; neither the control-channel refresh timestamp nor a checkpoint is
a substitute for B. Missing native creation coverage remains not-ready.

Compaction inherits the versions it actually read. If concurrent work advances
a parent or the shard frontier, an output requiring an earlier cursor cannot
be published by reading a newer parent cursor or merely clamping the output.
It needs the missing coverage or a retry against valid inputs.

### DataView shard cursor

`F(view, vchannel)` defines the full shared TransformLogBuffer range required
by that View: `(F, consumedThrough]`. It covers all Segments in the shard's
View, including Segments assigned to other nodes. Moving a Segment between
nodes serving that View must not require a separate historical subscription.

DataView membership contains only flushed, published, loadable Segments.
Growing Segments constrain F but are not added to DataView membership.
Successive DataVersions must have nondecreasing F for each VChannel; unchanged
F is valid, including after a Manifest update or partial compaction.

QueryView history and live incremental consumption use **TransformLog only**.
QueryView must not load or forward L0 Segments to fill a gap before F. Storage
compaction may still inline changes into the selected base; query recovery
consumes the remaining suffix from WAL/WALSummary.

## 2. Persisted W and A, runtime F

At CreateCollection, synchronously create the full shard skeleton and set
W = A = F = B, where B is that business VChannel's CreateCollection WAL TimeTick.
Do not use the control-channel TimeTick, wall-clock time, or a later checkpoint.
An early background notification cannot create the initial shard skeleton.

W is a WAL-side publication certificate: after the associated Flush transaction
commits, every ordinary WAL L1 requiring a replay start before W has already
been published. Other data publication paths must preserve equivalent constraints.
W advances only with successful final L1 publication, never with checkpoint
updates or growing data-pack persistence.

A is the minimum C(s, selected base) over **all members of the new DataView**,
across every partition of this shard. Its name refers to data-base coverage,
not to the most recent Compaction's timestamp. Recalculate A after Flush as well
as Compaction, import, removal and every other membership/base change. A may
fall when a late Segment joins; W must already protect that Segment's history.
For an empty shard use A = W, a finite bound, rather than publishing infinity.

```text
W(new) = max(W(previous), accepted WAL Flush certificate)
A(new) = min C(s, selected base) over new members, or W(new) when empty
F(new) = min(W(new), A(new))
```

Only W and F must be nondecreasing. Never clamp a regressing F: reject the
publication because the new data needs history consumers may already have
trimmed. Ordinary non-L0 Compaction inherits the actual input bases' C; an
L0/Transform compaction may advance C only with complete-prefix coverage for
the exact new base. A maximum Delete timestamp or a newer Manifest is not proof.

## 3. WAL Flush certificate

The final L1 SaveBinlogPaths request carries
`growing_transform_start_after_timetick`. It is separate from the Segment's
own C and from its Flush message timestamp.

Under the VChannel observation lock, capture:

```text
H = the completely observed ordered WAL prefix
P = every Segment not yet confirmed finally published/retired in DataCoord,
    across all partitions, excluding only this request's atomic publication
W(request) = min(H, min CreateSegmentTimeTick(s) for s in P)
```

An empty P contributes no constraint. Segment creation is a conservative bound
before its first effective Insert and exists before first-pack persistence.
An in-flight RPC remains in P until its durable publication is confirmed.
Do not exclude another task merely because its objects were written or its RPC
was sent. The current request is excluded conditionally: its certificate only
becomes effective with its own SegmentMeta + DataView transaction.

Observe and capture use the same serialization, so an unobserved future
CreateSegment is after H. H is an observation boundary, not a recovery checkpoint
or the append tail. Recovered pending Segments must be reconstructed before
issuing a certificate. Ownership validation applies to the entire publication.

Out-of-order Flush example: X needs C=50 and Y needs C=100, in different
partitions. Y publishes first, but its W is bounded by X's creation position,
which is <=50. A may be 100 while F remains <=50. When X publishes, W can advance
but A becomes 50. F therefore cannot skip X's history. Later coverage advances
can raise A. No cross-partition L1 Flush ordering is required.

A retry of a previously committed Segment returns the original
`sealed_at_data_version`; a newer certificate in a retry is not a new publication.
Unknown certificates from older writers do not authorize W advancement.

## 4. Atomic publication and Transform materialization

Persist the selected Segment metadata, W, A and DataVersion in the same final
Flush transaction. Publish F only after commit, and retain immutable old versions.
Compaction and other publication paths inherit W and recalculate A from their
exact post-change members. Checkpoint updates never enqueue DataView recompute.
Native frontier generation needs neither the lazy collection cache nor a
checkpoint RPC/report. A-only/W changes are published as part of their owning
data mutation, not as an independent checkpoint-driven version.

Import jobs and other asynchronous publication paths must not fall through the
WAL-L1 proof. Pending CommitImport positions remain constraints until selected
Segment bases carry the matching C. Unknown coverage remains not-ready. A stale
Compaction output cannot inherit coverage from a newer parent base it did not
read. QueryView continues to consume TransformLog only, never L0 as a replay
fallback.

### Required materialization ordering

Before materializing a complete Transform interval through T, all relevant L1
Segments created before T, in all partitions of the VChannel, must have finished
final DataCoord publication (or confirmed retirement). Growing registration,
object writes and the Sealed enum alone do not satisfy this condition. L1 final
commits execute independently; materialization waits for them, never vice versa.

The maximum position of a batch, not its first Delete, determines this gate.
All physical output groups must succeed before the interval is called complete.
L0 materialization by itself does not advance any selected L1 base's C; the
appropriate compaction must still inline the complete relevant prefix. Partial
partition/Segment completion does not prove a shard-wide coverage advance.

This ordering is required independently of W/A. W/A itself does not require a
separate periodic materialization-frontier report to DataCoord.

## 5. Recovery, retention, and shared buffers

Persist W and A with each immutable DataView version. Keep the per-Segment C
bound to its selected base/Manifest revision. Restore F from that version's own
W and A, and validate A against its selected members. Do not use today's
checkpoint, today's Segment bases or a successor View's W to reconstruct an
older View. The DataView proto and catalog store only W/A; F is calculated for QueryView.

A legacy snapshot without W/A requires conservative migration from trustworthy
historical inputs before it is ready; zero is unknown, not an unconstrained
frontier. Published read objects remain immutable. Old QueryViews retain their
own version's inputs until all references have been released.

The cursor proof and history availability are separate requirements:

```text
complete readable TransformLog lower bound <= required F
```

WAL/WALSummary must retain the suffix needed by every View still eligible for
serving, in-flight preparation, and future reload. Merely materializing Delete
records into L0 does not authorize dropping that suffix. Aggregate View
retention with SN local Segment retention; one owner must not overwrite the
other's requirement in `SetQueryRetention`.

A conservative initial implementation can pin history from B for the live
VChannel, installed before GC during creation/recovery. This trades storage
space for avoiding an incomplete distributed View-retention protocol. Advancing
storage GC later requires the minimum across the latest reloadable View and
all still-protected older Views. Collection drop releases the protection only
after its tombstone is durable, all local Segments have been cleaned, and no
QueryView references remain. Release the pin before waiting for Summary
retirement, then remove the VChannel metadata after retirement completes.
No pin can recreate history already deleted; existing collections require a
readable-history check.

Each QN buffer retains the minimum F of all locally held Views and any pending
Segment registrations. It must not trim based only on local assigned Segments.
Acquire replacement references before releasing old ones. Avoid admitting an
older View after its range has been trimmed. Within that continuously retained
buffer, forward View evolution and Segment moves need no separate historical
copy. Restart, a new node, or a destroyed buffer still requires initialization.
Subscription success alone is not readiness: Segment application must catch up.

## 6. Implementation and validation

The producer is complete only after these pieces are wired:

- Per-VChannel CreateCollection origin and initial F propagation.
- Segment C production, version-bound persistence, snapshot/load propagation,
  and compaction inheritance/coverage validation.
- A complete WAL Flush certificate across every partition, plus consistent
  pending external-data publication constraints.
- Atomic W/A publication, monotonic F validation, no checkpoint-triggered
  recompute, and exact-version recovery.
- WALSummary retention that protects the TransformLog-only query path.

Required scenarios include out-of-order Flush across partitions; delayed first
pack registration; checkpoint/projection races; concurrent compaction;
Import callback delay and restart; empty shards; metadata publication failures;
SN/DataCoord/QN restart; old/new View coexistence; Segment moves; and reload
while Summary GC runs after SN local Segments have been released. Assert both
nondecreasing dynamically generated F, persistence of W/A but absence of F from persisted DataViews,
and exact Delete/Upsert query results.

## Key packages and current evidence

- `internal/streamingnode/server/wal/vchannel/segment/view.go`:
  `FlushInsertChunk` publishes Growing data before releasing Insert handles.
- `internal/streamingnode/server/wal/vchannel/segment/lifecycle_writer.go`:
  Growing registration and final `SaveBinlogPaths` commit.
- `internal/streamingnode/server/wal/vchannel/checkpoint_updater.go`:
  existing recovery-checkpoint reporting, not a safe-start producer.
- `internal/datacoord/services.go` and `internal/dataview/manager.go`:
  atomic Flush publication and immutable snapshot management.
- `internal/datacoord/ddl_callbacks_import.go`: per-VChannel commit timestamps.
- `internal/core/src/segcore/DeletedRecord.h`: strict Delete/Insert MVCC ordering.
- `internal/querynodev2/transformlogbuffer/buffer.go`: shared range retention
  and per-Segment replay/application.
- `internal/streamingnode/server/wal/walsummary/gc.go`: retention integration.

Related contracts: [DataView](data_view.md),
[Transform subscriptions](pure_transform_subscription.md),
[Segment persistence](../wal/segment_view_module.md), and
[Checkpoint persistence](../wal/checkpoint-persistence.md).

### DataView wire representation

This feature is unreleased. Remove the former derived
`DataViewOfShard.transform_start_after_timetick` field and reuse tag 3 for
`growing_transform_start_after_timetick`; tag 4 stores
`compacted_transform_start_after_timetick`. DataView contains only W/A.
The coordinator calculates `min(W, A)` when projecting the immutable DataView
into QueryView; `QueryViewMeta.transform_start_after_timetick` remains the
consumer-facing exclusive replay start. There is no clear-on-serialization or
restore-a-third-field path.

### RestoreSnapshot creation and asynchronous publication

A new restore uses one RestoreSnapshot broadcast carrying the target collection,
partition and index creation recipe and the preallocated JobID. It addresses all
target business VChannels plus the automatically included control channel. The
business-channel copy also initializes SN shard/schema state, recovery metadata,
and TransformLog retention. Its own TimeTick R is the target creation origin;
initial W = A = R. Source WAL positions are not target replay cursors.

The coordinator callback creates the target metadata in CollectionRestoring,
persists indexes, then creates an asynchronous copy Job. Segment IDs are allocated
inside that callback and persisted in the Job's source-to-target mappings; they
are not part of the broadcast message. Job persistence precedes target segment
registration. Callback replay looks up the same WAL JobID and reuses the Job.
The background checker registers missing segments from the pinned snapshot and
durable mappings before dispatching copy tasks, so partial registration and
restart cannot allocate another set of target segments.

The API retains its asynchronous contract: it returns a JobID without waiting
for the target collection to become available. Internal metadata recovery includes
Restoring collections; public collection lookup excludes them. Their names and
resources stay reserved. Copy results retain `IsImporting` until the restore
owner has materialized every copied L0 into the restored L1/L2 bases. The Job
persists stable compaction plan IDs per channel/partition; those plans name
their exact target Segment IDs and bypass ordinary compaction's dynamic target
selection. Ordinary compaction and DataView projection continue to exclude the
importing bases. All-partition L0 inputs cover every matching restored partition.

The existing L0 executor preserves source Delete timestamps and writes per-base
deltalogs/Manifest revisions. The snapshot producer also preserves the source
`CommitTimestamp`: import/CDC rows must retain their effective insertion time,
including when a Delete precedes import commit. Neither timestamp is replaced
by R. Source and target clocks still require separate treatment for general
cross-cluster restoration; inlining a source Delete is not a clock translation.

L0 input retirement and target delta publication form one metadata mutation.
Retired importing inputs survive GC until Job completion, allowing recovery to
recognize a committed mutation even before its task-state write. A durable task
completion marker survives task cleanup, and task GC retains that proof while
the owning Job is unfinished. Duplicate completed copy responses do not replace
the now-materialized base with the original copied manifest. Failed, cleaned
compaction attempts retry their stable plan identity.

Once all expected copies and delete materialization tasks are installed, the
checker clears the data Segments' importing flags, publishes the DataView, makes the collection available, and finally
persists the Job's Completed state. Each step is idempotent. Durable completed
copy tasks protect this final publication from timeout/failure cleanup; no extra
Committing job state is introduced. A public Completed result requires successful
publication, but does not imply that the user has loaded the collection.

Legacy RestoreSnapshot messages with an absent creation payload retain the old
restore-into-an-already-created-target callback. No existing RPC is removed.
Snapshot creation must include source Deletes in the restored data: stamping R
alone never permits dropping source L0/delete content, and QueryView must not
load standalone L0 as a workaround. Delete preservation is a required restore
validation case.
