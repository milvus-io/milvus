# Transform Start-After TimeTick

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @congqixia
- Design Review: 2026-07-29
- Design Update: 2026-10-03

Status: the checkpoint-bounded producer, runtime-only DataView frontier,
version-bound Segment cursors, Import registration, and conservative history
retention are implemented. L0 compaction retains the prior Segment cursor until
it can supply a continuous-prefix coverage certificate; a manifest update alone
does not justify advancement. Legacy or external data without recoverable
coverage must remain not-ready instead of fabricating a replay start.

DataCoord calculates the shard frontier from the existing reported channel
checkpoint, published Segment coverage, and unpublished Segment constraints.
This replaces the earlier mandatory shard-wide rotation/Flush barrier proposal.
Partitions may Flush independently, and no new SN-to-DataCoord watermark RPC is
required while the existing Growing registration/checkpoint contract is retained.

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
| Non-L0 compaction | Minimum C of the actual input data versions |
| Split or exact data copy | Inherit the source data version's C |
| L0 compaction into Segment deltalogs | Advance to T only after complete coverage of the required interval through T is durable |
| No matching Delete | May advance after a complete evaluation proves the interval irrelevant |
| Index-only update | Keep C |
| Unknown/legacy coverage | Unknown; do not manufacture a usable cursor |

The earliest Insert itself can be the exclusive cursor: current Delete MVCC
only deletes rows with `insert_ts < delete_ts`. A Delete at or before the
earliest Insert cannot delete any row in that Segment. Transactions use their
effective outer commit time. Imported rows use their per-VChannel commit time.

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

## 2. Generation in DataCoord

At collection creation, initialize each shard independently:

```text
B = this VChannel's CreateCollection WAL TimeTick
F(initialView, vchannel) = B
```

Preserve B as an immutable origin. Do not substitute the control-channel
TimeTick, the broadcast maximum, wall-clock time, or a later checkpoint.

For each subsequent snapshot, calculate:

```text
K = accepted channel checkpoint for this VChannel
S = min C(s, selected base) over members of the new DataView
G = min safe start over registered data that can still enter a later DataView

F(newView, vchannel) = min(K, S, G)
```

Omit an empty S or G set from the minimum. G covers **all partitions** of the
VChannel and includes Growing, Sealed/Flushing, and data whose output is durable
but whose publication into DataView is not yet committed. It is defined by
publication status, not just the `Growing` enum. Pending Import and other new
data publication paths must supply equivalent constraints as described below.

K is the recovery checkpoint already delivered through
`PChannelCheckpointUpdater -> UpdateChannelCheckpoint`. It is a completeness
boundary, not the Segment cursor or the Transform materialization cursor.
Use the checkpoint belonging to the shard; do not compare timestamps from
unrelated PChannels to derive a collection-wide minimum.

If K is missing or predates B, it cannot authorize advancement: keep the
initialized safe frontier. With no current members, remaining unpublished data
still constrains F; with neither set, K supplies a finite bound. Never replace
an empty set with a published infinity or reset the frontier to zero.
Unknown coverage must not be skipped. It needs a proven conservative lower
bound or must prevent publication of the affected new View. A candidate below
the previous F is an invariant violation; `max(oldF, candidate)` would conceal
missing history and is not a repair.

Example, with X and Y in different partitions of the same shard:

| Event | DataView members | Unpublished constraint | K | F |
|---|---|---|---|---|
| Y, with C=100, Flushes before X | Y | X starts at 50 | 200 | 50 |
| X later Flushes | X, Y | none | 200 | 50 |

X moves from G to S. Neither early Flush of X nor delayed publication of Y is
required. A cold Growing Segment can retain more Transform history, but does
not impose a cross-partition Flush ordering requirement.

## 3. Why the checkpoint makes the calculation complete

The existing ordinary streaming L1 path has this ordering:

```text
persist the first Insert pack
  -> PersistGrowingSegment registers its StartPosition in DataCoord
  -> install stable Segment state and release Insert handles
  -> publish the continuous recovery checkpoint
  -> report that checkpoint to DataCoord
```

CreateSegment registration also precedes completion of its retained handle.
A Segment's first data position is the first effective Insert, not its creation
TimeTick. Later packs preserve the original StartPosition.

For an Insert at t whose initial data registration has not completed, K cannot
pass t. Thus DataCoord does not need to see the latest complete Growing list:

- Registered but unpublished data is constrained by G.
- Not-yet-registered data is constrained by K, with `F <= K <= t`.
- Published data is constrained by S, with `F <= C(s, base)`.

For every current member, replaying from F therefore includes every required
Transform. For later ordinary Flushes, the incoming cursor was already
protected by G or was no earlier than the previous K. Transferring a cursor
from G to S without a gap cannot lower F. K and existing coverage advance
monotonically; removing a constraint cannot lower a minimum. Atomic parent-to-
output replacement preserves this argument only when compaction inheritance
uses valid input versions.

These are the producer's proof obligations. Retiring legacy Growing binlog
publication or checkpoint reporting must preserve the first-pack registration
barrier until an equivalent registration-completeness protocol is installed.
A dedicated SN safe-watermark RPC is a possible replacement, not a prerequisite
for this design.

## 4. Consistent publication and other data sources

### Snapshot construction

1. Capture K before reading the Segment projection.
2. Under collection publication synchronization, construct a consistent new
   membership and unpublished set, with coverage for the selected base versions.
3. Calculate F and check that it does not regress.
4. Persist DataView membership and version-bound Segment coverage, together
   with any membership transfer that must be atomic. Keep the calculated F only
   in the runtime snapshot exposed to consumers; do not serialize it.

Do not pair a newer checkpoint with an older Segment list. The synchronization
must include state/coverage changes relevant to S and G; a DataView lock alone
is insufficient if a producer mutates the projected metadata outside it.

For ordinary Flush, keep the existing atomic SegmentMeta + DataView commit:
before it the Segment contributes to G, and after it the Segment contributes
to S. Observing Flush, finishing object writes, or changing a state enum must
not create an intermediate state where the Segment belongs to neither set.
Retries return the original `sealed_at_data_version`.

Checkpoint advancement requests a coalesced, retryable recompute. A changed F
alone creates a new `compact_version`; it does not increment streaming_version.
Ordinary Flush retains its existing streaming_version transition. Membership
or Manifest changes can publish with unchanged F. Unchanged snapshot content
creates no new version. Recovery must reconcile persisted inputs even if the
notification preceding a crash was lost.

### Import and other new data

The streaming Insert proof does not automatically cover an asynchronous
CommitImport callback. Pending imports must remain represented in G until
publication, including the interval after WAL commit but before its TimeTick
has reached DataCoord's callback.

The implementation registers each business VChannel's CommitImport TimeTick in
ImportJob metadata from the broadcaster's synchronous AckOnce callback. SN
retains ownership of the CommitImport WAL message until that callback succeeds,
so K cannot pass the commit before its constraint is durable. The asynchronous
joined callback then persists Segment C and visibility before completing the
job. The projection reads job constraints before Segment metadata, and includes
all healthy Segments even when their DataView publication is pending. Thus a
completed job's constraint is already represented by its Segments.

Missing task fences block frontier generation conservatively. This also keeps
legacy pending jobs without registration evidence from authorizing progress.
These task-level commit timestamps are recovery inputs; they are not a persisted
copy of DataView F.

This ordering must cover replicated imports and recovered tasks as well as
local broadcasts. An already-acknowledged commit cannot retrospectively be protected
by pinning today's F. Legacy tasks without a proof need reconciliation before
advancement. Equivalent obligations apply to copy/external publication paths;
do not assume their cursors satisfy the ordinary Insert registration rule.

## 5. Recovery, retention, and shared buffers

DataView F is dynamically calculated and is **not persisted**. Persist C with
each referenced Segment data revision, so recovery can calculate the minimum
for the exact revisions selected by a View. Recover channel checkpoints,
publication state, and pending task constraints before generating runtime F.
A shard frontier serialized by an older writer is not authoritative.

Reconstruct runtime snapshots before exposing them to QueryView builders. For
retained historical versions, use their own version-bound coverage and cap the
result by the next retained version's reconstructed F; do not substitute the
latest Segment revision's C. This preserves version ordering even for an old
empty View followed by a View containing Segments. Once a runtime snapshot is
handed to a consumer, keep it immutable. Existing QueryViews keep their own
adopted cursor until their references are released. Missing recovery inputs
must block readiness rather than fabricate a current timestamp. A zero legacy
field means unknown coverage.

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
- A consistent K/S/G projection over every partition, including unpublished
  and pending external-data states.
- Atomic G-to-S transfer, monotonic F validation, checkpoint-driven recompute,
  and immutable snapshot recovery.
- WALSummary retention that protects the TransformLog-only query path.

Required scenarios include out-of-order Flush across partitions; delayed first
pack registration; checkpoint/projection races; concurrent compaction;
Import callback delay and restart; empty shards; metadata publication failures;
SN/DataCoord/QN restart; old/new View coexistence; Segment moves; and reload
while Summary GC runs after SN local Segments have been released. Assert both
nondecreasing dynamically generated F, absence of F from persisted DataViews,
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
