# Segment View Component

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

`SegmentView` owns growing-segment assignment, Insert buffering, object-storage
persistence, lifecycle side effects, and recovery metadata for one segment. It
is an internal component of `VChannelRecoveryModule`.

## 1. State Model

SegmentView separates execution state from recoverable snapshot state:

```text
live observed state
  +-- pending asynchronous work in message order
  +-- one serial task queue per Segment

stable recoverable state
  +-- object references and lifecycle state
  +-- checkpoint_time_tick
  +-- persisted snapshot frontier
```

Only stable recoverable state is emitted to catalog. The current implementation
executes each Segment's tasks serially and updates stable state in that order.
Different Segments may finish out of order; one Segment does not currently need
an out-of-order completion queue.

This prevents a snapshot from containing an effect that its
`checkpoint_time_tick` cannot safely suppress during replay.

## 2. Message Ack

Each concrete asynchronous unit clones the dispatch Retained before it is
exposed. A batched object chunk keeps one handle for every contributing WAL
message and releases them only after the shared write succeeds and stable
recovery state is committed.

If dynamic child tasks are discovered asynchronously, SegmentView clones a
parent handle during synchronous observation and joins children behind it.

Retriable errors retain unfinished handles. A terminal failure poisons and
releases affected handles, freeing payload memory while leaving their Tracker
entries incomplete. The checkpoint and WAL truncation remain blocked before
the poisoned message; durable whole-message dumping is future work.

One Txn affecting several segments gives each SegmentView one independent
outer-Txn handle. Multiple assignments within one Txn for the same segment are
processed together by that SegmentView.

## 3. Observe

The public component path has no recovery mode:

```go
ObserveMessage(message RetainedImmutableMessage)
```

The first decision is:

```text
message.TimeTick <= checkpoint_time_tick -> durable no-op
otherwise                                -> route complete segment effect
```

SegmentView also deduplicates messages already present in its current pending
queue, although the normal PChannel scanner dispatches each message once.

### 3.1 CreateSegment

Create the view with the schema valid at the message TimeTick. Ensure-growing
holds a handle until the lifecycle side effect succeeds. Assignment metadata
and `checkpoint_time_tick` advance together in stable state.

### 3.2 Insert

Append the retained Insert to the segment's pending L1 buffer and update live
row/byte accounting. SegmentView may batch subsequent Inserts for this segment
when flushing.

Before QueryView is enabled, each written growing pack is also registered with
DataCoord through `SaveBinlogPaths(Flushed=false, WithFullBinlogs=true)`. The
request uses the cumulative stable pack snapshot, including matching row counts
and checkpoint. Object output is retained across RPC retries. Registration must
succeed before installing the durable snapshot or releasing Insert handles.
This temporary query-recovery bridge is marked `TODO: Remove after enabling queryview.`

The first data pack also publishes `StartPosition` from the first Insert's
TimeTick and LastConfirmedMessageID, with its VChannel and WAL name. A Txn uses
the complete outer transaction's position. CreateSegment's timestamp remains
lifecycle metadata and is not the data start position. Later packs omit
StartPosition so DataCoord preserves the original value. Each data pack reports
a complete DmlPosition from its last Insert/Txn boundary.

No StartPosition or MessageID is added to SN recovery metadata. Coordinator
publication precedes the stable SN snapshot: if the snapshot is lost, replay
reconstructs the first pack and the same position; if the snapshot survives,
Coordinator already owns that position and subsequent packs need not report it.


After the object chunk and this temporary registration succeed:

1. install the chunk reference into stable state;
2. complete the current task in the Segment's serial queue;
3. update stable row/byte accounting and `checkpoint_time_tick`;
4. mark SegmentView dirty;
5. release handles whose durable effects are now represented.

### 3.3 Txn Insert

The SegmentView consumes all Insert assignments for this segment from the outer
Txn atomically. It owns one Txn handle regardless of assignment count. Other
segments affected by the same Txn independently retain the same outer message.

### 3.4 Flush

Flush closes the segment using a runtime-only closing TimeTick. New Inserts
and CreateSegment observations are rejected, while already retained work keeps
running. Neither a FLUSHED state nor the close boundary is installed into the
stable metadata merely because Flush was observed. Completed intermediate packs
may still publish their growing (or legacy SEALED) snapshots.

After all preceding Insert writes and the final lifecycle commit succeed,
install the final checkpoint, publication DataVersion when present, and
TOMBSTONED state together under the Segment lock; mark dirty before releasing
handles. There is no intermediate FLUSHED snapshot in this write path and no
later owner callback needed to turn successful publication into a tombstone.
Before publication a crash replays the retained Flush/Drop to reconstruct close;
after publication the tombstone proves completion.

This tombstone retires the RecoveryStorage assignment, not the serving Segment.
A normally flushed Segment and its binlogs remain owned by DataCoord.

DataCoord binds `sealed_at_data_version` to the Segment's first growing-to-sealed
publication. SegmentInfo and the DataView are committed together; on the large
transaction fallback, the Segment record carrying this binding and the DataView
remain in the final atomic transaction, after binlog writes. SaveBinlogPaths
returns that original version on every retry, including after compaction has
retired the Segment. It never substitutes the latest Collection DataVersion.

SegmentView installs this version in both live and stable metadata, marks dirty,
and only then releases Flush handles. A version-only change must be published
even if the component checkpoint TimeTick did not advance. Recovery retries a
flushed Segment whose version is absent. A missing or malformed version in an
ordinary success response is an error, not completion.

Empty Segments do not enter DataView. An explicit retired result from DataCoord
(or SegmentNotFound) completes their lifecycle as a durable TOMBSTONED snapshot
without inventing a version. Other already-dropped targets follow the same
terminal path when they have no original publication version. Tombstones are
installed only after coordinator confirmation and do not require another commit
on recovery. No independent L1-committed boolean is persisted.
Final commit, including a recovered retry, omits StartPositions and CheckPoints:
all data packs were already registered, so sealing preserves DataCoord's complete
data positions and cumulative row count. It must not replace a usable physical
position with the SN snapshot's timestamp-only lifecycle checkpoint.


### 3.5 Flush-Style Messages

DropCollection, DropPartition, TruncateCollection, ManualFlush, FlushAll, CreateSnapshot,
schema-changing AlterCollection, and AlterWAL may flush several SegmentViews.
Every affected view owns its own handle and completion condition.

## 4. RequestPersistThrough

```go
RequestPersistThrough(targetTimeTick uint64)
```

The request is idempotent:

- target at or before `checkpoint_time_tick` is a no-op;
- target already covered by an in-flight task reuses that task;
- the task selects this segment's pending work needed to cover the target;
- batching may include later pending messages from the same segment;
- the request never batches another segment merely because it shares a
  VChannel or PChannel.

If TT=100 triggers a flush and the segment also contains 101 and 102, the
SegmentView may write all three together. A later Trigger for 101 is a no-op.

## 5. Dirty Snapshots

A Segment snapshot contains stable state only:

- identity and assignment;
- historical schema reference;
- object chunk references;
- cumulative writer `Statistics` and pre-existing delta-binlog references;
- stable row/byte statistics;
- lifecycle and tombstone state;
- `checkpoint_time_tick`;
- cleanup state.

There are no separate metadata and data checkpoint fields. `MarkPersisted`
advances only through the exact captured stable snapshot.

## 6. Recovery

1. reconstruct stable SegmentView state from catalog;
2. initialize `checkpoint_time_tick` from the snapshot;
3. replay once from the PChannel global checkpoint;
4. skip this segment's effects at or before its frontier;
5. rebuild pending work for later messages with fresh handles;
6. reconstruct unfinished close from replayed Flush/Drop; confirmed terminal
   tombstones need no retry. Existing legacy final-commit recovery remains separate.

Recovery is logically idempotent but not physically exactly once. A crash after
an object write and before snapshot publication may leave an unreferenced
object. GC/Defrag removes it later.

### 6.1 Legacy Flusher Migration

Migration reconciles the union of StreamingNode allocations and DataCoord
unflushed segments. DataCoord binlogs, row counts, and DmlPosition supply the
durable prefix; the old allocation observation checkpoint is not a persistence
frontier. A DataCoord-only segment uses its stored SchemaVersion and first data
position to recover the prefix. Missing DataCoord entries are queried separately
and may be discarded only when replay is guaranteed to include CreateSegment.

An unfinished segment whose allocation was retired or sealed recovers in durable
`SEALED` state. It cannot receive new allocations, but accepts replayed Inserts
after its durable frontier. At RecoveryBarrier it schedules the remaining packs
and final L1 commit. Intermediate pack snapshots remain SEALED so a crash retries
final publication without losing the seal intent. Flushing/Flushed/Dropped
DataCoord segments require no more insert-tail recovery.

Before deleting any old allocation metadata, migration persists the conservative
physical replay checkpoint with the legacy format marker and current owner term.
It then writes component snapshots/removals and publishes the new format marker
last. A crash during a chunked catalog update therefore reopens at the safe cursor
and reruns migration instead of skipping the removed allocation's creation.

## 7. Cleanup

Segment deletion persists a tombstone before removing recovery metadata or
objects. Object deletion occurs only after catalog no longer references the
objects. Cleanup tasks do not create a second checkpoint frontier.

## 8. Invariants

1. SegmentView has one stable `checkpoint_time_tick`.
2. Stable state never contains committed effects beyond a frontier gap.
3. Successful release follows dirty stable state; poisoned release never
   authorizes checkpoint advancement.
4. Same-segment batching is owned by SegmentView, not the Ack trigger.
5. Txn assignment count does not change message ownership count per segment.
6. Close and cancellation never release unfinished handles.
7. Segment snapshots precede the global checkpoint that covers them.
