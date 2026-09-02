# Segment View Component

`segment.SegmentView` owns growing-segment assignment, Insert buffering, object
storage persistence, lifecycle side effects, and segment recovery metadata for
one segment.

It is an internal component of `VChannelRecoveryModule`, not an independent
top-level `moduleapi.Module`. Message completion follows
[WAL Message Ack Design](message_ack.md).

## 1. Ownership

```text
PChannelRecoveryManager
  -> VChannelRecoveryModule
       -> SegmentView*
```

Each `SegmentView` owns:

- `SegmentAssignmentMeta` and its persisted snapshot frontier;
- the segment's historical schema snapshot;
- pending Insert messages and row/byte accounting;
- object-storage chunks already written for the growing segment;
- ensure-growing, flush-buffer, and commit-L1 tasks;
- segment tombstone and physical cleanup state;
- retained immutable message handles associated with pending data.

`VChannelRecoveryModule` owns:

- lookup and creation of SegmentViews;
- routing CreateSegment, Insert, Txn(Insert), Flush, and flush-style messages;
- passing the ref-counted message itself to every actual SegmentView consumer;
- composing Segment dirty snapshots into the PChannel recovery snapshot;
- advancing VChannel-level Segment DataVersion summaries.

SegmentView does not own TransformLog data, broadcast acknowledgement, WAL
checkpoints, or QueryView references.

## 2. Message Ack

Every affected SegmentView directly calls `Clone()` on the Retained dispatch
handle before
submitting or exposing asynchronous work. The returned retained immutable
message is owned by that concrete consumer or by a module-local parent operation
that waits for its dynamic child tasks.

```text
ref-counted immutable message
  -> SegmentView A retained handle
  -> SegmentView B retained handle
  -> pending Insert chunk retained handle
```

There is no message-level Segment bit or global Segment fanout. Segment tasks
remain independently scheduled, and Insert flushes may batch data from multiple
WAL messages. A batched chunk keeps one retained message per contributing WAL
message and calls `Release()` on all of them only after the shared write
succeeds.

If a SegmentView discovers child tasks asynchronously, it must clone one parent
message handle before returning from observation and manage the child count
inside the SegmentView. It cannot use the released dispatch handle after synchronous
dispatch returns.

## 3. Observe Rules

### 3.1 CreateSegment

If the target segment is absent, `VChannelRecoveryModule` creates a SegmentView
using the schema valid at the message timetick and updates segment metadata.

In MetaAndData mode, ensure-growing work clones a message handle before the task
is submitted. The handle releases after the lifecycle side effect succeeds.
Failure keeps the handle live and retries.

The metadata mutation marks the SegmentView dirty so the next persist batch
captures a stable Segment snapshot.

### 3.2 Insert

Insert metadata updates row and size statistics. In MetaAndData mode, one
retained specialized Insert message is appended to the SegmentView's pending L1
buffer. The payload and ownership remain one object.

The retained handle remains attached to the buffered message until the chunk that
contains it is successfully written to object storage and installed into the
SegmentView state. A flush may contain messages from several WAL records; all
of their handles release after the shared write succeeds.

If the chunk write fails, no handle releases.

### 3.3 Txn Insert

A transaction is observed as one WAL message. All Insert bodies affecting this
VChannel are applied atomically to the relevant SegmentViews. Every affected
SegmentView retains its own outer Txn message handle. Each handle releases after
that view's resulting data work succeeds.

Delete bodies are handled by TransformLog and do not create Segment handles.

### 3.4 Flush

Flush closes the target SegmentView at the message timetick and records the
sealed metadata transition. In MetaAndData mode, pending Insert data and the
commit-L1 side effect retain message handles until all required work is durable and
accepted by the segment lifecycle writer.

The final commit returns the exact first DataView version whose membership
contains this segment. The SegmentView stores that value as
`SealedAtDataVersion`. Repeating the external commit after a lost response must
return the same value even if unrelated Flushes have advanced the collection's
current DataVersion.

### 3.5 Flush-Style Broadcast Messages

DropCollection, DropPartition, TruncateCollection, ManualFlush, FlushAll,
schema-changing AlterCollection, and AlterWAL may flush one or more retained
SegmentViews according to their message scope.

Every affected view clones its own handle from the same ref-counted message.
This is required so the dedicated BroadcastAck sink cannot acknowledge the
message before all segment work caused by it has completed.

## 4. Object Storage And Metadata Publication

Segment completion has two layers:

```text
data layer: object chunk or lifecycle side effect succeeds
  -> Segment retainedMessage.Release()

metadata layer: Segment dirty snapshot persists to etcd
  -> MarkPersisted
```

Before releasing a retained handle, an asynchronous Segment consumer installs
its metadata changes and marks the SegmentView dirty. RecoveryStorage freezes
the batch checkpoint before consuming snapshots, persists those snapshots
first, and writes the checkpoint last.

Segment-local data timeticks may remain useful for internal state and cleanup.
They are not inputs to RecoveryStorage Data checkpoint advancement.

## 5. Dirty Snapshots

SegmentView emits `SegmentModuleSnapshot` payloads for catalog compatibility.
The snapshot module name does not imply an independently registered runtime
SegmentModule.

A dirty snapshot contains a stable clone of the segment assignment state and:

- identifies its PChannel/VChannel/segment catalog key;
- records the metadata timetick covered by the snapshot;
- records the data timetick represented by persisted segment state;
- advances persisted SegmentView state only through `MarkPersisted()`.

RecoveryStorage may coalesce updates from several WAL messages into one stable
snapshot. Message Ack therefore does not attempt to map each metadata field
back to individual messages.

## 6. Tombstone And Cleanup

Segment drop is logical before it is physical:

1. record and persist the segment tombstone;
2. retain the SegmentView while replay, DataVersion, or serving rules still
   require it;
3. remove object/catalog state only after the SegmentView's own cleanup
   conditions are met;
4. emit a catalog delete snapshot and finalize cleanup after persistence.

Segment cleanup does not depend on TransformLog private state. VChannel-level
summary updates are performed through the owning `VChannelRecoveryModule`.

## 7. Recovery

On recovery:

1. `PChannelRecoveryManager` groups persisted segment snapshots by VChannel;
2. `VChannelRecoveryModule` reconstructs SegmentViews with their historical
   schemas, metadata, durable chunks, and tombstones;
3. the data scanner replays from the persisted Data checkpoint;
4. replayed messages at or before the durable Segment data checkpoint are
   skipped, while messages not covered by that data checkpoint are applied
   against recovered Growing or Flushed state;
5. new replay work creates fresh ref-counted messages and retained handles;
6. every recovered `FLUSHED` segment without `SealedAtDataVersion` immediately
   schedules or reuses one final-commit task. That task first completes any
   remaining data work, then performs the idempotent lifecycle commit and
   installs the returned version.

Retained handles themselves are not recovered from Segment metadata.

Segment replay is required to be data-safe, not physically exactly once.
Ensure-growing uses the fixed SegmentID and accepts retry. A repeated Insert
pack write may allocate new object paths and leave older unreferenced objects.
A recovered final commit may repeat the external lifecycle RPC. Those duplicate
objects or events require storage GC or external reconciliation, but recovered
Segment metadata must reference a complete durable data set and must not skip
unfinished Insert work.

## 8. Invariants

1. SegmentView is VChannel-owned, not a top-level recovery module.
2. Every actual SegmentView consumer clones one message handle before asynchronous
   work is exposed.
3. Dynamic child work is joined behind a module-local parent handle cloned
   during synchronous dispatch.
4. A buffered Insert handle releases only after its containing object chunk is
   durable.
5. Ensure-growing and commit handles release only after their side effects
   succeed.
6. Work that has not reached its success condition keeps its handle live;
   cancellation and close do not release it.
7. Segment metadata publication is captured by the frozen persist batch.
8. Segment retained handles are the only Segment completion input to the
   RecoveryStorage Data checkpoint.
9. Every broadcast-triggered Segment consumer clones a message handle before
   synchronous VChannel observation returns; BroadcastAck waits for Owner
   exclusivity before acknowledging the message.
10. Async Segment consumers mark metadata dirty before releasing their handle.
11. `SealedAtDataVersion` is the segment's first DataView membership version,
    not the latest collection version observed by a retry.
12. Every retained `FLUSHED` segment without `SealedAtDataVersion` has one
    pending or retrying final-commit task before QueryView WAL view capture.
