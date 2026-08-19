# Segment View Component

`SegmentView` owns growing-segment assignment, Insert buffering, object-storage
persistence, lifecycle side effects, and recovery metadata for one segment. It
is an internal component of `VChannelRecoveryModule`.

## 1. State Model

SegmentView separates execution state from recoverable snapshot state:

```text
live observed state
  +-- pending asynchronous work in message order
  +-- completed-but-not-committed work behind an earlier gap

stable recoverable state
  +-- object references and lifecycle state
  +-- checkpoint_time_tick
  +-- persisted snapshot frontier
```

Only stable recoverable state is emitted to catalog. A later object write may
finish before an earlier write, but its reference stays in the pending commit
queue until the segment-local relevant prefix is continuous.

This prevents a snapshot from containing an effect that its
`checkpoint_time_tick` cannot safely suppress during replay.

## 2. Message Ack

Each concrete asynchronous unit clones the dispatch Retained before it is
exposed. A batched object chunk keeps one handle for every contributing WAL
message and releases them only after the shared write succeeds and stable
recovery state is committed.

If dynamic child tasks are discovered asynchronously, SegmentView clones a
parent handle during synchronous observation and joins children behind it.

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

After the object chunk succeeds:

1. install the chunk reference into completed pending state;
2. advance the continuous commit queue as far as gaps allow;
3. update stable row/byte accounting and `checkpoint_time_tick`;
4. mark SegmentView dirty;
5. release handles whose durable effects are now represented.

### 3.3 Txn Insert

The SegmentView consumes all Insert assignments for this segment from the outer
Txn atomically. It owns one Txn handle regardless of assignment count. Other
segments affected by the same Txn independently retain the same outer message.

### 3.4 Flush

Flush closes the segment at the message TimeTick. It joins pending Insert
writes and final lifecycle commit. Stable state advances through the Flush only
after all preceding segment work and the idempotent commit succeed.

The final commit stores the exact first DataView version containing the sealed
segment as `SealedAtDataVersion`.

### 3.5 Flush-Style Messages

DropCollection, DropPartition, TruncateCollection, ManualFlush, FlushAll,
schema-changing AlterCollection, and AlterWAL may flush several SegmentViews.
Every affected view owns its own handle and completion condition.

## 4. PersistThrough

```go
PersistThrough(ctx context.Context, targetTimeTick uint64)
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
6. schedule or reuse final commit for every recovered flushed segment missing
   `SealedAtDataVersion`.

Recovery is logically idempotent but not physically exactly once. A crash after
an object write and before snapshot publication may leave an unreferenced
object. GC/Defrag removes it later.

## 7. Cleanup

Segment deletion persists a tombstone before removing recovery metadata or
objects. Object deletion occurs only after catalog no longer references the
objects. Cleanup tasks do not create a second checkpoint frontier.

## 8. Invariants

1. SegmentView has one stable `checkpoint_time_tick`.
2. Stable state never contains committed effects beyond a frontier gap.
3. A buffered message handle releases only after its durable effect is
   represented by dirty stable state.
4. Same-segment batching is owned by SegmentView, not the Ack trigger.
5. Txn assignment count does not change message ownership count per segment.
6. Close and cancellation never release unfinished handles.
7. Segment snapshots precede the global checkpoint that covers them.
