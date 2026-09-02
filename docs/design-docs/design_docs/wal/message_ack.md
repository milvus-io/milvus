# WAL Message Ack Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

This document defines how RecoveryStorage tracks one WAL message until all
required persistence consumers and Coordinator broadcast acknowledgement have
finished. The resulting continuous completed prefix is the sole candidate for
the global recovery checkpoint.

## 1. Scope

One WAL message may create asynchronous work in multiple SegmentViews and
TransformLogs. RecoveryStorage may advance past it only after every concrete
consumer succeeds. Broadcast messages additionally wait for consuming-side Ack
to StreamingCoord.

Ack observes completion. It does not define Segment or TransformLog scheduling,
batch boundaries, or object layout.

## 2. Message Handles

The common wrapper is:

```go
type OwnedImmutableMessage interface {
    Message() ImmutableMessage
    Clone() RetainedImmutableMessage
    RegisterExclusiveCallback(callback func())
    Release()
}

type RetainedImmutableMessage interface {
    Message() ImmutableMessage
    Clone() RetainedImmutableMessage
    Release()
}
```

`NewOwnedImmutableMessage` creates the unique root reference. Every independent
async unit gets its own clone. The finalizer runs exactly once when the total
reference count reaches zero.

`RegisterExclusiveCallback` fires when the Owner is the only remaining
reference. BroadcastAck uses it as the readiness signal for Coordinator Ack.
The callback itself is not message completion; finalization still waits for
BroadcastAck to release the Owner.

A committed Txn is retained as one immutable outer message. Child message
iteration never creates independent ownership.

## 3. Tracker Entry

```go
type trackedEntry struct {
    point            WALCheckpoint
    logicalEndOffset  uint64
    vchannel          string
    message           ImmutableMessage
    trackedAt         time.Time
    completed         bool
}
```

`Tracker.Track(raw)` appends an entry in WAL order and returns an Owner whose
finalizer:

1. clears the entry's message pointer immediately;
2. marks the entry completed;
3. removes the continuous completed prefix;
4. advances the completed point and byte offset to the last removed entry.

Completion may be out of order:

```text
M1 incomplete: message != nil, completed=false
M2 complete:   message == nil, completed=true
M3 complete:   message == nil, completed=true
```

M2 and M3 retain only lightweight ordered records while M1 blocks the global
prefix. Their payloads do not stay live solely because the checkpoint is
blocked.

## 4. Dispatch

```text
Owner O = Tracker.Track(M)
dispatch D = O.Clone()
manager.ObserveMessage(D)
D.Release()
BroadcastAck.Accept(O)
```

PChannel-wide routing clones once for every affected VChannel. SegmentView and
TransformLog clone only when they expose actual asynchronous work.

There is no special untracked metadata flow. Every recovered or live WAL
message enters the same Tracker path.

## 5. Consumer Completion

### SegmentView

A Segment handle releases after the required object write or lifecycle side
effect succeeds, the resulting recovery state is installed, its continuous
`checkpoint_time_tick` advances when possible, and the view is marked dirty.

One object chunk may cover multiple handles. Failure keeps all uncovered
handles live.

### TransformLog

A TransformLog handle releases after the chunk covering the message is durable
and recoverable TransformLog state is installed and marked dirty. L0
materialization does not participate in source-message Ack.

### Metadata Components

The VChannel metadata views (VChannelView) apply their state, advance their
continuous component `checkpoint_time_tick`, mark themselves dirty, and then
return. They do not retain a handle when no asynchronous work is needed.

### QueryRuntime

QueryRuntime receives a plain immutable message or copy. It has independent
TimeTick filtering and never owns a persistence handle.

## 6. Continuous Checkpoint Prefix

Tracker exposes:

```go
CompletedPoint() WALCheckpoint
CompletedLogicalOffset() uint64
```

The publisher may freeze this point but cannot publish a newer point. The
published checkpoint remains a separate state until catalog commit succeeds.

An asynchronous consumer always marks its component dirty before releasing its
last handle. Therefore a snapshot collection after freezing `CompletedPoint`
contains the recoverable component state required by every message in the
candidate prefix.

## 7. Stall Trigger

Tracker owns the ordered knowledge needed to identify the oldest incomplete
prefix. It runs a periodic background check and invokes a directly held
VChannel-scoped requester:

```go
type VChannelPersistRequester interface {
    RequestPersistThrough(vchannel string, targetTimeTick uint64)
}
```

For each VChannel, Tracker requests the largest TimeTick that currently
satisfies the stall timeout. It does not pass message objects and does not route
through RecoveryStorageImpl.

The trigger is meaningful only for persistence consumers. BroadcastAck or
catalog publication stalls are exposed to the tail controller as separate
blocker categories.

## 8. Broadcast Ack And Retry

Ordinary messages release the Owner immediately after dispatch. BroadcastAck
keeps the Owner, waits for the exclusive callback, and performs Coordinator Ack
under ResourceKey ordering. Ack failure keeps the Owner and retries.

Reference count zero for a broadcast therefore proves both local consumer
completion and Coordinator Ack success.

## 9. Close

Close cancels background stall checks, dispatchers, and retry timers. It does
not release unfinished handles or fabricate completion. Unpublished work is
reconstructed by replay from the global checkpoint.

## 10. Invariants

1. Every WAL message has one Tracker entry and one Owner.
2. Each async consumer owns an independent Retained clone.
3. Finalization occurs only at reference count zero.
4. Completed payloads are released independently of ordered-prefix progress.
5. Tracker checkpoint progress is continuous and monotonic.
6. Component dirty state is installed before the corresponding handle release.
7. Broadcast Ack success is part of broadcast-message completion.
8. RequestPersistThrough calls are VChannel-scoped and TimeTick-based.
9. Txn messages complete as one whole outer message.
