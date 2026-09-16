# WAL Message Ack Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

This document defines how RecoveryStorage tracks one WAL message until all
required persistence consumers and Coordinator broadcast acknowledgement have
finished. The resulting continuous successful prefix, capped by
`WALSummary.LastAcked()`, bounds the global recovery checkpoint.

## 1. Scope

One WAL message may create asynchronous work in multiple SegmentViews and
copied records in WALSummary. Segment work participates in reference-counted
completion; Summary has its own confirmation frontier. The target
[L0Materializer](l0_materializer.md) observes only window boundaries and reads
Summary without holding WAL handles.
Broadcast messages additionally wait for consuming-side Ack to StreamingCoord.

Ack observes completion. It does not define Segment or L0Materializer scheduling,
batch boundaries, or object layout.

## 2. Message Handles

The common wrapper is:

```go
type OwnedImmutableMessage interface {
    Message() ImmutableMessage
    Clone() RetainedImmutableMessage
    IsPoisoned() bool
    RegisterExclusiveCallback(callback func())
    Release()
}

type RetainedImmutableMessage interface {
    Message() ImmutableMessage
    Clone() RetainedImmutableMessage
    Release()
    PoisonedRelease()
    IntoPoisoned()
    IsPoisoned() bool
}
```

`NewOwnedImmutableMessage` creates the unique root reference. Every independent
async unit gets its own clone. The finalizer runs exactly once when the total
reference count reaches zero. Tracker uses
`NewOwnedImmutableMessageWithFinalizer`, whose callback also receives the final
poison status. The original constructor retains its cleanup-only callback API.

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
2. leaves a poisoned entry incomplete, or marks a successful entry completed;
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

A poisoned entry also releases its payload, but remains an incomplete prefix
blocker. Its raw message stays recoverable from the untruncated WAL. A future
durable dump of the entire poisoned message may permit checkpoint advancement
without data loss; until that protocol exists, poison never means success.

## 4. Dispatch

```text
Owner O = Tracker.Track(M)
dispatch D = O.Clone()
Summary.ObserveMessage(M) // install records and readable coverage first
manager.ObserveMessage(D) // Segment state, then L0 requested window
D.Release()
BroadcastAck.Accept(O)
```

PChannel-wide routing clones once for every affected VChannel. SegmentView
clones when it exposes asynchronous work. Summary copies records without
retaining source handles; L0Materializer records only boundary positions.

There is no special untracked metadata flow. Every recovered or live WAL
message enters the same Tracker path.

## 5. Consumer Completion

### SegmentView

A Segment handle releases after the required object write or lifecycle side
effect succeeds, the resulting recovery state is installed, its continuous
`checkpoint_time_tick` advances when possible, and the view is marked dirty.

One object chunk may cover multiple handles. Retriable failures keep uncovered
handles live. Terminal failures poison and release them; the Tracker retains
the incomplete positions and cannot advance through them.

### L0Materializer

L0Materializer retains no WAL handle and keeps no copied record window.
WALSummary independently persists Delete records and exposes `LastAcked`.
L0Materializer reads Summary in bounded batches. L0 materialization does not
participate in source-message Ack or wait for a TransformLog subscriber.
The ownership split is an implementation target; see the component status in
[L0 Materializer](l0_materializer.md).

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
Completed() (WALCheckpoint, uint64)
```

The publisher freezes the minimum by TimeTick of this point and Summary's
`LastAcked`; neither frontier alone permits publication. The
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

Summary runs its own backlog check based on the oldest staged record's age or
tail pressure. It can flush even when Tracker has no pending entries, including
low-traffic Deletes followed by no new messages. Chunk/manifest retries remain
owned by the scheduler. BroadcastAck and catalog publication have separate
retry paths; explicit blocker-category reporting is not yet implemented.

## 8. Broadcast Ack And Retry

Ordinary messages release the Owner immediately after dispatch. BroadcastAck
keeps the Owner, waits for the exclusive callback, and performs Coordinator Ack
under ResourceKey ordering. Ack failure keeps the Owner and retries.

A poisoned broadcast releases its payload without sending Coordinator Ack and
remains a ResourceKey ordering blocker. Non-conflicting broadcasts can proceed.
Only successful, non-poisoned finalization proves local handle-consumer
completion and Coordinator Ack success; Summary confirmation is checked separately.

## 9. Close

Close cancels background stall checks, dispatchers, and retry timers. It does
not release unfinished handles or fabricate completion. Unpublished work is
reconstructed by replay from the global checkpoint.

## 10. Invariants

1. Every WAL message has one Tracker entry and one Owner.
2. Each async Segment consumer owns an independent Retained clone; Summary
   records and L0Materializer window boundaries do not.
3. Finalization occurs only at reference count zero.
4. Completed payloads are released independently of ordered-prefix progress.
5. Tracker checkpoint progress is continuous and monotonic.
6. Component dirty state is installed before the corresponding handle release.
7. Broadcast Ack success is part of broadcast-message completion.
8. RequestPersistThrough calls are VChannel-scoped and TimeTick-based.
9. Txn messages complete as one whole outer message.
