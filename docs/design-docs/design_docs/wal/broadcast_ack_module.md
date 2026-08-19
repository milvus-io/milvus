# Broadcast Ack Module

`BroadcastAck` sends consuming-side acknowledgements for broadcast WAL messages
to StreamingCoord. It is a dedicated RecoveryStorage sink, not a data
persistence component.

The common lifetime contract is defined in
[WAL Message Ack Design](message_ack.md).

## 1. Ownership

```go
func (m *BroadcastAck) Accept(owner message.OwnedImmutableMessage)
```

`Accept` consumes top-level ownership:

- ordinary messages are released immediately;
- broadcast messages are queued in PChannel observation order;
- the caller must not clone or use the Owner after `Accept`.

## 2. Readiness Callback

BroadcastAck registers one exclusive callback. The callback fires when all
local Retained consumers have released and BroadcastAck is the only remaining
Owner holder.

The callback only marks the task ready and nonblockingly wakes the dispatcher.
It performs no Coordinator IO and does not release the Owner.

## 3. ResourceKey Ordering

Two tasks conflict when they share the same `(Domain, Key)` and at least one
side is exclusive. A task is schedulable when:

```text
exclusive callback fired
AND task is not in flight
AND no earlier unfinished task conflicts
```

Conflicting tasks preserve WAL observation order. Independent tasks may Ack
concurrently.

## 4. Ack And Retry

On success, BroadcastAck releases the Owner and unblocks later conflicting
tasks. On failure, it keeps the Owner and ResourceKey claim, waits for retry,
and does not block unrelated tasks.

Coordinator Ack is idempotent. A crash before global checkpoint publication may
replay and repeat it.

## 5. Recovery Tail Interaction

A stalled BroadcastAck holds the global continuous prefix but cannot be fixed
by Segment or TransformLog persistence. Tracker reports it as an Ack blocker;
the tail controller records the category and relies on Ack retry rather than
issuing `RequestPersistThrough`.

## 6. Close

Close cancels dispatch and retry work. It does not release an unfinished Owner.
The message is replayed from the last published global checkpoint.

## 7. Invariants

1. `Accept` consumes the Owner exactly once.
2. A broadcast Owner remains live until Coordinator Ack succeeds.
3. The readiness callback is one-shot and nonblocking.
4. Earlier conflicting tasks retain their ResourceKey claims through retry.
5. BroadcastAck has no component `checkpoint_time_tick`.
6. BroadcastAck does not wait for checkpoint catalog publication.
