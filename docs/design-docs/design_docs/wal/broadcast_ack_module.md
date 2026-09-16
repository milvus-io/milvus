# Broadcast Ack Module

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

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

For successful consumers the callback marks the task ready and nonblockingly
wakes the dispatcher; it performs no Coordinator I/O. If any consumer poisoned
the message, the callback releases the Owner to free payload memory and records
a poisoned task. That task cannot Ack and retains its ResourceKey ordering claim.

## 3. ResourceKey Ordering

Two tasks conflict when they share the same `(Domain, Key)` and at least one
side is exclusive. A task is schedulable when:

```text
exclusive callback fired
AND message is not poisoned
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
by Segment persistence. Coordinator failures use the Ack retry path; poisoned
local work remains incomplete and keeps the WAL available for replay. Explicit
Tracker blocker categories are not implemented yet, so a VChannel persist
request may still be issued for such an entry without resolving it.

## 6. Close

Close cancels dispatch and retry work. It does not release an unfinished Owner.
The message is replayed from the last published global checkpoint.

## 7. Invariants

1. `Accept` consumes the Owner exactly once.
2. A successful broadcast releases its Owner after Coordinator Ack; poisoned release never acknowledges success.
3. The readiness callback is one-shot and nonblocking.
4. Earlier conflicting tasks retain their ResourceKey claims through retry.
5. BroadcastAck has no component `checkpoint_time_tick`.
6. BroadcastAck does not wait for checkpoint catalog publication.
