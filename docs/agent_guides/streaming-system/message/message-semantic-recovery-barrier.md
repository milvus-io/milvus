# RecoveryBarrier Message

`RecoveryBarrier` is a WAL-internal empty message appended by a StreamingNode at
the beginning of WAL recovery for a PChannel.

It is not a user mutation and it carries no payload:

```proto
message RecoveryBarrier {}
```

## Purpose

`RecoveryBarrier` has two recovery-time responsibilities:

1. It verifies WAL writer ownership by performing the first recovery write. On
   backends that support writer fencing, this also fences stale owners so they
   can no longer append to this PChannel.
2. It establishes a per-VChannel query-plan MVCC target during recovery without
   storing per-VChannel MVCC snapshots in the recovery checkpoint.

At recovery start, after the persisted checkpoint is read and before
RecoveryStorage loads its catalog state, the node appends one persisted
`RecoveryBarrier` message to the recovered PChannel. The append must succeed
before the node can continue recovery or serve the PChannel; failure means this
node is fenced or otherwise does not own a writable WAL. The ability to prevent
an old owner from writing after a newer recovery owner has written the barrier
depends on the WAL backend; currently this is provided by Woodpecker writer
fencing.

Recovery then performs a bounded Meta-only scan from the checkpoint through the
`RecoveryBarrier`. That scan reconstructs VChannel and Segment metadata but does
not claim that growing or transform data has been replayed through the barrier.
The barrier TimeTick becomes the initial query-plan MVCC target for every live
VChannel. QueryRuntime may prepare earlier from its current WALView data
frontier; query execution waits until DataScanner advances the runtime's growing
and transform frontiers to the requested MVCC.

`RecoveryBarrier` carries the same TimeTick confirmation semantics as
[TimeTick](message-semantic-time-tick.md). In addition to that TimeTick barrier
role, it establishes the recovered query-plan MVCC target.

## Dispatch

| Message | Dispatch | ExclusiveRequired | ResourceKey |
|---------|----------|-------------------|-------------|
| RecoveryBarrier | PChannel-level WAL internal | No | - |

`RecoveryBarrier` has an empty body. Its VChannel scope is not encoded in the
message. Consumers derive the scope from the live VChannel metadata after replay
has reached the barrier. If checkpoint-to-barrier replay contains collection or
partition lifecycle messages, the barrier applies only to VChannels that are
still live after those messages have been applied.

## Semantics

When a recovering node successfully appends a `RecoveryBarrier`, the append
proves that the node currently owns a writable WAL for the PChannel. If the
backend supports writer fencing, such as Woodpecker, the append also prevents old
owners from appending any later WAL entries. A stale owner that cannot append the
barrier must stop recovery and must not serve writes, query planning, or query
execution for that PChannel. All recovered local state that becomes serviceable
is therefore ordered before or at the barrier, and all later writes are ordered
after the barrier under the current owner.

When a `RecoveryBarrier` with TimeTick `T` is applied during recovery, every
live VChannel on the PChannel uses the barrier as a query-plan MVCC target:

- The query-plan MVCC manager initializes or advances
  `mvcc[vchannel].growing_timetick` to `T`.
- The query-plan MVCC manager initializes or advances
  `mvcc[vchannel].transforming_timetick` to `T`.
- DataScanner eventually delivers the barrier event to QueryRuntime modules,
  advancing their local frontiers to `T` after all earlier data events have
  been applied. Runtime-specific behavior is defined by
  [StreamingNode Growing Segment Runtime Design](../../../design-docs/design_docs/qviews/snview/growing_segment_runtime.md)
  and
  [QueryNode QueryView Resource Preparation Design](../../../design-docs/design_docs/qviews/qnview/querynode_queryview_resource_preparation.md).

This makes future QueryView query plans safe to request the barrier TimeTick as
the starting per-VChannel MVCC. A runtime prepared before DataScanner reaches
the barrier blocks query task acquisition in `WaitMVCCVisible` until both local
frontiers cover the request.

## Invariants

- `RecoveryBarrier` must be persisted in the WAL. An in-memory idle sync is not
  sufficient.
- `RecoveryBarrier` is appended before recovery stream replay starts and is
  consumed as part of that replay.
- Successful append of the barrier proves the recovering node can write the
  PChannel WAL. On backends with writer fencing, currently Woodpecker, it also
  fences old owners so they cannot append later entries. If the append fails
  because the writer is fenced, recovery must fail and the node must not serve
  the PChannel.
- The barrier must be appended before the recovered WAL is considered writable.
  QueryRuntime resource preparation does not wait for DataScanner to consume it;
  individual queries requiring its TimeTick wait on runtime MVCC visibility.
- The barrier must be processed with the same TimeTick confirmation semantics
  defined by [TimeTick](message-semantic-time-tick.md).
- The barrier does not carry VChannel names; the post-replay recovered VChannel
  metadata at the barrier is the single source of the live VChannel set.
- `RecoveryBarrier` is not a PChannel-level MVCC fallback. It explicitly creates
  or advances per-VChannel query MVCC entries for each live VChannel.
- Normal TimeTick messages confirm WAL ordering and wake waiters, but they do not
  create per-VChannel query MVCC entries by themselves.

## Replication

`RecoveryBarrier` is self-controlled local recovery state. It is generated by the
WAL owner that is recovering a PChannel and should not be treated as a user DML
or DDL message. Secondary clusters generate their own recovery barriers during
local WAL recovery instead of relying on CDC replication of another cluster's
barrier.
