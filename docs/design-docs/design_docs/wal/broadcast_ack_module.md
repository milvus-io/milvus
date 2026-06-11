# Broadcast Ack Module

`AckModule` owns StreamingNode local acknowledgement for broadcast WAL messages.
It is a RecoveryStorage module because broadcast ack is a replayable Data-side
effect.

## 1. Ownership

`AckModule` owns:

- detecting persisted messages that carry a `BroadcastHeader`;
- submitting coordinator broadcast Ack RPC tasks;
- ordering ack tasks by WAL ack order;
- Data barriers that block RecoveryStorage Data checkpoint until ack succeeds.

`AckModule` does not own:

- VChannel, Segment, or TransformLog metadata;
- data flush decisions;
- object storage writes;
- module dirty snapshots.

## 2. Ack As Data Work

Broadcast ack is a Data-side effect. For every persisted message with a
`BroadcastHeader`, AckModule submits an ack task in MetaAndData mode and returns
a Data barrier. The Data barrier disappears only after the coordinator Ack API
succeeds.

Ack is replayable and idempotent. If StreamingNode crashes after sending ack
but before WALCheckpoint persistence, recovery may send the same ack again.

## 3. Preconditions

Ack tasks always wait for previous ack task completion. Some message types also
wait for data-module progress before acking the coordinator.

Preconditions are defined by message type and message scope:

- VChannel-scoped flush/drop/schema-changing messages wait for the composed
  Data frontier of affected modules in that vchannel.
- Partition-scoped drop messages wait for the composed Data frontier of the
  affected partition.
- PChannel-wide flush-style messages wait for the composed all-local data-module
  Data frontier.
- Broadcast messages without growing-data dependency wait only for previous ack
  task completion.

Examples:

- `DropCollection` waits for the target vchannel's composed
  SegmentModule/TransformLogModule Data frontier.
- `DropPartition` waits for the affected partition's SegmentModule frontier and
  the vchannel TransformLogModule frontier.
- `FlushAll` waits for all local SegmentModule/TransformLogModule frontiers.
- `CommitImport` waits only for previous ack task completion.

## 4. Module Interaction

AckModule does not call data modules to flush data, persist Views, or mutate
state. It only observes module barriers/frontiers exposed through the
RecoveryStorage framework.

This keeps ack logic as transport-level acknowledgement while leaving business
decisions inside VChannelModule, SegmentModule, and TransformLogModule.

## 5. ModuleAPI Implementation

`AckModule` implements the core `Module` API. It depends on a composed
`DataFrontierProvider` built by RecoveryStorage.

```go
type AckModule struct {
    frontierProvider moduleapi.DataFrontierProvider
}

var _ moduleapi.Module = (*AckModule)(nil)
```

### Module.Name

Returns `ModuleNameAck`.

### Module.ObserveMessage

If the message has no `BroadcastHeader`, AckModule returns an empty
`ObserveResult`.

If the message has a `BroadcastHeader`, AckModule creates an ack task and
returns a Data barrier. The task precondition combines:

- previous ack task completion;
- the composed Data frontier for the message scope when required by message
  semantics.

AckModule does not return Meta barriers.

### Module.SwitchIntoMetaAndData

Switches ack processing into MetaAndData mode and returns nil. AckModule does
not contribute WAL open data snapshots.

### Module.ConsumeDirtySnapshots

Returns nil. Ack state is represented by in-memory Data barriers and replayable
coordinator Ack RPCs, not by catalog snapshots.

### DataFrontierProvider Dependency

AckModule asks the provider for scope barriers:

```go
frontierProvider.DataFrontier(scope)
```

The provider composes all modules implementing `DataFrontierView`, such as
SegmentModule and TransformLogModule. AckModule does not depend on those
modules directly.

AckModule does not implement `DataCheckpointView`, `DataFrontierView`, or
`CheckpointPersistedObserver`.

## 6. Invariants

1. AckModule returns Data barriers, not Meta barriers.
2. Ack tasks are ordered by WAL ack order.
3. Ack preconditions wait on module frontiers but do not mutate module state.
4. Ack is idempotent across recovery.
5. RecoveryStorage Data checkpoint cannot pass a broadcast message until its
   ack barrier disappears.
