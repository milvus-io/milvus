# WAL Recovery Architecture

This document defines the RecoveryStorage framework shared by all WAL recovery
modules. Module-specific state and workflows are described in separate files:

- [VChannel View Module](vchannel_view_module.md)
- [Segment View Module](segment_view_module.md)
- [TransformLog View Module](transform_log_view_module.md)
- [Broadcast Ack Module](broadcast_ack_module.md)
- [Message Workflow](message-workflow.md)

## 1. Background

StreamingNode consumes persisted WAL messages to rebuild and advance
module-owned recovery state. Each module maintains one or more in-memory Views.
A View is the module's consistency state and can be persisted to the recovery
catalog when dirty.

Each View has two logical parts:

- **Meta**: the synchronous part updated by `ObserveMessage`.
- **Data**: the asynchronous part advanced by durable tasks, lifecycle side
  effects, or coordinator acknowledgements.

Recovery uses persisted View snapshots plus historical WAL messages to rebuild
the same in-memory state through the same `ObserveMessage` path used by live
consumption.

## 2. View Model

A View contains:

- module-specific Meta state;
- module-specific Data state;
- `MetaTimeTick`, the latest WAL timetick reflected by Meta;
- `DataTimeTick`, the latest WAL timetick whose Data-side effects are durable.

The relationship is:

```text
ObserveMessage(M)
  -> module synchronously updates View.Meta and View.MetaTimeTick
  -> View becomes dirty

Data task completion
  -> module asynchronously updates View.Data and View.DataTimeTick
  -> View becomes dirty

Snapshot persist task
  -> RecoveryStorage persists module DirtySnapshot to catalog
  -> RecoveryStorage calls DirtySnapshot.MarkPersisted()
  -> owning module advances MetaBarrier and/or DataBarrier
```

The invariant is:

```text
View.MetaTimeTick >= View.DataTimeTick
```

`MetaTimeTick` usually advances first because Meta updates are synchronous.
`DataTimeTick` advances after asynchronous durable work completes.

## 3. Core Architecture

Each PChannel has one RecoveryStorage instance. RecoveryStorage dispatches
persisted WAL messages to modules, tracks physical checkpoints through
CheckpointManager, and persists WALCheckpoint.

```text
WAL Scanner
    |
    v
RecoveryStorage
    |
    +--> VChannelModule
    |       +-- VChannel/schema/partition views
    |       +-- Meta barriers
    |
    +--> SegmentModule
    |       +-- Segment assignment and L1 views
    |       +-- Segment data tasks
    |       +-- Meta/Data barriers
    |
    +--> TransformLogModule
    |       +-- Delete TransformLog views
    |       +-- TransformLog data tasks
    |       +-- Meta/Data barriers
    |
    +--> AckModule
    |       +-- broadcast ack tasks
    |       +-- Data barriers
    |
    +--> Scheduler
    |       +-- asynchronous module tasks
    |
    +--> CheckpointManager
            +-- Meta physical checkpoint
            +-- Data physical checkpoint
```

The framework has four layers:

1. **Observe layer**: synchronously consumes WAL messages and updates module
   Meta.
2. **Task layer**: runs module-owned Data tasks and RecoveryStorage-owned
   DirtySnapshot persistence tasks through Scheduler.
3. **Barrier layer**: exposes persisted View progress as Meta/Data barriers.
4. **Physical checkpoint layer**: advances WALCheckpoint after
   CheckpointManager sees no remaining barrier for the ordered prefix.

## 4. RecoveryStorage

RecoveryStorage is the PChannel-level orchestration layer.

It owns:

- WAL scanner lifecycle;
- module dispatch;
- CheckpointManager;
- WALCheckpoint persistence;
- WAL truncation by persisted Data physical checkpoint;
- background collection and persistence of module dirty snapshots.

It does not own:

- module Views;
- module dirty state;
- module business decisions;
- Data tasks;
- object storage writes;
- lifecycle side effects;
- broadcast ack conditions.

RecoveryStorage calls every module for every persisted WAL message. Modules
decide whether the message is relevant, update their own state, and return
barriers. RecoveryStorage registers the message physical point and returned
barriers into CheckpointManager.

RecoveryStorage may collect dirty snapshots from modules and persist them, but
the module decides when a snapshot is dirty and how its View changes.

## 5. CheckpointManager

CheckpointManager owns ordered physical checkpoint advancement.

It tracks two physical lanes:

- **Meta physical checkpoint**: WAL restart point for Meta recovery.
- **Data physical checkpoint**: WAL restart point for Data recovery and the WAL
  retention point.

For each consumed message, RecoveryStorage registers a physical point and
optional barriers:

```text
physical point P + MetaBarrier
physical point P + DataBarrier
```

A physical lane advances only through the continuous prefix whose barriers have
disappeared. A barrier disappears when the owning module has persisted the View
state required for that physical point.

CheckpointManager does not know vchannel, segment, schema, tombstone, import,
ack, object storage, or lifecycle semantics.

## 6. Scheduler

Scheduler is the asynchronous execution plane. Modules submit tasks to Scheduler
for:

- Data persistence;
- lifecycle side effects;
- broadcast ack;
- DirtySnapshot persistence;
- cleanup.

Scheduler is parallel by default. Task ordering is expressed through
preconditions:

- Segment Data tasks are ordered per segment.
- TransformLog Data tasks are ordered per vchannel.
- Snapshot persist tasks are ordered per module or per owner according to
  RecoveryStorage policy.
- Ack tasks are ordered by WAL ack order.
- Cleanup waits for persisted physical checkpoints to pass the retained
  tombstone timetick.

Scheduler does not understand module business keys. It only checks task
preconditions.

## 7. Module Boundaries

RecoveryStorage owns three independent growing-data modules:

- `VChannelModule`: VChannel metadata, schema history, partition lifecycle, and
  VChannel tombstones.
- `SegmentModule`: Segment assignment metadata, Insert/L1 output, segment
  lifecycle side effects, and segment tombstones.
- `TransformLogModule`: Delete and Txn(Delete) TransformLog buffers, chunk
  files, scanners, truncation, L0 materialization, and transform-log
  tombstones.

Each module implements the RecoveryStorage module API directly. There is no
outer data-module coordinator owning their business logic.

The only required cross-module read is:

```text
SegmentModule -> VChannelModule.SchemaAt(vchannel, partitionID, timetick)
```

This is needed to attach the correct historical schema when `SegmentModule`
creates segment state. Tombstone finalize and cleanup are module-local
responsibilities. `TransformLogModule` does not read VChannel or Segment state
for Delete replay, tombstone finalize, or cleanup.

## 8. ModuleAPI

The RecoveryStorage module API is intentionally small. It contains only the
contracts required by WAL recovery orchestration. Module-specific capabilities,
such as schema lookup or TransformLog subscription, are not part of the common
API.

### 8.1 Core Module

```go
type Module interface {
    Name() ModuleName

    // RecoveryStorage broadcasts every persisted WAL message to every module.
    // The module decides whether the message is relevant.
    ObserveMessage(ctx context.Context, msg message.ImmutableMessage) ObserveResult

    // Switch from MetaOnly replay into MetaAndData mode and return the
    // module snapshot needed by WAL open.
    SwitchIntoMetaAndData() ModuleSnapshot

    // Capture dirty views as stable snapshots for RecoveryStorage-owned
    // catalog persistence. This is a local memory snapshot operation and
    // therefore does not return an error.
    ConsumeDirtySnapshots() []DirtySnapshot
}
```

```go
type ModuleName string

const (
    ModuleNameVChannel     ModuleName = "vchannel"
    ModuleNameSegment      ModuleName = "segment"
    ModuleNameTransformLog ModuleName = "transformlog"
    ModuleNameAck          ModuleName = "ack"
)
```

```go
type ObserveResult struct {
    Meta walcheckpoint.Barrier
    Data walcheckpoint.Barrier
}
```

There is no `CanPlay`, `CanReplay`, `RequirePersist`, or `Trigger` method in
the common API. Message relevance, replay idempotency, data work scheduling,
and cleanup decisions are module-local responsibilities.

### 8.2 ModuleSnapshot

`ModuleSnapshot` is returned by `SwitchIntoMetaAndData` for WAL open. It is not
the same thing as a dirty snapshot.

```go
type ModuleSnapshot interface {
    ModuleName() ModuleName
}
```

Concrete module snapshots are typed by module:

```go
type VChannelModuleSnapshot struct {
    VChannels map[string]*streamingpb.VChannelMeta
}

type SegmentModuleSnapshot struct {
    Segments map[int64]*streamingpb.SegmentAssignmentMeta
}

type TransformLogModuleSnapshot struct {
    TransformLogs map[string]*streamingpb.VChannelTransformLogMeta
}
```

RecoveryStorage assembles these module snapshots into the WAL open
`RecoverySnapshot`. Modules that do not contribute WAL open data, such as
`AckModule`, return nil.

### 8.3 DirtySnapshot

`DirtySnapshot` is the smallest catalog persistence unit consumed by
RecoveryStorage.

```go
type DirtySnapshot interface {
    ModuleName() ModuleName
    Key() SnapshotKey
    Op() SnapshotOp
    Payload() proto.Message

    MetaTimeTick() uint64
    DataTimeTick() uint64

    // Called by RecoveryStorage after the snapshot is persisted successfully.
    MarkPersisted()
}
```

```go
type SnapshotKey struct {
    PChannel  string
    VChannel  string
    SegmentID int64
}
```

```go
type SnapshotOp int

const (
    SnapshotOpUpsert SnapshotOp = iota
    SnapshotOpDelete
)
```

`MarkPersisted` belongs to the dirty snapshot because the snapshot can carry
the owning module pointer and the exact in-flight view needed to update
persisted frontiers safely:

```go
type transformLogDirtySnapshot struct {
    owner *TransformLogModule
    key   SnapshotKey
    meta  *streamingpb.VChannelTransformLogMeta
    op    SnapshotOp
}

func (s *transformLogDirtySnapshot) MarkPersisted() {
    s.owner.markSnapshotPersisted(s)
}
```

RecoveryStorage does not know how a module advances its internal barriers. It
only persists the snapshot and calls `MarkPersisted` after success.

`ConsumeDirtySnapshots` is not a dirty queue pop and does not perform catalog or
object-storage I/O. It captures the current dirty view as a stable in-flight
snapshot. RecoveryStorage owns that in-flight snapshot until it is persisted
successfully. If persistence fails, RecoveryStorage retries the same snapshot
and does not call `MarkPersisted`.

While an in-flight snapshot exists for an owner, repeated consume calls return
that same stable view. If the underlying view becomes dirty again while the
in-flight snapshot is being persisted, the module records the newer dirty state
in memory. After `MarkPersisted` clears the in-flight snapshot, the next
`ConsumeDirtySnapshots` call captures the newer dirty view as the next stable
snapshot.

### 8.4 Snapshot Persistence

RecoveryStorage dispatches catalog writes by `ModuleName`, `SnapshotOp`, and
`SnapshotKey`:

```text
vchannel/upsert      -> SaveVChannel
vchannel/delete      -> DropVChannel
segment/upsert       -> SaveSegmentAssignment
segment/delete       -> DropSegmentAssignment
transformlog/upsert  -> SaveTransformLogMeta
transformlog/delete  -> DropTransformLogMeta
```

The common persister API is:

```go
type ModuleSnapshotPersister interface {
    Persist(ctx context.Context, snapshot DirtySnapshot) error
}
```

The persistence loop is:

```text
for module in modules:
    snapshots := module.ConsumeDirtySnapshots()
    for snapshot in snapshots:
        keep snapshot as in-flight
        retry persist snapshot by ModuleName + Op + Key until success
        snapshot.MarkPersisted()
        clear in-flight snapshot
        NotifyBarrierUpdated()
```

### 8.5 Optional Module Views

Modules expose additional framework capabilities only when needed.

```go
type DataCheckpointView interface {
    DataCheckpointTimeTick() uint64
}
```

`SegmentModule` and `TransformLogModule` implement this so RecoveryStorage can
compute the global data checkpoint as the minimum data checkpoint across data
owners.

```go
type DataProgressKind int

const (
    // DataProgressDurable is the normal Data checkpoint frontier. It means the
    // module's recovery-visible Data effect has been persisted.
    DataProgressDurable DataProgressKind = iota

    // DataProgressMaterialized is the coordinator-visible materialization
    // frontier used by synchronous flush/drop acknowledgements.
    DataProgressMaterialized
)

type DataFrontierView interface {
    DataFrontier(scope Scope) walcheckpoint.Barrier
}
```

```go
type Scope struct {
    Type ScopeType
    Kind DataProgressKind

    VChannel     string
    CollectionID int64
    PartitionID  int64
}

type ScopeType int

const (
    ScopeAll ScopeType = iota
    ScopeVChannel
    ScopePartition
)
```

`AckModule` depends on an aggregated `DataFrontierProvider`, not on concrete
data modules:

```go
type DataFrontierProvider interface {
    DataFrontier(scope Scope) walcheckpoint.Barrier
}
```

RecoveryStorage builds the provider by composing all modules that implement
`DataFrontierView`.

`Scope.Kind` selects the progress kind. `DataProgressDurable` is used for
normal RecoveryStorage/Ack data dependency checks. `DataProgressMaterialized`
is used when the caller needs coordinator-visible data output, currently
`DropCollection`, `ManualFlush`, and `FlushAll`. `SegmentModule` maps
`DataProgressMaterialized` to its normal Segment Data frontier because L1 flush
is already committed through DataCoord. `TransformLogModule` returns a distinct
L0 materialized frontier backed by catalog-persisted
`materialized_time_tick`.

```go
type CheckpointPersistedObserver interface {
    NotifyCheckpointPersisted(metaTimeTick uint64, dataTimeTick uint64)
}
```

Modules implement this only when persisted WALCheckpoint progress can create
cleanup opportunities. Cleanup still flows through `ConsumeDirtySnapshots`,
catalog persistence, and `DirtySnapshot.MarkPersisted`.

### 8.6 Runtime

Modules receive a runtime for asynchronous tasks and notifications:

```go
type Runtime struct {
    Scheduler AsyncTaskScheduler
    Notifier  ModuleNotifier
}
```

```go
type AsyncTaskScheduler interface {
    Submit(task scheduler.Task) scheduler.TaskHandle
    Notify()
}
```

```go
type ModuleNotifier interface {
    NotifyModuleUpdated(module ModuleName)
    NotifyBarrierUpdated()
}
```

`NotifyModuleUpdated` means the module may have dirty snapshots or completed
internal work. `NotifyBarrierUpdated` means RecoveryStorage can attempt
checkpoint advancement.

## 9. Normal Workflow

### 9.1 WAL Open

```text
Load WALCheckpoint
Load module View snapshots from catalog
Construct modules in MetaOnly mode
Run bounded Meta scanner from Meta physical checkpoint to open tail
ObserveMessage rebuilds module Meta and dirty Views
Switch modules into MetaAndData mode
Start data/live scanner from Data physical checkpoint
WAL open succeeds
```

Meta recovery and live consumption use the same `ObserveMessage`
implementation. During the bounded Meta scanner, modules are in MetaOnly mode,
so `ObserveMessage` updates only Meta and does not submit Data-chain work. After
modules switch into MetaAndData mode, the data/live scanner enables Data-chain
buffering and task submission.

### 9.2 ObserveMessage

```text
Scanner reads persisted message M
RecoveryStorage dispatches M to every module
Each module synchronously updates relevant Meta and MetaTimeTick
Each module may append lightweight in-memory buffers
Each module may submit tasks according to policy
Each module returns Meta/Data barriers
RecoveryStorage registers M into CheckpointManager
```

`ObserveMessage` must not perform object storage writes, catalog writes,
lifecycle RPCs, broadcast RPCs, or long retry loops.

### 9.3 Data Task Completion

```text
Scheduler runs module Data task
Task performs durable side effect
Owning module updates Data state and DataTimeTick
Owner becomes dirty
Module marks a DirtySnapshot and notifies RecoveryStorage
```

Data task completion updates View state in memory. It does not directly advance
a physical checkpoint.

### 9.4 Snapshot Persistence

```text
RecoveryStorage consumes module DirtySnapshots
RecoveryStorage persists each DirtySnapshot to catalog
RecoveryStorage calls DirtySnapshot.MarkPersisted()
Owning module updates MetaBarrier/DataBarrier from persisted snapshot
CheckpointManager observes that barriers disappeared
RecoveryStorage persists new physical WALCheckpoint
```

Snapshot persistence is RecoveryStorage-owned asynchronous work. Modules own the
dirty snapshot content and the barrier update performed by `MarkPersisted`.

### 9.5 Physical Checkpoint Persistence

```text
CheckpointManager advances in-memory physical checkpoints
RecoveryStorage persists WALCheckpoint
RecoveryStorage truncates WAL by persisted Data physical checkpoint
```

WAL truncation never uses Meta physical checkpoint.

## 10. Failure Recovery

Recovery restarts from persisted WALCheckpoint and module View snapshots
persisted in catalog.

| Failure point | Recovery behavior |
|---|---|
| Crash after ObserveMessage but before DirtySnapshot persist | WALCheckpoint cannot cross the message because the View-backed barrier still blocks. Recovery scans the message again and rebuilds Meta through `ObserveMessage`. |
| Crash after Data side effect but before DirtySnapshot persist | Data physical checkpoint cannot cross the message. The data scanner restarts from the older Data checkpoint and repeats or reconciles the Data task. |
| Crash after DirtySnapshot persist but before WALCheckpoint persist | Recovery starts from an older physical checkpoint. The persisted snapshot already records progress, so repeated `ObserveMessage` is skipped or handled idempotently by module state. |
| Crash after Meta physical checkpoint persist while Data checkpoint is older | Meta recovery starts later; the data scanner starts earlier. Retained Views and tombstones remain available for Data-chain observation. |
| Crash after Data physical checkpoint persist | The data scanner starts after that checkpoint. This is safe because Data checkpoint persistence happens only after Data barriers disappear. |
| Crash after broadcast ack before WALCheckpoint persist | Recovery may ack again. Broadcast ack is replayable and idempotent. |

## 11. Tombstone Retention

Dropped or flushed objects remain in retained Views until the module that owns
the object can safely remove them.

Tombstone state is module-local:

- VChannel and partition tombstones are owned by `VChannelModule`.
- Segment tombstones are owned by `SegmentModule`.
- TransformLog tombstones and truncation cleanup are owned by
  `TransformLogModule`.

Retained tombstoned Views can be physically deleted only when the owning module
has persisted the tombstone state and both physical checkpoint lanes have passed
the tombstone timetick:

```text
Meta physical checkpoint > tombstone timetick
Data physical checkpoint > tombstone timetick
```

This guarantees neither scanner can restart from a point that still needs the
retained View or historical-message filter.

## 12. Design Constraints

- View is the module-owned consistency state in memory and can be persisted
  when dirty.
- Meta and Data are both parts of View.
- `ObserveMessage` synchronously updates View.Meta and `MetaTimeTick`.
- Data tasks asynchronously update Data state and `DataTimeTick`.
- Modules expose dirty state as `DirtySnapshot` values.
- RecoveryStorage persists dirty snapshots to catalog.
- MetaBarrier and DataBarrier advance only after the corresponding dirty
  snapshot is persisted and `DirtySnapshot.MarkPersisted()` runs.
- RecoveryStorage persists WALCheckpoint and dispatches module work, but does
  not own module business state.
- CheckpointManager advances physical checkpoints after barriers disappear from
  the ordered prefix.
- Scheduler executes module-owned asynchronous work and uses preconditions for
  ordering.
- WAL truncation uses only the persisted Data physical checkpoint.
