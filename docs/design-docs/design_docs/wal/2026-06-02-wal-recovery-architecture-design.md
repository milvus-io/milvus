# WAL Recovery Architecture Design Document

## 1. Background and Motivation

StreamingNode consumes WAL messages to rebuild and advance module-owned recovery state. Each module maintains one or more **Views**. A View is an in-memory consistency view that can be persisted into ETCD when dirty.

Each View has two parts:

- **Meta**: the synchronous part of the View, updated by `ObserveMessage`.
- **Data**: the asynchronous part of the View, advanced by tasks such as object storage writes, lifecycle side effects, and broadcast acknowledgement.

ETCD stores persisted snapshots of dirty Views. Recovery uses those snapshots together with historical WAL messages to rebuild the in-memory Views through the same `ObserveMessage` path used by live consumption.

## 2. View Model

A View is a module-owned in-memory consistency view.

It contains:

- Module-specific Meta state.
- Module-specific Data state.
- `MetaTimeTick`: the latest WAL timetick whose Meta changes are reflected in the View.
- `DataTimeTick`: the latest WAL timetick whose Data-side effects are durable and reflected in the View.

A View is dirty when its in-memory content differs from the latest View snapshot persisted in ETCD. A dirty View can be persisted asynchronously to make ETCD consistent with memory.

The relationship is:

```
ObserveMessage(M)
  -> module synchronously updates View.Meta and View.MetaTimeTick
  -> View becomes dirty

Data task completion
  -> module asynchronously updates View.Data and View.DataTimeTick
  -> View becomes dirty

View persist task
  -> module persists dirty View snapshot to ETCD
  -> module updates MetaBarrier and DataBarrier
```

The invariant is:

```
View.MetaTimeTick >= View.DataTimeTick
```

`MetaTimeTick` usually advances earlier because View.Meta is updated synchronously during WAL observe. `DataTimeTick` advances later because View.Data depends on asynchronous durable work.

## 3. Core Architecture

Each PChannel has one RecoveryStorage instance. RecoveryStorage dispatches persisted WAL messages to modules, tracks physical checkpoints through CheckpointManager, and persists WALCheckpoint.

```
WAL Scanner
    │
    ▼
RecoveryStorage
    │
    ├── GrowingModule
    │       ├── Views
    │       ├── Data tasks
    │       └── Meta/Data barriers
    │
    ├── AckModule
    │       ├── Ack tasks
    │       └── Data barriers
    │
    ├── Scheduler
    │       └── asynchronous module tasks
    │
    └── CheckpointManager
            ├── Meta physical checkpoint
            └── Data physical checkpoint
```

The framework has four layers:

1. **Observe layer**: synchronously consumes WAL messages and updates View.Meta.
2. **Task layer**: runs module-owned Data tasks and View persistence tasks through Scheduler.
3. **Barrier layer**: exposes persisted View snapshot progress as Meta/Data barriers.
4. **Physical checkpoint layer**: advances WALCheckpoint after CheckpointManager sees no remaining barrier for the ordered prefix.

Per-message recovery behavior is described in [WAL Recovery Message Flow](2026-06-02-wal-recovery-message-flow.md).

## 4. Component Responsibilities

### 4.1 RecoveryStorage

RecoveryStorage is the PChannel-level orchestration layer.

It owns:

- WAL scanner lifecycle.
- Module dispatch.
- CheckpointManager.
- WALCheckpoint persistence.
- WAL truncation by persisted Data physical checkpoint.
- Background triggering of module persistence and policy-driven module checks.

It does not own:

- Module Views.
- Module dirty state.
- Data tasks.
- Object storage writes.
- Lifecycle side effects.
- Broadcast ack conditions.

RecoveryStorage calls every module for every persisted WAL message. Modules return barriers. RecoveryStorage registers the message physical point and returned barriers into CheckpointManager.

RecoveryStorage background work may trigger modules to submit tasks, but the module decides what task to generate and how its View changes.

### 4.2 CheckpointManager

CheckpointManager owns ordered physical checkpoint advancement.

It tracks two physical lanes:

- **Meta physical checkpoint**: WAL restart point for Meta recovery.
- **Data physical checkpoint**: WAL restart point for Data recovery and the WAL retention point.

For each consumed message, RecoveryStorage registers a physical point and optional barriers:

```
physical point P + MetaBarrier
physical point P + DataBarrier
```

A physical lane advances only through the continuous prefix whose barriers have disappeared. A barrier disappears when the owning module has persisted the View state required for that physical point.

CheckpointManager does not know vchannel, segment, schema, tombstone, import, ack, object storage, or lifecycle semantics.

### 4.3 Scheduler

Scheduler is the asynchronous execution plane.

Modules submit tasks to Scheduler for:

- Data persistence.
- Lifecycle side effects.
- Broadcast ack.
- View persistence.
- Cleanup.

Scheduler is parallel by default. Task ordering is expressed through preconditions:

- Segment Data tasks are ordered per segment.
- VChannel TransformLog tasks are ordered per vchannel.
- View persist tasks are ordered per module or per owner according to module policy.
- Ack tasks are ordered by WAL ack order.
- Cleanup waits for persisted physical checkpoints to pass the retained tombstone timetick.

Scheduler does not understand module business keys. It only checks task preconditions.

### 4.4 GrowingModule

GrowingModule owns growing data recovery Views.

It owns:

- Views that include VChannel, schema, partition, and segment Meta.
- Views that include Segment insert buffers and L1 Data state.
- Views that include VChannel TransformLog buffers and L0 Data state.
- Retained tombstone state inside Views.
- Dirty Views.
- Growing Data tasks and View persistence tasks.

`ObserveMessage` is the only synchronous WAL consumption entry. It updates the corresponding View's Meta part and `MetaTimeTick`. It may append lightweight in-memory buffers, but it does not perform heavy durable work.

Data work is asynchronous. Segment flush, TransformLog flush, lifecycle commit, and tombstone cleanup are submitted to Scheduler according to GrowingModule policy or RecoveryStorage background triggering. After a Data task completes, GrowingModule updates the corresponding View's Data state and `DataTimeTick`.

Dirty Views are persisted asynchronously. After a dirty View snapshot is persisted to ETCD, GrowingModule updates the owner MetaBarrier and DataBarrier from the snapshot's `MetaTimeTick` and `DataTimeTick`.

VChannel names are not reusable. Schema history is VChannel child state. Schema changes append schema versions and retain old versions for historical `ObserveMessage` scans. Schema is not an independent tombstone key space; final VChannel cleanup removes the VChannel owner key and all schema keys under the VChannel prefix.

### 4.5 AckModule

AckModule owns StreamingNode local broadcast acknowledgement.

Ack is a Data-side effect because it calls the coordinator broadcast Ack API. For every persisted message with a `BroadcastHeader`, AckModule submits an ack task in MetaAndData mode and returns a DataBarrier. The DataBarrier disappears only after the Ack API succeeds.

Ack task preconditions include:

- Previous ack task completion.
- Message semantic preconditions that require related GrowingModule Data progress.

Ack preconditions are defined by message type and message scope. VChannel-scoped flush/drop/schema-changing messages wait for the related vchannel's GrowingModule Data frontier. Partition-scoped drop messages wait for the related partition Data frontier. PChannel-wide flush-style messages wait for the all-local GrowingModule Data frontier. Broadcast messages without growing data dependency wait only for previous ack task completion.

Examples:

- Ack tasks are ordered by previous ack task completion.
- DropCollection waits for the target vchannel's GrowingModule Data frontier.
- DropPartition waits for the target vchannel's partition Data frontier.
- FlushAll waits for all local GrowingModule Data frontiers.
- CommitImport waits only for previous ack task completion.

AckModule does not call GrowingModule to flush data or persist Views. It only waits on module barriers/frontiers.

## 5. Meta Chain and Data Chain

### 5.1 Meta Chain

The Meta chain describes the synchronous View update chain.

It includes:

- VChannel Meta in View.
- Schema history in View.
- Partition Meta in View.
- Segment assignment/stat Meta in View.
- Retained tombstone Meta in View.

Workflow:

```
ObserveMessage(M)
  -> module updates View.Meta
  -> module updates View.MetaTimeTick to M.TimeTick
  -> View becomes dirty
  -> View persist task writes dirty View snapshot to ETCD
  -> module updates MetaBarrier
```

The Meta physical checkpoint can advance only after CheckpointManager sees that the corresponding MetaBarrier no longer blocks the ordered prefix.

### 5.2 Data Chain

The Data chain describes the asynchronous View update chain.

It includes:

- Insert L1 object storage output.
- Segment lifecycle side effects.
- Delete TransformLog L0 output.
- L0 lifecycle side effects.
- Broadcast ack.
- View.Data updates that record durable Data progress.

Workflow:

```
ObserveMessage(M)
  -> module submits Data task if needed
  -> Scheduler runs Data task when preconditions are ready
  -> Data task completes durable side effect
  -> module updates View.Data and View.DataTimeTick
  -> View becomes dirty
  -> View persist task writes dirty View snapshot to ETCD
  -> module updates DataBarrier
```

The Data physical checkpoint can advance only after:

- Meta physical progress has covered the same ordered prefix.
- The corresponding DataBarrier no longer blocks the prefix.

The persisted Data physical checkpoint is the WAL retention point.

## 6. Normal Workflow

### 6.1 WAL Open

```
Load WALCheckpoint
Load module View snapshots from ETCD
Construct module Views from snapshots
Construct modules in MetaOnly mode
Run bounded Meta scanner from Meta physical checkpoint to open tail
ObserveMessage rebuilds View.Meta and dirty Views
Switch modules into MetaAndData mode
Start data/live scanner from Data physical checkpoint
WAL open succeeds
```

Meta recovery and live consumption use the same `ObserveMessage` implementation. During the bounded Meta scanner, modules are in MetaOnly mode, so `ObserveMessage` updates only the Meta part of Views and does not submit Data-chain work. After modules switch into MetaAndData mode, the data/live scanner uses the same `ObserveMessage` entry and enables Data-chain buffering and task submission.

The Meta part of the recovered Views is returned as the Snapshot used by WAL Open. The data/live scanner does not need to catch up to the open tail before WAL open returns.

### 6.2 ObserveMessage

```
Scanner reads persisted message M
RecoveryStorage dispatches M to every module
Each module synchronously updates View.Meta and View.MetaTimeTick
Each module may append lightweight in-memory buffers
Each module may submit tasks according to policy
Each module returns Meta/Data barriers
RecoveryStorage registers M into CheckpointManager
```

ObserveMessage must not perform object storage writes, ETCD writes, lifecycle RPCs, broadcast RPCs, or long retry loops.

### 6.3 Data Task Completion

```
Scheduler runs module Data task
Task performs durable side effect
Module updates View.Data and View.DataTimeTick
View becomes dirty
RecoveryStorage background or module policy triggers View persist task
```

Data task completion updates View in memory. It does not directly advance a physical checkpoint.

### 6.4 View Persistence

```
Scheduler runs View persist task
Task persists dirty View snapshot to ETCD
Module updates MetaBarrier from persisted View snapshot's MetaTimeTick
Module updates DataBarrier from persisted View snapshot's DataTimeTick
CheckpointManager observes that barriers disappeared
RecoveryStorage persists new physical WALCheckpoint
```

View persistence is module-owned asynchronous work. RecoveryStorage only triggers and observes barriers.

### 6.5 Physical Checkpoint Persistence

```
CheckpointManager advances in-memory physical checkpoints
RecoveryStorage persists WALCheckpoint
RecoveryStorage truncates WAL by persisted Data physical checkpoint
```

WAL truncation never uses Meta physical checkpoint.

## 7. Failure Recovery Flow

Recovery restarts from persisted WALCheckpoint and module View snapshots persisted in ETCD.

| Failure point | Recovery behavior |
|---|---|
| Crash after ObserveMessage but before View persist | WALCheckpoint cannot cross the message because the View-backed barrier still blocks. Recovery scans the message again and rebuilds View.Meta through `ObserveMessage`. |
| Crash after Data side effect but before View persist | Data physical checkpoint cannot cross the message. The data scanner restarts from the older Data checkpoint and repeats or reconciles the Data task. |
| Crash after View persist but before WALCheckpoint persist | Recovery starts from an older physical checkpoint. The persisted View snapshot already records Meta/Data progress, so repeated `ObserveMessage` is skipped or handled idempotently by module state. |
| Crash after Meta physical checkpoint persist while Data checkpoint is older | Meta recovery starts later; the data scanner starts earlier. Retained Views and tombstones remain available for Data-chain observation. |
| Crash after Data physical checkpoint persist | The data scanner starts after that checkpoint. This is safe because Data checkpoint persistence happens only after Data barriers disappear. |
| Crash after broadcast ack before WALCheckpoint persist | Recovery may ack again. Broadcast ack is replayable and idempotent. |

## 8. Retained Tombstone View

Dropped or flushed objects remain in retained Views until Data progress reaches the close timetick.

Historical ObserveMessage filter scopes are:

- VChannel tombstone.
- Partition tombstone.
- Segment tombstone.

Schema is retained VChannel child state and is filtered by the owning VChannel tombstone. It is not an independent tombstone key space.

Tombstone state is generated in View only after the corresponding DataTimeTick reaches the close timetick. Retained tombstoned Views can be physically deleted only when:

```
Meta physical checkpoint > tombstone timetick
Data physical checkpoint > tombstone timetick
```

This guarantees neither scanner can restart from a point that still needs the retained View or historical-message filter.

## 9. Design Constraints

- View is the module-owned consistency view in memory and can be persisted to ETCD when dirty.
- Meta and Data are both parts of View.
- Every View has `MetaTimeTick` and `DataTimeTick`.
- `ObserveMessage` synchronously updates View.Meta and `MetaTimeTick`.
- Data tasks asynchronously update View.Data and `DataTimeTick`.
- Dirty Views are persisted by module-owned asynchronous tasks.
- MetaBarrier and DataBarrier advance only after the corresponding dirty View snapshot is persisted.
- RecoveryStorage persists only WALCheckpoint.
- CheckpointManager advances physical checkpoints after barriers disappear from the ordered prefix.
- Scheduler executes module-owned asynchronous work and uses preconditions for ordering.
- WAL truncation uses only the persisted Data physical checkpoint.
