# StreamingNode VChannel WAL Input View

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

`VChannelWALView` is an internal preparation DTO built by
`VChannelRecoveryModule` for QueryRuntime. It is not a RecoveryStorage API and
does not participate in the global checkpoint protocol.

> Status: design intent. `VChannelWALView` and the QueryRuntime integration
> chain (`QueryViewStateMachine.Acquire` → `PChannelRecoveryManager.Acquire` →
> `VChannelRecoveryModule.queryWALViewLocked` → `QueryRuntime.Initialize`) are
> **not yet implemented** in the current code; they are pending the qviews
> feature (#51887). TransformLog subscriptions are also a future integration,
> independent of the [L0Materializer](l0_materializer.md) split targeted by this PR.

## 1. Ownership

```text
QueryViewStateMachine.Acquire
  -> PChannelRecoveryManager.Acquire
  -> VChannelRecoveryModule.queryWALViewLocked
  -> QueryRuntime.Initialize
  -> QueryRuntimeModule.Prepare
```

The VChannel module must coordinate these inputs for a no-gap view:

- VChannel and schema history;
- growing Segment stable and pending state;
- Segment lifecycle and durable commit state;
- WALSummary readable history through the future TransformLog adaptor;
- the live message observation path.

## 2. Runtime Frontiers

The view may contain runtime Growing and Transform frontiers used for MVCC.
These are not RecoveryStorage checkpoints:

- they do not start WAL replay;
- they may describe observed in-memory pending state;
- QueryRuntime waits on them according to query-plan TimeTicks;
- they are reconstructed from component snapshots plus the one WAL replay.

The persisted global checkpoint is only the initial lower bound for startup
observation. Component `checkpoint_time_tick` fields independently suppress
effects already represented by snapshots.

## 3. No-Gap Capture

WAL-view capture and QueryRuntime registration use the same VChannel lock:

```text
hold VChannel lock
  -> capture stable and pending Segment state
  -> capture the Transform replay boundary required by the observed snapshot
  -> protect its historical start point in Summary retention
  -> construct VChannelWALView
  -> install QueryRuntime in Preparing state
release VChannel lock
```

Messages observed before capture are represented by stable objects, pending
buffers, pending tasks, or WALSummary records. Messages observed afterward see
the installed QueryRuntime and enter its pending event queue.

The captured Transform boundary describes the snapshot's required WAL prefix,
not L0Materializer's cursor. Bounded replay through the future TransformLog
adaptor must wait until Summary can completely provide that range before
reporting SyncUp/completion. Do not lower the required end because a sampled
Summary frontier is behind, or raise it to recovered Summary history ahead of
VChannel observation. The target observation order installs Summary records
before VChannel state/window updates; snapshot capture must preserve the same
no-gap guarantee when future query wiring is added.

Protect the history before GC can remove it and hold that requirement through
preparation. An already truncated start is an error, not an empty replay.
Neither L0 completion nor a subscription's delivery cursor proves that retained
QueryViews no longer need historical Delete data.

QueryRuntime receives ordinary immutable copies and never retains Message Ack
handles.

## 4. Startup Readiness

The single recovery scanner reaches RecoveryBarrier before the startup
write-path snapshot is published. QueryRuntime preparation may additionally
wait for actual component conditions, including:

- every retained flushed Segment has `l1_commit_done`;
- required TransformLog subscription start points remain readable;
- captured schema and segment state form a consistent VChannel snapshot.

It does not wait for a metadata scanner, data scanner, Observe-mode transition,
or second checkpoint.

## 5. Segment Lifecycle Selection

Each segment is classified independently from its durable lifecycle state:

```text
GROWING
    -> growing Segment snapshot
FLUSHED && !l1_commit_done
    -> retry or wait for the idempotent DataCoord final commit
FLUSHED && l1_commit_done
    -> DataCoord already owns the final full-binlog state
```

There is no second recovery checkpoint tied to this lifecycle classification.

## 6. Invariants

1. VChannelRecoveryModule is the only builder of VChannelWALView.
2. View capture and live observer installation have no message gap.
3. QueryRuntime owns no RecoveryStorage handle.
4. Runtime MVCC frontiers are not global recovery checkpoints.
5. Readiness depends on concrete component state, not dual recovery phases.
