# StreamingNode VChannel WAL Input View

`VChannelWALView` is an internal preparation DTO built by
`VChannelRecoveryModule` for QueryRuntime. It is not a RecoveryStorage API and
does not participate in the global checkpoint protocol.

## 1. Ownership

```text
QueryViewStateMachine.Acquire
  -> PChannelRecoveryManager.Acquire
  -> VChannelRecoveryModule.queryWALViewLocked
  -> QueryRuntime.Initialize
  -> QueryRuntimeModule.Prepare
```

The VChannel module already owns the inputs needed for a no-gap view:

- VChannel and schema history;
- growing Segment stable and pending state;
- Segment DataVersion summaries;
- TransformLog state and stream;
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
  -> capture TransformLog readable state
  -> construct VChannelWALView
  -> install QueryRuntime in Preparing state
release VChannel lock
```

Messages observed before capture are represented by stable objects, pending
buffers, pending tasks, or TransformLog state. Messages observed afterward see
the installed QueryRuntime and enter its pending event queue.

QueryRuntime receives ordinary immutable copies and never retains Message Ack
handles.

## 4. Startup Readiness

The single recovery scanner reaches RecoveryBarrier before the startup
write-path snapshot is published. QueryRuntime preparation may additionally
wait for actual component conditions, including:

- every retained flushed Segment has `SealedAtDataVersion`;
- required TransformLog subscription start points remain readable;
- captured schema and segment state form a consistent VChannel snapshot.

It does not wait for a metadata scanner, data scanner, Observe-mode transition,
or second checkpoint.

## 5. DataVersion Selection

Each segment is classified independently against the target QueryView
DataVersion:

```text
GROWING
    -> growing Segment snapshot
FLUSHED && SealedAtDataVersion > target
    -> retained growing-visible snapshot
FLUSHED && SealedAtDataVersion <= target
    -> flushed Segment set
```

There is no VChannel-level aggregate DataVersion fence. DataVersion may advance
because another VChannel flushed independently.

## 6. Invariants

1. VChannelRecoveryModule is the only builder of VChannelWALView.
2. View capture and live observer installation have no message gap.
3. QueryRuntime owns no RecoveryStorage handle.
4. Runtime MVCC frontiers are not global recovery checkpoints.
5. Readiness depends on concrete component state, not dual recovery phases.
