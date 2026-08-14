# RecoveryStorage

Persists WAL consumer state to the catalog (etcd) and object storage. **Core invariant**: from any WAL position + the corresponding persisted state, RecoveryStorage can replay the WAL forward and recover a fully consistent in-memory state.

## Persisted State

- **WALCheckpoint** (etcd): `TimeTick` is the logical consumed-through
  boundary; `MessageID` is that message's `LastConfirmedMessageID`, used as a
  conservative resume anchor with `DeliverPolicyStartFrom`. The checkpoint also
  contains `ReplicateCheckpoint` for secondary clusters and `AlterWalState` for
  WAL backend migration.
- **VChannel metadata** (etcd): Per-VChannel collection info, partition list,
  schema history, lifecycle state, and the greatest sealed data version retained
  after obsolete segment assignments are cleaned.
- **Segment assignments** (etcd): Per-segment growing/flushed status with row
  count and binary size stats. Tombstoned assignments are deleted only after
  their sealed data version is covered by a persisted VChannel summary.
- **Segment data** (object storage): Sealed segment binlog, indexes, and stats files.
- **TransformLog metadata** (etcd): Per-VChannel durable chunk range,
  checkpoint, materialization, and truncation cursors.
- **TransformLog chunks** (object storage): Durable Delete/transform payloads
  stored in deterministic per-VChannel chunk paths.

## Recovery Flow

1. **Recovery barrier append**: After reading the persisted checkpoint, append a
   persisted
   [RecoveryBarrier](../message/message-semantic-recovery-barrier.md) message as
   the first recovery WAL write for this PChannel. The append proves that the
   recovering node can write this WAL; on backends with writer fencing, currently
   Woodpecker, it also prevents old owners from appending later entries. If the
   append fails because the writer is fenced, recovery must stop and the node
   must not serve the PChannel.
2. **Persisted-state recovery** (`recoverRecoveryInfoFromMeta`): Load VChannel,
   Segment, and TransformLog metadata from the catalog. Each VChannel's initial
   query data frontier is the persisted Data checkpoint TimeTick.
3. **Bounded metadata recovery** (`runBoundedMetaScannerAndSwitchModules`): Scan
   from the checkpoint MessageID through the startup `RecoveryBarrier` with
   Meta-only envelopes. This reconstructs current metadata and the uncommitted
   `TxnBuffer`, but it does not claim that Insert or Transform data has been
   replayed through the barrier. Each envelope receives a temporary
   `OwnedImmutableMessage`, clones one Retained dispatch handle for the
   PChannel manager, and creates no Tracker entry.
4. **Data replay and QueryView recovery**: Switch modules into MetaAndData mode
   and start the DataScanner from the persisted Data checkpoint. Persisted
   QueryViews may build their QueryRuntime concurrently from the current
   VChannel WALView; they do not wait for DataScanner to reach the startup
   barrier. Runtime-specific handling is defined by
   [StreamingNode Growing Segment Runtime Design](../../../design-docs/design_docs/qviews/snview/growing_segment_runtime.md)
   and
   [QueryNode QueryView Resource Preparation Design](../../../design-docs/design_docs/qviews/qnview/querynode_queryview_resource_preparation.md).

`RecoveryBarrier` remains the writer-fencing proof and the bounded Meta-only
scan endpoint. It is not a QueryRuntime readiness fence. QueryRuntime's baseline
comes from the WALView's data-observed and TransformLog frontiers.

## Checkpoint Persistence

RecoveryStorage freezes a persist-batch boundary before consuming module dirty
snapshots:

- Meta checkpoint is the latest completely observed WAL point frozen for the
  batch.
- Data checkpoint is the minimum of that Meta point and the frozen continuous
  Message Ack completed frontier. It is clamped against the persisted Data
  checkpoint and never moves backward when `DeliverPolicyStartFrom` replays an
  already completed message.
- All captured DirtySnapshots are persisted before the batch checkpoint.
- Every actual Segment and TransformLog data consumer retains a direct message
  handle until its object-storage work succeeds and its metadata changes are
  marked dirty.
- BroadcastAck takes the Owner after dispatch. It immediately releases an
  ordinary message; for a broadcast it registers a one-shot exclusive callback
  that nonblockingly wakes one background dispatcher. The dispatcher preserves
  observation order for conflicting ResourceKeys and submits non-conflicting
  Acks concurrently. It releases the Owner only after Coordinator Ack succeeds.
  Failed Acks retain the same Owner and ResourceKey claim through retry.
- `AckSyncUp` disables Coordinator FastAck and waits for the RecoveryStorage
  consumer Ack; it does not require checkpoint persistence before that Ack.
- Retry, cancellation, and close keep incomplete handles retained. Restart
  rebuilds tracked message wrappers from the persisted Data checkpoint.

See
[WAL Message Ack Design](../../../design-docs/design_docs/wal/message_ack.md).

## Key Packages

- `internal/streamingnode/server/wal/recovery/` — `RecoveryStorage`, `RecoverySnapshot`, WAL replay orchestration, meta recovery and background persist task
- `internal/streamingnode/server/wal/moduleapi/` — recovery snapshot and task runtime value types; it has no generic recovery consumer interface
- `internal/streamingnode/server/wal/messageack/` — ordered tracked entries,
  message-reference cleanup, and continuous completion tracking
- `pkg/streaming/util/message/` — Owned/Retained immutable message wrappers and
  typed handle specializations
- `internal/streamingnode/server/wal/vchannel/` — VChannel metadata, schema history, partition lifecycle, and VChannel tombstones
- `internal/streamingnode/server/wal/vchannel/segment/` — growing segment assignment metadata, Insert/L1 persistence, segment lifecycle, and segment tombstones
- `internal/streamingnode/server/wal/vchannel/transformlog/` — Delete TransformLog storage, recovery, chunk replay, scanners, and truncation
- `internal/streamingnode/server/wal/recovery/` owns frozen persist-batch points
  and checkpoint advancement; there is no separate checkpoint barrier manager.
