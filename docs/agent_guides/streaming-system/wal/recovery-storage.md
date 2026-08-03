# RecoveryStorage

Persists WAL consumer state to the catalog (etcd) and object storage. **Core invariant**: from any WAL position + the corresponding persisted state, RecoveryStorage can replay the WAL forward and recover a fully consistent in-memory state.

## Persisted State

- **WALCheckpoint** (etcd): `MessageID` (= LastConfirmedMessageID of last consumed message), `TimeTick`, `ReplicateCheckpoint` (for secondary clusters), `AlterWalState` (for WAL backend migration).
- **VChannel metadata** (etcd): Per-VChannel collection info, partition list,
  schema history, lifecycle state, and the greatest sealed data version retained
  after obsolete segment assignments are cleaned.
- **Segment assignments** (etcd): Per-segment growing/flushed status with row
  count and binary size stats. Tombstoned assignments are deleted only after
  their sealed data version is covered by a persisted VChannel summary.
- **Segment data** (object storage): Sealed segment binlog, indexes, and stats files.

## Recovery Flow

1. **Persist recovery** (`recoverRecoveryInfoFromMeta`): Load checkpoint, VChannel metadata, and segment assignments from catalog in parallel.
2. **Recovery barrier append**: Append a persisted
   [RecoveryBarrier](../message/message-semantic-recovery-barrier.md) message as
   the first recovery WAL write for this PChannel. The append proves that the
   recovering node can write this WAL; on backends with writer fencing, currently
   Woodpecker, it also prevents old owners from appending later entries. If the
   append fails because the writer is fenced, recovery must stop and the node
   must not serve the PChannel.
3. **Stream recovery** (`recoverFromStream`): Build a `RecoveryStream` from the
   checkpoint's MessageID through the `RecoveryBarrier` message. Replay all
   messages to reconstruct in-memory state. Extract uncommitted `TxnBuffer`.
   Applying the empty `RecoveryBarrier` initializes or advances per-VChannel
   query MVCC for every VChannel that is live after replay reaches the barrier
   and makes the corresponding growing and transforming resources visible at the
   barrier TimeTick. Runtime-specific handling is defined by
   [StreamingNode Growing Segment Runtime Design](../../../design-docs/design_docs/qviews/snview/growing_segment_runtime.md)
   and
   [QueryNode QueryView Resource Preparation Design](../../../design-docs/design_docs/qviews/qnview/querynode_queryview_resource_preparation.md).

`RecoveryBarrier` avoids persisting per-VChannel query MVCC snapshots in the
checkpoint. The checkpoint remains focused on recovery position and durable WAL
state; the barrier establishes the query-resource baseline as part of recovery
replay.

## Key Packages

- `internal/streamingnode/server/wal/recovery/` — `RecoveryStorage`, `RecoverySnapshot`, WAL replay orchestration, meta recovery and background persist task
- `internal/streamingnode/server/wal/moduleapi/` — common RecoveryStorage module contracts, dirty snapshots, data checkpoints, and data frontiers
- `internal/streamingnode/server/wal/vchannel/` — VChannel metadata, schema history, partition lifecycle, and VChannel tombstones
- `internal/streamingnode/server/wal/vchannel/segment/` — growing segment assignment metadata, Insert/L1 persistence, segment lifecycle, and segment tombstones
- `internal/streamingnode/server/wal/vchannel/transformlog/` — Delete TransformLog storage, recovery, chunk replay, scanners, and truncation
- `internal/streamingnode/server/wal/checkpoint/` — WAL checkpoint manager, meta/data barriers and checkpoint advancement rules
