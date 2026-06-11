# RecoveryStorage

Persists WAL consumer state to the catalog (etcd) and object storage. **Core invariant**: from any WAL position + the corresponding persisted state, RecoveryStorage can replay the WAL forward and recover a fully consistent in-memory state.

## Persisted State

- **WALCheckpoint** (etcd): `MessageID` (= LastConfirmedMessageID of last consumed message), `TimeTick`, `ReplicateCheckpoint` (for secondary clusters), `AlterWalState` (for WAL backend migration).
- **VChannel metadata** (etcd): Per-VChannel collection info, partition list, schema history, state (NORMAL / DROPPED).
- **Segment assignments** (etcd): Per-segment growing/flushed status with row count and binary size stats.
- **Segment data** (object storage): Sealed segment binlog, indexes, and stats files.

## Recovery Flow

1. **Persist recovery** (`recoverRecoveryInfoFromMeta`): Load checkpoint, VChannel metadata, and segment assignments from catalog in parallel.
2. **Stream recovery** (`recoverFromStream`): Build a `RecoveryStream` from the checkpoint's MessageID to the current WAL position. Replay all messages to reconstruct in-memory state. Extract uncommitted `TxnBuffer`.

## Key Packages

- `internal/streamingnode/server/wal/recovery/` — `RecoveryStorage`, `RecoverySnapshot`, WAL replay orchestration, meta recovery and background persist task
- `internal/streamingnode/server/wal/moduleapi/` — common RecoveryStorage module contracts, dirty snapshots, data checkpoints, and data frontiers
- `internal/streamingnode/server/wal/vchannel/` — VChannel metadata, schema history, partition lifecycle, and VChannel tombstones
- `internal/streamingnode/server/wal/segment/` — growing segment assignment metadata, Insert/L1 persistence, segment lifecycle, and segment tombstones
- `internal/streamingnode/server/wal/transformlog/` — Delete TransformLog storage, recovery, chunk replay, scanners, and truncation
- `internal/streamingnode/server/wal/checkpoint/` — WAL checkpoint manager, meta/data barriers and checkpoint advancement rules
