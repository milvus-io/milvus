# RecoveryStorage

Persists WAL consumer state to the catalog (etcd) and object storage. **Core invariant**: from any WAL position + the corresponding persisted state, RecoveryStorage can replay the WAL forward and recover a fully consistent in-memory state.

## Persisted State

- **WALCheckpoint** (etcd): `MessageID` (= LastConfirmedMessageID of last consumed message), `TimeTick`, `ReplicateCheckpoint` (for secondary clusters), `AlterWalState` (for WAL backend migration).
- **VChannel metadata** (etcd): Per-VChannel collection info, partition list, schema history, state (NORMAL / DROPPED / SPLITTED), plus `split_time_tick` and `retired` for a split source.
- **Segment assignments** (etcd): Per-segment growing/flushed status with row count and binary size stats.
- **Segment data** (object storage): Sealed segment binlog, indexes, and stats files.

## Recovery Flow

1. **Persist recovery** (`recoverRecoveryInfoFromMeta`): Load checkpoint, VChannel metadata, and segment assignments from catalog in parallel.
2. **Stream recovery** (`recoverFromStream`): Build a `RecoveryStream` from the checkpoint's MessageID to the current WAL position. Replay all messages to reconstruct in-memory state. Extract uncommitted `TxnBuffer`.

## SPLITTED VChannels

A shard split's source VChannel goes to **SPLITTED**, not DROPPED, and stays there. The source replica of `SplitShard` sets the state and records `T_switch` in `split_time_tick` (a later fence record of the *same* split task raises it; the state is already SPLITTED, so nothing is re-fenced from scratch). DROPPED is wrong for it in two ways: a background task would call DataCoord's `DropVirtualChannel` for a channel the split coordinator retires itself, and once the row left the catalog a restart would lose the fence the [shard manager](shard-management.md) rebuilds its tombstone from.

Collection happens later and locally, on two conditions:

1. the **retire replica** — the `AlterCollection(shard_split_routing)` whose post-image no longer names this VChannel — sets `retired` (`ObserveRetire`, only legal on a SPLITTED VChannel);
2. the **flusher checkpoint** for this PChannel reaches `split_time_tick`, proving nothing of the source is left to replay.

`ConsumeDirtyAndGetSnapshot` then returns a snapshot rewritten to **DROPPED with `retired` still set**, which is what actually deletes the catalog row — the catalog has no "retired and drained" concept of its own. This is emitted even when the VChannel is not dirty, because `retired` is typically persisted long before the checkpoint catches up, and without that round the row would linger in etcd forever. `dropAllVirtualChannel` reads `retired` back off a DROPPED snapshot to tell it apart from a genuine drop and skips `DropVirtualChannel` for it. Retired-but-not-yet-collected VChannels are tracked in `retiredVChannels` so the persist gate keeps re-checking even if nothing else touches the PChannel again.

## Key Packages

- `internal/streamingnode/server/wal/recovery/` — `RecoveryStorage`, `RecoverySnapshot`, `WALCheckpoint`, background persist task
- `internal/streamingnode/server/flusher/flusherimpl/` — `WALFlusherImpl`, segment data flush to object storage
