# RecoveryStorage

Persists WAL consumer state to the catalog (etcd) and object storage. The authoritative design is [WAL Recovery Architecture](../../../design-docs/design_docs/wal/wal-recovery-architecture.md) and its linked documents. **Core invariant**: the published checkpoint and persisted component state allow replay of the remaining WAL tail without losing data.

## Persisted State

- **WALCheckpoint** (etcd): safe `LastConfirmedMessageID` and logical `TimeTick`, publisher term, replication configuration/progress, and AlterWAL state. Publication is bounded by both AckTracker's successful continuous prefix and WALSummary's `LastAcked`.
- **VChannel metadata** (etcd): Per-VChannel collection info, partition list, schema history, state (NORMAL / DROPPED).
- **Segment assignments** (etcd): Per-segment growing/flushed status with row count and binary size stats.
- **Segment data** (object storage): SegmentView writes L1 binlogs and statistics; TransformLog materializes Delete records into L0. Index building is outside RecoveryStorage.
- **WALSummary** (object storage): PChannel-scoped immutable chunks and term manifests, storing keyed-write summaries and Delete records independently of source-message handles.

## Recovery Flow

1. RW WAL opening appends a RecoveryBarrier to fence the old writer.
2. **Metadata recovery** (`recoverRecoveryInfoFromMeta`): Claim the checkpoint with the assignment term, load component metadata and restore WALSummary; rebuild outstanding transform windows.
3. **Bounded recovery** (`runBoundedRecovery`): Observe the WAL from the checkpoint through the barrier and build the write-path snapshot and uncommitted `TxnBuffer`. Asynchronous persistence need not have finished.
4. Start live observation, AckTracker stall checks, independent Summary backlog checks, and catalog publication. Component snapshots precede checkpoint publication and WAL truncation. Poisoned messages remain incomplete and block the checkpoint.

Control may persist its latest state ahead of the global checkpoint, like a Segment snapshot. Its `control_checkpoint_time_tick` suppresses already covered control effects without skipping data replay. External effects still require idempotent retries when a crash precedes metadata publication. The bounded/live scanner handoff remains an open point documented in the architecture design.

## Key Packages

- `internal/streamingnode/server/wal/recovery/` — recovery orchestration, BroadcastAck, tail control and checkpoint publication
- `internal/streamingnode/server/wal/utility/` — checkpoint and recovery snapshot types
- `internal/streamingnode/server/wal/messageack/` — message completion and stall tracking
- `internal/streamingnode/server/wal/vchannel/` — VChannel metadata and component ownership
- `internal/streamingnode/server/wal/vchannel/segment/` — L1 persistence and final DataCoord commit
- `internal/streamingnode/server/wal/vchannel/transformlog/` — L0 materialization
- `internal/streamingnode/server/wal/walsummary/` — summary persistence, recovery and retention

The former `flusher/flusherimpl` path has been removed. A compatibility VChannel checkpoint updater still reports flush progress to DataCoord; it is not another recovery or truncation cursor.
