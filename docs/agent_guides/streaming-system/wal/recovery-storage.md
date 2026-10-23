# RecoveryStorage

Persists WAL consumer state to the catalog (etcd) and object storage. The authoritative design is [WAL Recovery Architecture](../../../design-docs/design_docs/wal/wal-recovery-architecture.md) and its linked documents. **Core invariant**: the published checkpoint and persisted component state allow replay of the remaining WAL tail without losing data.

## Persisted State

- **WALCheckpoint** (etcd): safe `LastConfirmedMessageID` and logical `TimeTick`, publisher term, replication configuration/progress, and AlterWAL state. Publication is bounded by both AckTracker's successful continuous prefix and WALSummary's `LastAcked`.
- **VChannel metadata** (etcd): Per-VChannel collection info, partition list, schema history, state (NORMAL / DROPPED).
- **Segment assignments** (etcd): Per-segment growing/flushed status with row count and binary size stats.
- **Segment data** (object storage): SegmentView writes L1 binlogs and statistics; [L0Materializer](../../../design-docs/design_docs/wal/l0_materializer.md) owns Delete-to-L0 materialization. Index building is outside RecoveryStorage.
- **WALSummary** (object storage): PChannel-scoped immutable chunks and term manifests, storing keyed-write summaries and Delete records independently of source-message handles.

## Recovery Flow

1. RW WAL opening appends a RecoveryBarrier to fence the old writer.
2. **Metadata recovery** (`recoverRecoveryInfoFromMeta`): Claim the checkpoint with the assignment term, load component metadata and restore WALSummary. WALMaterializer restores its cursor and rebuilds unmaterialized Delete handles through WAL replay.
3. **Startup recovery** (`runBoundedRecovery`): Use the ordinary scanner to observe the WAL from the checkpoint through this open's exact barrier. Capture the write-path snapshot and an independent copy of unfinished transaction builders, and restore idempotency snapshots from retained Summary history plus replayed records (retrying transient object reads until recovery or cancellation, and failing WAL open on permanent errors or cancellation); pause further raw input until write-path initialization finishes. Asynchronous persistence need not have finished.
4. Resume the same scanner exclusively after the barrier through the opener-provided WAB, retaining its ordering and transaction state. An empty WAB needs no additional persisted TimeTick to switch. WAB retains the live suffix until RecoveryStorage reports complete consumption; capacity/keepalive cannot evict that suffix. After observing each confirmation, including Summary and ordered VChannel query-barrier dispatch, mark its complete prefix eligible for WAB eviction. Capacity/TTL still determines actual eviction, and the latest durable confirmation is retained separately for catchup handoff without pinning consumed volatile barriers in the queue. This consumption boundary is separate from persistence completion. WAB occupancy directly controls producer backpressure; scanner catchup/tailing transitions do not. Run AckTracker stall checks, independent Summary backlog checks, and catalog publication. Component snapshots precede checkpoint publication and WAL truncation. Poisoned messages remain incomplete and block the checkpoint.

Control may persist its latest state ahead of the global checkpoint, like a Segment snapshot. Its `control_checkpoint_time_tick` suppresses already covered control effects without skipping data replay. External effects still require idempotent retries when a crash precedes metadata publication. Startup failure and normal shutdown close the retained stream, including when it is paused at the barrier.

The physical LastConfirmedMessageID may overlap the completed prefix. Startup
observation skips scanner output at or before the recovered checkpoint's
TimeTick, including broadcasts whose consuming ACK has already completed. This
filter runs after ordering and transaction assembly; raw transaction fragments
before the checkpoint remain available to assemble commits beyond it. The exact
RecoveryBarrier is still recognized independently of this observation filter.

Legacy DataCoord seek timestamps are inclusive replay positions, not completed
prefixes. Migration preserves the safe physical MessageID but publishes the
final V2 checkpoint with TimeTick zero. Replay therefore includes the legacy
boundary, even if the process restarts immediately after migration commits.
Component checkpoints and the independent control frontier retain their existing
values and provide deduplication. The preliminary legacy-format rewind keeps its
original seek timestamp so an interrupted migration can still rebuild component
baselines; only the final V2 global TimeTick is reset.

WAL append cancellation ends the writer lifecycle and requires this recovery flow
before writes resume. It does not prove that the interrupted record was absent.
A durable Commit is replayed as a completed transaction; an incomplete receiving
prefix reconstructs both TxnManager and ReplicatesManager from the same TxnBuffer;
a durable exclusive operation (including ManualFlush/FlushAll) removes the aborted
prefix in its scope, regardless of replication origin or the `_ur` marker.
Explicitly ignored topology records have no abort effect. The old session is not reset
for another Commit attempt on the cancelled writer. See
[Append retry and writer lifetime](../walbackend/walbackend.md#append-retry-and-writer-lifetime).

The temporary WAL L0 materializer retains Delete/Txn and explicit Flush handles.
Size, buffer age, explicit Flush and recovery-tail requests trigger output.
Earlier L1 segments must be registered in DataCoord, but need not be flushed.
L1/L0 Flush work joins via message handles and consuming-side acknowledgements;
Flush completion does not wait for global checkpoint publication. The global
checkpoint is persisted by RecoveryStorage, without periodic reporting to
DataCoord. The Summary reader materializer is retained for
future QueryView wiring and is not active simultaneously. Durable materialized
metadata still governs Summary GC and tombstone retirement.

## Key Packages

- `internal/streamingnode/server/wal/adaptor/` — shared scanner, startup boundary and durable WAL/WAB source switching
- `internal/streamingnode/server/wal/recovery/` — recovery orchestration, BroadcastAck, tail control and checkpoint publication
- `internal/streamingnode/server/wal/utility/` — checkpoint and recovery snapshot types
- `internal/streamingnode/server/wal/messageack/` — message completion and stall tracking
- `internal/streamingnode/server/wal/vchannel/` — VChannel metadata and component ownership
- `internal/streamingnode/server/wal/vchannel/segment/` — L1 persistence and final DataCoord commit
- `internal/streamingnode/server/wal/vchannel/l0materializer/` — window tracking and bounded Summary-to-L0 materialization
- `internal/streamingnode/server/wal/walsummary/` — summary persistence, recovery and retention

[TransformLog](../../../design-docs/design_docs/wal/transform_log.md) is a read-only subscription adaptor over Summary. Local SN bounded Delete replay is wired into QueryRuntime preparation via `walsummary.Stream`; subsequent live events arrive through VChannel observation. Remote/QN subscription integration remains planned. The adaptor owns neither WAL observation nor L0 materialization.

The former `flusher/flusherimpl` path and periodic VChannel checkpoint reporting
have been removed. Historical DataCoord checkpoints remain readable for legacy
recovery. Dropped-segment GC reads `WALCheckpoint.time_tick` directly from
StreamingNode catalog once per PChannel per sweep, reusing the value for all
its segments. Missing checkpoints and read failures prevent reclamation for
that PChannel. Native RecoveryStorage checkpoint persistence, replay and
truncation remain independent.
