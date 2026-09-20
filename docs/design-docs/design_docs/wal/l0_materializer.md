# WAL L0 Materializer

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031

## 1. Current Integration

Until QueryView is enabled, VChannelRecoveryModule owns one `WALMaterializer`.
It observes retained WAL messages and holds Delete handles until L0 output and
DataCoord registration succeed. Together with SegmentView's temporary growing
SaveBinlogPaths publication, this makes the published global WAL checkpoint a
safe DataCoord channel checkpoint for legacy query recovery.

The [Summary reader materializer](summary_l0_materializer.md) remains in the
same package with its tests, but is not instantiated by the current manager.
The implementations are mutually exclusive. There is no user enable switch.
After QueryView is enabled, remove WALMaterializer, cp_updater and the growing
publication bridge, and reconnect the Summary consumer.

## 2. Observation and Ownership

`ObserveMessage(RetainedImmutableMessage)` runs after Segment observation.

- Delete and committed Txn containing Delete retain one outer-message clone.
- Pure Insert, ordinary TimeTick, RecoveryBarrier and generic DDL retain no L0
  handle and do not force output.
- ManualFlush, FlushAll, DropCollection, DropPartition, TruncateCollection,
  CreateSnapshot and AlterWAL retain a clone and force completion through their message boundary.
- Ordinary single-Segment Flush only flushes that Segment, matching master.

Each VChannel has an ordered pending buffer and at most one active serial task.
The buffer holds immutable WAL handles, not another durable record store.
Mixed transactions count only their Delete payloads for capacity admission;
all Delete children use the outer commit TimeTick and complete together.

The WAL consumer passes runtime-only positions to the output writer. Each
physical L0 group's StartPosition comes from its first Delete's TimeTick and
LastConfirmedMessageID; all groups use the captured batch-end message's complete
position as their checkpoint. Transaction positions come from the outer Txn,
never an individual child or the raw Commit message ID. Positions include the
consumer VChannel and WAL name, including for PChannel-wide Flush messages.
Summary storage gains no MessageID fields. Its retained consumer still supplies
only timestamps for future QueryView integration.

Snapshot replay filters against the materialized cursor, independently of the
VChannel metadata checkpoint. Observed positions suppress duplicate pending work.

## 3. Triggers

| Trigger | Rule |
|---|---|
| Capacity | Pending Delete bytes reach `dataNode.segment.deleteBufBytes`. |
| Age | The oldest pending buffer exceeds `dataNode.segment.syncPeriod`. |
| Explicit completion | A Flush/lifecycle message requires the preceding Delete prefix. |
| Recovery tail | `RequestPersistThrough` requests stalled or pressure-blocking data. |

The production config sources are `DataNodeCfg.FlushDeleteBufferBytes` and
`DataNodeCfg.SyncPeriod`, as in master's writebuffer policies. Shard-layer
`FlushL0MaxLifetime/MaxSize` are not this consumer's age/size admission policy.
`FlushL0MaxRowNum` continues to bound physical output groups.

One periodic worker per PChannel checks all VChannels once per second. It runs
independently of WAL traffic because idle non-persisted TimeTicks are filtered
by RecoveryStorage. New arrivals do not reset the oldest buffer age.

Admission freezes a finite batch. Messages arriving during execution accumulate
in the next buffer; completion schedules that buffer only when capacity or a
captured force request permits it. Every explicit WAL flush/lifecycle message
is a hard batch boundary: even if queued behind an active task, select only the prefix through the first
pending ManualFlush, FlushAll, DropCollection, DropPartition, TruncateCollection,
AlterWAL or CreateSnapshot, retaining later messages and their byte accounting
for the next batch. Whole L0 files must not mix deletes across these boundaries.
Capacity, age and RequestPersistThrough keep their existing trigger policies;
they do not introduce additional batch boundaries.
Empty explicit batches advance metadata without writing an empty L0. TimeTick progress alone does not write metadata or
create empty output merely to match the global checkpoint.

Summary remains permanently enabled, with independent persistence/backlog and
LastAcked gating. Its materialization-request callback is disconnected for this
consumer; it does not add a competing materialization policy.

## 4. L1 Registration, Not L1 Flush

L0 output can precede L1 final flush. DataCoord's L0 compaction policy waits on
the earliest growing segment start position before merging eligible L0 output.

Because SegmentView's initial AllocSegment is asynchronous, an L0 task must
first check that every earlier L1 has completed growing registration. A stable
Segment checkpoint at or after CreateSegment proves this. An unregistered L1
returns scheduler delay; it does not block a worker or force L1 flush. A later
CreateSegment cannot invalidate a captured earlier L0 batch.

Explicit Flush tasks on L1 and L0 execute independently. Their clones of the
same message join through Tracker/BroadcastAck. There is no VChannel-wide L1
creation upper bound on L0 output and no second L1 completion queue in L0.
DropPartition keeps the existing routing that flushes all earlier VChannel
segments while logically dropping only the requested partition.

## 5. Completion and Recovery

The sequence is:

```
write all L0 outputs and register them with DataCoord
  -> install dirty VChannelMeta.transform_materialized_time_tick
  -> release covered Delete/Flush handles
  -> publish component snapshots before the global WAL checkpoint
```

Retries keep handles and the fixed batch; unfinished cancellation never means
successful completion. The reused output writer may repeat physical output
following a partial failure or crash, as with the Summary consumer; this is not
physical exactly-once object creation. The logical cursor advances only after
all partition/output groups succeed.

Restart restores the materialized cursor and rebuilds remaining Delete/Flush
handles from the single WAL replay. Unmaterialized Delete messages cannot be
behind the global checkpoint in this runtime. There is no new persisted request
position, physical cursor, or intermediate branch-format compatibility path.

Both full and base-only VChannel snapshot callbacks continue to report their
captured durable materialized position to Summary for GC. In-memory completion
alone cannot authorize deletion. Tombstones remain until Summary has durably
retired the relevant history.

## 6. Checkpoint Reporting

cp_updater reports the published global checkpoint's original MessageID and
TimeTick together for each VChannel. It has no candidate queue, materialized
TimeTick minimum, or completed-Flush fallback. RPC failure retries a currently
published point on the next tick.

Insert registration and L0 completion are already required by the message
handles before checkpoint publication. No additional requirement forces an L0
cursor to equal checkpoint TimeTick across a range containing no Deletes.
A blocked VChannel can hold the PChannel prefix and other VChannels' reported
progress; this is the deliberate cost of the shared checkpoint.

## 7. Validation

Cover size/age/explicit/stall triggers, complete mixed Txns, frozen batches,
retry and cancellation, replay before/after materialized metadata persistence,
L0-before-L1-flush with registration delay, both L1/L0 Flush completion orders,
empty Flush, checkpoint pair reporting, and Drop/Summary retirement.
Live validation must include growing Inserts plus Deletes, reload, SN/QN crash
recovery, and enabled L0 compaction. WAL physical Truncate integration remains
the separately deferred work.
