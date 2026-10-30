# WAL L0 Materializer

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031

## 1. Current Integration

Until QueryView is enabled, VChannelRecoveryModule owns one `WALMaterializer`.
It observes retained WAL messages and holds Delete handles until L0 output and
DataCoord registration succeed. Together with SegmentView's temporary growing
SaveBinlogPaths publication, this protects the recoverability of the WAL
prefix covered by the published global checkpoint.

The [Summary reader materializer](summary_l0_materializer.md) remains in the
same package with its tests, but is not instantiated by the current manager.
The implementations are mutually exclusive. There is no user enable switch.
After QueryView is enabled, remove WALMaterializer and the growing
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

## 4. Final L1 Publication Before Materialization

Materialization through T requires every L1 created before T in the VChannel,
across every partition, to complete final DataCoord publication or retirement.
Initial AllocSegment registration or a durable first pack is insufficient.
The owner checks final-commit state, including recovered `sealed_at_data_version`;
concurrent final Flush RPCs remain blockers until acknowledged. A later
CreateSegment cannot invalidate the captured earlier batch.

A blocked task returns scheduler delay, releasing its worker. L1 tasks run
independently and never wait for L0; L0 retries after L1 publication. Explicit
Flush message handles join both completions before acknowledging the request.
Capacity/idle materialization also observes this prerequisite, so open earlier
L1 can delay it until the ordinary segment seal/Flush path completes.

The shard manager's DropPartition API fences allocation for every earlier
Segment in the Collection after successful WAL append while the VChannel
exclusive append lock is held, matching its VChannel-wide persistence scope.
Only the requested partition is logically dropped; surviving partitions allocate
new Segments for later Inserts.

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

### Registration errors and WAL ownership

DataCoord distinguishes lifecycle retirement from a missing assignment. A
persisted dropped-channel checkpoint permits `ChannelNotFound`; the consumer
can finish because that channel's data lifecycle has ended. An unavailable WAL
assignment returns `ChannelNotAvailable`, and lookup failures propagate as
errors. Neither means registration completed.

Publication continues to compare only the publisher NodeID against the current
WAL owner. A mismatch returns `ChannelMisrouted`. No assignment term or new RPC
field is introduced. This retains the existing limitation for reassignment back
to the same NodeID.

On `ChannelMisrouted`, the old WALMaterializer terminates, poisons and releases
its active and queued Delete/Flush handles, and poisons later relevant messages.
It does not advance the materialized cursor, install completion metadata, or
acknowledge the failed WAL prefix. Poison is local to that consumer; a new owner
replays the original messages from the published checkpoint. L1 publication
uses the same ownership error to enter its existing terminal/poison path.

Other registration failures retain the fixed batch and return scheduler delay.
The RecoveryStorage MetaWriter makes one registration attempt per task execution;
the coordinator client's internal RPC retries are also bounded. The scheduler
owns subsequent attempts. Only an actual successful registration or confirmed
lifecycle retirement allows normal handle release.

Restart restores the materialized cursor and rebuilds remaining Delete/Flush
handles from the single WAL replay. Unmaterialized Delete messages cannot be
behind the global checkpoint in this runtime. There is no new persisted request
position, physical cursor, or intermediate branch-format compatibility path.

Both full and base-only VChannel snapshot callbacks continue to report their
captured durable materialized position to Summary for GC. In-memory completion
alone cannot authorize deletion. Tombstones remain until Summary has durably
retired the relevant history.

## 6. Checkpoint And Flush Completion

RecoveryStorage persists the global checkpoint after its covered component
snapshots. Insert registration and L0 completion are required by the retained
message handles before publication. No additional requirement forces an L0
cursor to equal checkpoint TimeTick across a range containing no Deletes.

There is no periodic per-VChannel checkpoint reporting to DataCoord. Explicit
Flush completion joins L1/L0 work through message handles and consuming-side
acknowledgements; unrelated VChannels holding the global checkpoint cannot
prevent that Flush from completing. WAL replay and truncation continue to use
the durable global checkpoint.

## 7. Validation

Cover size/age/explicit/stall triggers, complete mixed Txns, frozen batches,
retry and cancellation, replay before/after materialized metadata persistence,
L0 retry before final L1 publication, registration-insufficient and cross-partition gates,
empty Flush, durable checkpoint publication, and Drop/Summary retirement.
Live validation must include growing Inserts plus Deletes, reload, SN/QN crash
recovery, and enabled L0 compaction. WAL physical Truncate integration remains
the separately deferred work.

## Runtime Closing And Tombstone Publication

Drop/Flush observation first sets a runtime-only closing boundary. Existing
asynchronous work continues; completion callbacks must not be suppressed merely
because the object is closing. Stable tombstones become dirty only after all
boundary dependencies finish, before the corresponding retained handles release.
VChannel Drop joins both Segment final commits and L0 completion. Metadata
publication cannot cross an unfinished Partition Drop. See
[VChannel lifecycle](vchannel_view_module.md#runtime-close-and-stable-tombstones)
and [Segment Flush](segment_view_module.md#34-flush).

Global checkpoint progress and Summary retirement gate final metadata GC, not
local task completion or tombstone installation. Shutdown does not complete
pending closes; WAL replay reconstructs them.
