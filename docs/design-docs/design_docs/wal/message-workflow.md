# Message Workflow

This document describes how RecoveryStorage modules handle each consumer-observable persisted WAL message type. Transaction control messages are consumed by the transaction assembly layer and are not delivered to RecoveryStorage modules. Deprecated message types are not listed.

Common rules:

- RecoveryStorage dispatches every persisted message to every module.
- Recovery scans and live consumption use the same module `ObserveMessage` implementation.
- In MetaOnly mode, `ObserveMessage` updates only View.Meta and does not submit Data-chain work.
- In MetaAndData mode, `ObserveMessage` enables Data-chain buffering and task submission.
- Each data module updates its own View.Meta synchronously in `ObserveMessage`.
- Each data module updates its own View.Data asynchronously through Scheduler
  tasks.
- Dirty snapshots are consumed and persisted by RecoveryStorage. After
  persistence succeeds, RecoveryStorage calls `DirtySnapshot.MarkPersisted()`.
- CheckpointManager advances physical checkpoints only after returned barriers disappear.
- AckModule submits an ack task and returns a DataBarrier for every persisted message with a `BroadcastHeader`.
- AckModule's DataBarrier disappears only after the coordinator broadcast Ack API succeeds.
- AckModule preconditions are defined by message type and message scope.
- Messages irrelevant to a module do not mutate its Views and return no module barrier.

## TimeTick

VChannelModule, SegmentModule, and TransformLogModule do not update any View
for TimeTick.

AckModule does not ack TimeTick. CheckpointManager can treat the message as immediate for both lanes unless another module returns a barrier.

## CreateCollection

VChannelModule observes the message through the common `ObserveMessage` path.
If the target VChannel View is absent, it creates the View. The View.Meta
contains collection information, initial partitions, schema history, normal
vchannel state, and `MetaTimeTick` equal to the message timetick. The View
becomes dirty and returns a Meta barrier.

No data task is required. AckModule returns an ack DataBarrier for the broadcast
message and calls the coordinator Ack API after previous ack task completion.

## DropCollection

SegmentModule flushes every retained segment in the target vchannel whose create
timetick is older than the message timetick. TransformLogModule first flushes
the target vchannel TransformLog buffer to make Delete entries durable, then
materializes TransformLog entries up to the message timetick into L0 segments.
These operations return Data barriers owned by the affected SegmentModule and
TransformLogModule state.

VChannelModule updates the VChannel View.Meta to dropped at the message
timetick and marks the View dirty. The retained View remains available for
historical-message filtering and VChannelModule-local tombstone finalization.

AckModule returns an ack DataBarrier for the broadcast message. The ack
precondition waits for previous ack task completion and the target vchannel's
composed SegmentModule/TransformLogModule materialized frontier to reach the
DropCollection timetick.

## TruncateCollection

SegmentModule flushes every retained segment in the target vchannel whose create
timetick is older than the message timetick. TransformLogModule flushes the
target vchannel TransformLog buffer.

VChannelModule advances the target VChannel View.Meta and `MetaTimeTick` to the
TruncateCollection timetick. The collection View.Meta is not removed by this
message. Data barriers are returned for the affected SegmentModule and
TransformLogModule work. AckModule returns an ack DataBarrier for the broadcast
message. The ack precondition waits for previous ack task completion and the
target vchannel's composed Data frontier to reach the TruncateCollection
timetick.

## CreatePartition

VChannelModule updates the target VChannel View.Meta by adding the partition in
normal state and advances `MetaTimeTick` to the message timetick. The View
becomes dirty and returns a Meta barrier.

No data task is required. AckModule returns an ack DataBarrier for the broadcast
message and calls the coordinator Ack API after previous ack task completion.

## DropPartition

SegmentModule flushes every retained segment in the partition whose create
timetick is older than the message timetick. TransformLogModule flushes the
vchannel TransformLog buffer.

VChannelModule updates the partition state in VChannel View.Meta to dropped at
the message timetick and marks the View dirty. The partition metadata remains
retained until VChannelModule-local tombstone cleanup removes it.

AckModule returns an ack DataBarrier for the broadcast message. The ack
precondition waits for previous ack task completion, the affected partition's
SegmentModule Data frontier, and the vchannel TransformLogModule Data frontier
to reach the DropPartition timetick.

## Import

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for
Import. Import state transitions are not owned by StreamingNode recovery.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## CommitImport

VChannelModule, SegmentModule, and TransformLogModule do not commit import
lifecycle state. Segment lifecycle changes produced by import are not modeled
as growing segment lifecycle changes.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## RollbackImport

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for
RollbackImport.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## BatchUpdateManifest

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for
BatchUpdateManifest.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## CreateSegment

SegmentModule observes the message through the common `ObserveMessage` path. If
the target Segment View is absent, it creates the View. SegmentModule reads
`SchemaAt(vchannel, partitionID, timetick)` from VChannelModule to attach the
correct historical schema. Segment View.Meta records collection, partition,
vchannel, segment id, storage version, growing state, create timetick, row
limits, schema snapshot, and `MetaTimeTick` equal to the message timetick. The
View becomes dirty and returns a Meta barrier.

In MetaAndData mode, SegmentModule submits an EnsureGrowingSegment task for the
segment. The Segment View returns a Data barrier until the lifecycle side effect
completes and the View.Data progress is persisted.

This message is not a broadcast message, so AckModule does not call the coordinator Ack API.

## Insert

SegmentModule finds the target Segment View from the message assignment. It
updates Segment View.Meta stats synchronously: modified rows, binary size, last
modified timestamp, and `MetaTimeTick`. The Segment View becomes dirty and
returns a Meta barrier.

In MetaAndData mode, the insert payload is appended to the Segment View's in-memory L1 buffer. The Segment View returns a Data barrier. If the flush policy is triggered, it submits a Segment-owned FlushBuffer task to write a fixed chunk to object storage. Task completion updates View.Data, advances `DataTimeTick`, and marks the View dirty. The Data barrier advances after the dirty View snapshot is persisted.

This message is not a broadcast message, so AckModule does not call the coordinator Ack API.

## Delete

TransformLogModule handles Delete as vchannel-level TransformLog data. In
MetaOnly mode it does not append data buffers. In MetaAndData mode it appends
the Delete WAL message to the TransformLog buffer and returns a TransformLog
Data barrier.

TransformLogModule does not validate Delete replay through VChannelModule or
SegmentModule state. Delete legality is guaranteed by WAL write-time checks,
exclusive/shared lock ordering, and WAL message order. Replay-side behavior is
module-local idempotent consumption based on TransformLog meta.

When the TransformLog buffer reaches policy thresholds or a later flush-style
message requires it, TransformLogModule submits a TransformLog task. Task
completion writes chunk data, updates TransformLog meta and `DataTimeTick`,
marks a TransformLog DirtySnapshot, and notifies RecoveryStorage. RecoveryStorage
persists the snapshot and calls `DirtySnapshot.MarkPersisted()`, which advances
the Data barrier.

This message is not a broadcast message, so AckModule does not call the coordinator Ack API.

## Flush

SegmentModule flushes the specified Segment View. The segment View.Meta is
closed at the flush timetick and marked dirty. If the segment was already
tombstoned for that timetick, the message is skipped.

In MetaAndData mode, SegmentModule submits the Segment-owned CommitL1Segment
task. The Segment Data barrier remains until all pending L1 output is durable,
lifecycle commit completes, View.Data advances to the flush timetick, and the
dirty View snapshot is persisted.

This message is not a broadcast message, so AckModule does not call the coordinator Ack API.

## ManualFlush

SegmentModule flushes every retained segment in the target vchannel whose
create timetick is older than the message timetick. It ignores any segment id
hints in the message body for recovery semantics.

TransformLogModule flushes the target vchannel TransformLog buffer to the
message timetick and then materializes TransformLog entries up to that timetick
into L0 segments.

The affected Segment Views and TransformLog return Data barriers according to
their durable and materialized progress rules. AckModule returns an ack
DataBarrier for the broadcast message. The ack precondition waits for previous
ack task completion and the target vchannel's composed
SegmentModule/TransformLogModule materialized frontier to reach the ManualFlush
timetick.

## FlushAll

SegmentModule flushes every retained segment on the PChannel whose create
timetick is older than the message timetick. TransformLogModule flushes all
eligible TransformLog buffers and materializes all local TransformLogs up to
the message timetick.

AckModule returns an ack DataBarrier for the broadcast message. The ack
precondition waits for previous ack task completion and the all-local composed
SegmentModule/TransformLogModule materialized frontier to reach the FlushAll
timetick.

## AlterReplicateConfig

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for
replication topology changes.

RecoveryStorage records replicate configuration progress in WALCheckpoint-related state outside the data modules. AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## Txn

Txn is the synthetic committed transaction message delivered by the recovery stream. It may contain Insert and Delete body messages.

For Insert bodies, SegmentModule updates each affected Segment View once at the
transaction timetick, appends the insert payload to the Segment L1 buffer in
MetaAndData mode, and returns the composed Meta/Data barriers for those
segments.

For Delete bodies, TransformLogModule handles the Txn message directly as one
atomic WAL message. It collects all Delete bodies in the transaction, groups
them into one TransformLog entry per vchannel at the transaction timetick, and
returns TransformLog Data barriers. The Txn is not split by external recovery
code before it reaches TransformLogModule.

This synthetic message is not a broadcast message, so AckModule does not call the coordinator Ack API.

## AlterCollection

VChannelModule updates collection metadata in the VChannel View.Meta and
advances `MetaTimeTick`.

If the AlterCollection changes schema, SegmentModule first flushes every
retained segment in the target vchannel whose create timetick is older than the
message timetick, and TransformLogModule flushes the target vchannel
TransformLog buffer. Then VChannelModule updates schema history. New segments
created after this timetick read the new schema through `SchemaAt`; existing
retained SegmentModule state keeps the schema snapshot it already owns.
Non-schema alterations do not require SegmentModule or TransformLogModule data
flush work.

AckModule returns an ack DataBarrier for the broadcast message. For
schema-changing AlterCollection, the ack precondition waits for previous ack
task completion and the target vchannel's composed SegmentModule/TransformLogModule
Data frontier to reach the AlterCollection timetick. For non-schema
AlterCollection, it waits only for previous ack task completion.

## AlterLoadConfig

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for AlterLoadConfig.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropLoadConfig

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for DropLoadConfig.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## CreateDatabase

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for CreateDatabase.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterDatabase

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for AlterDatabase.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropDatabase

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for DropDatabase.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterAlias

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for AlterAlias.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropAlias

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for DropAlias.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterUser

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for AlterUser.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropUser

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for DropUser.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterRole

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for AlterRole.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropRole

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for DropRole.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterUserRole

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for AlterUserRole.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropUserRole

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for DropUserRole.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterPrivilege

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for AlterPrivilege.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropPrivilege

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for DropPrivilege.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterPrivilegeGroup

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for AlterPrivilegeGroup.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropPrivilegeGroup

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for DropPrivilegeGroup.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## RestoreRBAC

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for RestoreRBAC.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterResourceGroup

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for AlterResourceGroup.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropResourceGroup

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for DropResourceGroup.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## CreateIndex

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for CreateIndex.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterIndex

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for AlterIndex.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropIndex

VChannelModule, SegmentModule, and TransformLogModule do not mutate Views for DropIndex.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterWAL

SegmentModule and TransformLogModule treat AlterWAL as a PChannel-wide flush
barrier. SegmentModule flushes every retained segment whose create timetick is
older than the message timetick, and TransformLogModule flushes all eligible
TransformLog buffers.

RecoveryStorage records AlterWAL information in WALCheckpoint-related state
outside the data modules. AckModule returns an ack DataBarrier for the broadcast
message. The ack precondition waits for previous ack task completion and the
all-local composed SegmentModule/TransformLogModule Data frontier to reach the
AlterWAL timetick.
