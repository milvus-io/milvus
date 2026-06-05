# WAL Recovery Message Flow

This document describes how RecoveryStorage modules handle each consumer-observable persisted WAL message type. Transaction control messages are consumed by the transaction assembly layer and are not delivered to RecoveryStorage modules. Deprecated message types are not listed.

Common rules:

- RecoveryStorage dispatches every persisted message to every module.
- Recovery scans and live consumption use the same module `ObserveMessage` implementation.
- In MetaOnly mode, `ObserveMessage` updates only View.Meta and does not submit Data-chain work.
- In MetaAndData mode, `ObserveMessage` enables Data-chain buffering and task submission.
- GrowingModule updates View.Meta synchronously in `ObserveMessage`.
- GrowingModule updates View.Data asynchronously through Scheduler tasks.
- Dirty Views are persisted by module-owned persist tasks.
- CheckpointManager advances physical checkpoints only after returned barriers disappear.
- AckModule submits an ack task and returns a DataBarrier for every persisted message with a `BroadcastHeader`.
- AckModule's DataBarrier disappears only after the coordinator broadcast Ack API succeeds.
- AckModule preconditions are defined by message type and message scope.
- Messages irrelevant to a module do not mutate its Views and return no module barrier.

## TimeTick

GrowingModule does not update any View for TimeTick.

AckModule does not ack TimeTick. CheckpointManager can treat the message as immediate for both lanes unless another module returns a barrier.

## CreateCollection

GrowingModule observes the message through the common `ObserveMessage` path. If the target VChannel View is absent, it creates the View. The View.Meta contains collection information, initial partitions, schema history, normal vchannel state, growing segment mode, and `MetaTimeTick` equal to the message timetick. The View becomes dirty and returns a Meta barrier.

No GrowingModule Data task is required. AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropCollection

GrowingModule first flushes every retained segment in the target vchannel whose create timetick is older than the message timetick. It also flushes the vchannel TransformLog buffer. These operations return Data barriers owned by the affected Segment Views and VChannel View.

After the flush work is scheduled, GrowingModule updates the VChannel View.Meta to dropped at the message timetick and marks the View dirty. The retained View remains available for historical-message filtering and later tombstone finalization.

AckModule returns an ack DataBarrier for the broadcast message. The ack precondition waits for previous ack task completion and the related vchannel GrowingModule Data frontier to reach the DropCollection timetick.

## TruncateCollection

GrowingModule flushes every retained segment in the target vchannel whose create timetick is older than the message timetick. It also flushes the vchannel TransformLog buffer.

GrowingModule advances the target VChannel View.Meta and `MetaTimeTick` to the TruncateCollection timetick. The collection View.Meta is not removed by this message. Data barriers are returned for the affected segment and TransformLog work. AckModule returns an ack DataBarrier for the broadcast message. The ack precondition waits for previous ack task completion and the related vchannel GrowingModule Data frontier to reach the TruncateCollection timetick.

## CreatePartition

GrowingModule updates the target VChannel View.Meta by adding the partition in normal state and advances `MetaTimeTick` to the message timetick. The View becomes dirty and returns a Meta barrier.

No GrowingModule Data task is required. AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropPartition

GrowingModule first flushes every retained segment in the partition whose create timetick is older than the message timetick. It also flushes the vchannel TransformLog buffer.

After the flush work is scheduled, GrowingModule updates the partition state in VChannel View.Meta to dropped at the message timetick and marks the View dirty. The partition metadata remains retained until its Data progress reaches the tombstone boundary.

AckModule returns an ack DataBarrier for the broadcast message. The ack precondition waits for previous ack task completion and the related partition GrowingModule Data frontier to reach the DropPartition timetick.

## Import

GrowingModule does not mutate Segment View or VChannel View for Import. Import state transitions are not owned by StreamingNode recovery.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## CommitImport

GrowingModule does not commit import lifecycle state. Segment lifecycle changes produced by import are not modeled as growing segment lifecycle changes.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## RollbackImport

GrowingModule does not mutate Segment View or VChannel View for RollbackImport.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## BatchUpdateManifest

GrowingModule does not mutate growing Views for BatchUpdateManifest.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## CreateSegment

GrowingModule observes the message through the common `ObserveMessage` path. If the target Segment View is absent, it creates the View. Segment View.Meta records collection, partition, vchannel, segment id, storage version, growing state, create timetick, row limits, and `MetaTimeTick` equal to the message timetick. The View becomes dirty and returns a Meta barrier.

In MetaAndData mode, GrowingModule submits an EnsureGrowingSegment task for the segment. The Segment View returns a Data barrier until the lifecycle side effect completes and the View.Data progress is persisted.

This message is not a broadcast message, so AckModule does not call the coordinator Ack API.

## Insert

GrowingModule finds the target VChannel View, partition, and Segment View from the message assignment. It updates Segment View.Meta stats synchronously: modified rows, binary size, last modified timestamp, and `MetaTimeTick`. The Segment View becomes dirty and returns a Meta barrier.

In MetaAndData mode, the insert payload is appended to the Segment View's in-memory L1 buffer. The Segment View returns a Data barrier. If the flush policy is triggered, it submits a Segment-owned FlushBuffer task to write a fixed chunk to object storage. Task completion updates View.Data, advances `DataTimeTick`, and marks the View dirty. The Data barrier advances after the dirty View snapshot is persisted.

This message is not a broadcast message, so AckModule does not call the coordinator Ack API.

## Delete

GrowingModule handles Delete as vchannel-level TransformLog data. In MetaOnly mode it does not append data buffers. In MetaAndData mode it appends the delete record to the VChannel View TransformLog buffer and returns a VChannel Data barrier.

When the TransformLog buffer reaches policy thresholds or a later flush-style message requires it, GrowingModule submits a TransformLog task. Task completion writes L0 data, updates View.Data and `DataTimeTick`, marks the View dirty, and later View persistence advances the Data barrier.

This message is not a broadcast message, so AckModule does not call the coordinator Ack API.

## Flush

GrowingModule flushes the specified Segment View. The segment View.Meta is closed at the flush timetick and marked dirty. If the segment was already tombstoned for that timetick, the message is skipped.

In MetaAndData mode, GrowingModule submits the Segment-owned CommitL1Segment task. The Segment Data barrier remains until all pending L1 output is durable, lifecycle commit completes, View.Data advances to the flush timetick, and the dirty View snapshot is persisted.

This message is not a broadcast message, so AckModule does not call the coordinator Ack API.

## ManualFlush

GrowingModule flushes every retained segment in the target vchannel whose create timetick is older than the message timetick. It ignores any segment id hints in the message body for recovery semantics.

The affected Segment Views return Meta and Data barriers according to the Flush rules. AckModule returns an ack DataBarrier for the broadcast message. The ack precondition waits for previous ack task completion and the related vchannel GrowingModule Data frontier to reach the ManualFlush timetick.

## FlushAll

GrowingModule flushes every retained segment on the PChannel whose create timetick is older than the message timetick. It also flushes all eligible VChannel TransformLog buffers.

AckModule returns an ack DataBarrier for the broadcast message. The ack precondition waits for previous ack task completion and the all-local GrowingModule Data frontier to reach the FlushAll timetick.

## AlterReplicateConfig

GrowingModule does not mutate growing Views for replication topology changes.

RecoveryStorage records replicate configuration progress in WALCheckpoint-related state outside GrowingModule. AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## Txn

Txn is the synthetic committed transaction message delivered by the recovery stream. It may contain Insert and Delete body messages.

For Insert bodies, GrowingModule updates each affected Segment View once at the transaction timetick, appends the insert payload to the Segment L1 buffer in MetaAndData mode, and returns the composed Meta/Data barriers for those segments.

For Delete bodies, GrowingModule groups deletes by vchannel, rewrites their effective timetick to the transaction timetick, appends them to the corresponding TransformLog buffer in MetaAndData mode, and returns VChannel Data barriers.

This synthetic message is not a broadcast message, so AckModule does not call the coordinator Ack API.

## AlterCollection

GrowingModule updates collection metadata in the VChannel View.Meta and advances `MetaTimeTick`.

If the AlterCollection changes schema, GrowingModule first flushes every retained segment in the target vchannel whose create timetick is older than the message timetick, flushes the TransformLog buffer, updates schema history, and refreshes retained Segment View schemas. Non-schema alterations do not require growing data flush work.

AckModule returns an ack DataBarrier for the broadcast message. For schema-changing AlterCollection, the ack precondition waits for previous ack task completion and the related vchannel GrowingModule Data frontier to reach the AlterCollection timetick. For non-schema AlterCollection, it waits only for previous ack task completion.

## AlterLoadConfig

GrowingModule does not mutate growing Views for AlterLoadConfig.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropLoadConfig

GrowingModule does not mutate growing Views for DropLoadConfig.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## CreateDatabase

GrowingModule does not mutate growing Views for CreateDatabase.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterDatabase

GrowingModule does not mutate growing Views for AlterDatabase.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropDatabase

GrowingModule does not mutate growing Views for DropDatabase.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterAlias

GrowingModule does not mutate growing Views for AlterAlias.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropAlias

GrowingModule does not mutate growing Views for DropAlias.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterUser

GrowingModule does not mutate growing Views for AlterUser.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropUser

GrowingModule does not mutate growing Views for DropUser.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterRole

GrowingModule does not mutate growing Views for AlterRole.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropRole

GrowingModule does not mutate growing Views for DropRole.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterUserRole

GrowingModule does not mutate growing Views for AlterUserRole.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropUserRole

GrowingModule does not mutate growing Views for DropUserRole.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterPrivilege

GrowingModule does not mutate growing Views for AlterPrivilege.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropPrivilege

GrowingModule does not mutate growing Views for DropPrivilege.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterPrivilegeGroup

GrowingModule does not mutate growing Views for AlterPrivilegeGroup.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropPrivilegeGroup

GrowingModule does not mutate growing Views for DropPrivilegeGroup.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## RestoreRBAC

GrowingModule does not mutate growing Views for RestoreRBAC.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterResourceGroup

GrowingModule does not mutate growing Views for AlterResourceGroup.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropResourceGroup

GrowingModule does not mutate growing Views for DropResourceGroup.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## CreateIndex

GrowingModule does not mutate growing Views for CreateIndex.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterIndex

GrowingModule does not mutate growing Views for AlterIndex.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## DropIndex

GrowingModule does not mutate growing Views for DropIndex.

AckModule returns an ack DataBarrier for the broadcast message and calls the coordinator Ack API after previous ack task completion.

## AlterWAL

GrowingModule treats AlterWAL as a PChannel-wide flush barrier. It flushes every retained segment whose create timetick is older than the message timetick and flushes all eligible VChannel TransformLog buffers.

RecoveryStorage records AlterWAL information in WALCheckpoint-related state outside GrowingModule. AckModule returns an ack DataBarrier for the broadcast message. The ack precondition waits for previous ack task completion and the all-local GrowingModule Data frontier to reach the AlterWAL timetick.
