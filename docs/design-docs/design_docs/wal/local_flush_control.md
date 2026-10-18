# Local Flush Control

Flush and FlushAll operate on the receiving cluster, on either replication role.
DataCoord uses `StartUnreplicableBroadcastWithResourceKeys` and marks new
ManualFlush and FlushAll messages `Unreplicable` (`_ur`). CDC skips these concrete
messages; historical messages without the marker retain their replication behavior.

## Dispatch and completion

ManualFlush broadcasts to every collection VChannel plus CChannel, retaining
SharedDBName and ExclusiveCollectionName. FlushAll retains ExclusiveCluster and
broadcasts to all PChannels. Both retain AckSyncUp: success means consuming-side
L1 and required L0 output has completed locally. The collection CChannel copy has
no data to flush. Flush responses and flush-state RPCs retain their contracts.
No timestamp comparison across PChannels or clusters is used.

## Concurrent collection drop

DataCoord validates the collection before persisting a local ManualFlush. RootCoord
publishes collection metadata only after CreateCollection has ACKed on all business
VChannels, and collection IDs are never reused. Therefore a validated local flush
whose shard is absent on append has been overtaken by DropCollection, possibly
before a retry or WAL handoff. This precondition avoids maintaining another set of
permanent dropped-collection tombstones in the write path.

Only an unreplicable, locally originated ManualFlush treats the shard manager's
ErrCollectionNotFound as an empty flush. It still appends a real WAL record with
empty SegmentIds and AckSyncUp. Recovery's BroadcastAck preserves conflicting
resource-key order and explicitly orders a local ManualFlush after earlier Drops
on its business VChannel. The latter survives replicated renames that change the
name resource key after Flush admission. Thus the empty flush cannot ACK before
the preceding Drop's L1/L0 persistence and ACK. Other errors and historical/replicated flushes retain their
existing behavior. Replicated DDL does not acquire the local API resource locks;
those locks alone cannot prevent this race. No ACK is synthesized outside WAL.

## Ordering against replication

A PChannel admission gate in the Lock interceptor precedes WAL locks and TimeTick
allocation. Registering a local flush and admitting a replicated BeginTxn are
atomic under the same mutex. A Begin admitted before registration counts as the
current transaction even while its WAL append is in flight.

1. Drain that transaction: admit its body, CommitTxn and reconnect duplicate
   Begin/body messages, including older deduplicated traffic needed to reach the
   current transaction on reconnection. Hold subsequent new transactions and non-transactional
   replicated messages.
2. After CommitTxn successfully appends locally, append the local flush under its
   usual exclusive WAL lock.
3. Release subsequent replication after flush append. The RPC separately waits
   for consuming ACKs; replication does not wait for L1/L0 completion.

The local order is `current replicated Txn → local Flush → next Txn`. Registered
flushes take precedence over the next transaction; independent flushes have no
FIFO contract. A pending count and an append reservation (`busy`) preserve this
priority until each admitted flush finishes appending. ManualFlush's CChannel copy does not enter the gate;
FlushAll gates every PChannel. The gate is PChannel-wide, matching the existing
single replicated transaction per PChannel. A collection flush may therefore wait
for the current transaction of another VChannel on that PChannel. This is not a
global transaction cut across PChannels.

Ordinary exclusive writes only force-fail locally originated transactions.
Replicated transactions retain their source ID, infinite lease and segment
references. A failed replicated Commit append restores its session to InFlight
and leaves the replication helper's current transaction intact. The replication
manager serializes the entire append through its acker, so Commit attempts cannot
race each other. Preparation owns its state transition and restores it on wait
failure; append completion handles failures after preparation. Ordinary request
cancellation is removed by the WAL adaptor, and backend errors normally retry
inside it; RetryCommit is not an RPC-timeout retry mechanism. Receiving Commit
or entering OnCommit does not release the flush. Successful retry releases it;
reconnect duplicates keep the existing replication deduplication behavior.

## Recovery, cancellation and promotion

WAL recovery restores the gate's active transaction from the recovered transaction
buffer and source checkpoint. Local flushes have no ReplicateHeader and do not
advance the source replication checkpoint. Request timeout does not remove a
persisted broadcast task or its resource locks. An append attempt's cancellation
removes its gate waiter; retry registers again at the then-current local boundary.
WAL shutdown wakes gate waiters so migration cannot hang on the gate.

Replication disconnecting mid-transaction keeps Flush waiting for reconnection
and commit. ForcePromote bypasses the gate, takes the PChannel exclusive WAL lock,
and only after successful append rolls back unfinished transactions and clears
the gate's current transaction. An unsuccessful promotion does not release Flush.

ForcePromote uses a fixed RW promotion admission lock to avoid waiting behind a
local Flush's resource locks. Ordinary replicable broadcasts hold the shared
admission lock in addition to normal resource keys; ForcePromote takes the
exclusive admission lock. Unreplicable broadcasts retain normal resource locks
without admission locks. Primary-only producers that mark a message `_ur` release
their temporary admission guard before registering the task, so recovery depends
only on persisted properties. ForcePromote still carries ExclusiveCluster in its
broadcast header for callback ordering and incomplete-broadcast repair. Recovery
reconstructs both lock domains from persisted message properties and headers.
Ordinary broadcasts acquire normal resource keys before admission, so requests
blocked behind a local Flush cannot prevent promotion.

Admission alone does not guarantee progress: a local Flush waiting for a source
transaction occupies a broadcaster worker. ForcePromote appends through a dedicated
worker and a separate pending queue. New tasks, recovered tasks and backoff retries
all route by the persisted ForcePromote header, so a saturated ordinary pool cannot
prevent promotion from rolling back the transaction. Callback ordering still uses
the existing resource-key scheduler. Other local `_ur` controls (including resource
group operations) may append concurrently with promotion; their conflicting ACKs
and callbacks remain ordered by the header keys.

## Operational behavior

Flushing the primary no longer flushes a secondary. Before a backup or other
operation that requires local persisted data on a secondary, call Flush/FlushAll
on that secondary explicitly. This does not wait for source replication catch-up;
replication progress must be checked separately when that is required.

## Compatibility boundary

No RPC or protobuf changes are required. CDC and consumers use the existing `_ur`
property. Secondary local-flush ordering requires upgraded WAL writers: deploying
only the DataCoord producer cannot provide the new transaction boundary on an
older StreamingNode. Upgrade StreamingNodes before enabling the new DataCoord
producer on secondary clusters. During rollback, finish pending local-flush and
ForcePromote tasks before handing coordination back to a version that reconstructs
both operations in the same resource-lock domain. Historical unmarked flush
messages remain replicable.

## Scope of other control operations

| Operation | Scope and entrance | Change here |
| --- | --- | --- |
| Flush / FlushAll | Local WAL, either role, `_ur`, AckSyncUp | Enabled on secondary with transaction-boundary admission |
| Resource-group create/update/drop and node transfer | Local CChannel, either role, `_ur` | Existing behavior retained |
| ForcePromote | Secondary-only topology transition | Independent promotion admission; explicit txn rollback retained |
| Snapshot/manifest/external-collection controls marked `_ur` | Concrete messages stay local; ordinary producers still check primary | Role admission unchanged |
| Collection/partition DDL, DML, load configuration and TransferReplica | Existing logical replication path | Remain replicated; no message-type blacklist |
| WAL-generated CreateSegment/Flush/TimeTick | Self-controlled local messages | Unchanged |

## Key Packages

- `internal/datacoord/` — local producers and resource keys
- `internal/streamingcoord/server/broadcaster/` — promotion admission and recovery
- `internal/streamingnode/server/wal/interceptors/lock/` — transaction boundary gate
- `internal/streamingnode/server/wal/interceptors/replicate/replicates/` — commit tracking
- `internal/streamingnode/server/wal/interceptors/txn/` — transaction origin and retry
- `internal/streamingnode/server/wal/interceptors/timetick/` — commit outcome handling
