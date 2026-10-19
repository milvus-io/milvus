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

## Abort and replay against replication

Transactions are VChannel-scoped. A local ManualFlush takes the existing exclusive
VChannel WAL lock; FlushAll takes the PChannel exclusive lock. It does not wait for
an incomplete transaction to commit. Ordinary lock cleanup continues to fail local
sessions. Once the local flush record is durable, the Replicate interceptor aborts
receiving sessions in that scope, releases their segment/proof references, and
clears the replication helper's accepted prefix before releasing the WAL lock.
Aborted sessions are marked terminal so infinite leases cannot pin
LastConfirmedMessageID; completing the last session also wakes graceful shutdown.
ManualFlush's CChannel copy does not abort business transactions. Other VChannels
on the same PChannel remain unaffected. Ordinary non-flush exclusive operations
continue to skip replicated sessions.

The consumer TxnBuffer applies the same abort boundary before forwarding a local
flush. Thus a WAL reopen cannot recover a pre-flush partial transaction. Only the
replayed body's newly assigned local segments can appear in the committed txn.
No rollback message is sent back to the source and no replication checkpoint is
advanced. The local flush itself is the durable abort record.

CDC retains each complete source transaction until Commit is acknowledged. A
transaction-expired response terminates the stream; reconnect replays Begin, all
body messages and Commit. CDC restart obtains the complete transaction again from
the source WAL/checkpoint. No new transaction ID, term, or runtime owner is needed.
An intact prefix resumes with deduplication. A prefix aborted by Flush starts again
from Begin. Duplicate Begin does not replace a still-valid prefix.

## Transaction body count

`CommitTxnMessageHeader.body_cnt` is the expected number of logical body messages.
It excludes Begin, Commit, physical WAL chunks and backend retry copies. A value
of **zero skips validation everywhere**, preserving old WAL/senders and empty
transactions without an additional presence field.

The primary producer fills the count from its VChannel batch and sends Commit
only after every body append succeeds. TxnSession increments its successful-body
counter once per successful logical append, not per backend attempt. Commit closes
body admission, waits for in-flight appends to finish, and compares the count.
A mismatch terminates the session, releases references and returns
TransactionExpired so the producer retries the whole transaction with a new TxnID.
This catches an upper-layer retry that appended an extra body with a new TimeTick.
Late appends cannot revive the rejected session. A backend retry retains its local
TimeTick; the consumer's existing physical deduplication removes its extra copies.

Consumers validate the count after chunk assembly and TimeTick deduplication,
before publishing an assembled transaction. A nonzero mismatch in a durable
Commit is a data-integrity failure and stops consumption/recovery rather than
silently publishing or dropping an incomplete transaction. The source consumer
performs this validation before CDC sees a transaction.

CDC forwards a nonzero source count unchanged. For old commits with zero count,
it fills the number of assembled bodies on a copy of the Commit header; it does
not mutate retained source messages. Empty transactions retain zero. Each replay
preserves the assembled source order and the same immutable source body IDs.
There is no per-body predecessor property, ID manifest, replication term or owner.

Under its existing mutex, held through append and acker completion, ReplicatesManager:

1. Checks the source checkpoint. Already committed traffic remains idempotent.
2. Admits Begin when there is no current transaction; duplicate active Begin is ignored.
3. Ignores already accepted source body IDs; records new IDs only after append succeeds.
4. Compares a nonzero Commit count to the accepted source-ID set size **before**
   preparing the session. A mismatch returns TransactionExpired without changing
   the session to OnCommit, allowing reconnect to replay and fill missing bodies.
5. Clears the receiving state and advances the source checkpoint only after a
   successful Commit append.

For a fixed source transaction, the accepted distinct IDs are a subset of its
immutable body set. Equal cardinality therefore proves completeness even when
old and new connections overlap. A duplicate body cannot substitute for a missing
one. A stale Commit is safe once all bodies exist; no connection needs to win.
A zero-count Commit intentionally bypasses this proof for compatibility.

Missing Begin also returns TransactionExpired. A durable local Flush clears the
body dedup set and aborts the session, so replayed bodies receive new local segments.
There is still one current replicated transaction per PChannel, following the
existing sequential CDC channel stream; no concurrent-attempt map is introduced.

## Failure, recovery and promotion

Ordinary backend failures retry inside the WAL adaptor. There is no RetryCommit
state restoration. A final failure after replicated Commit preparation retains the
session and segment references and fences the WAL before releasing append locks.
A final backend failure for any replicated append or local flush also fences the
writer: the record may already exist, so memory cannot safely resume from an
assumed failed append. WAL recovery resolves the durable outcome before retry.
A replicated body-count mismatch is rejected before Commit preparation and does not fence a
healthy WAL. Caller RPC cancellation is not a transaction rollback protocol.

Recovery reconstructs the accepted source-ID set and successful body count from the uncommitted
TxnBuffer, and the session from the same snapshot. A replayed Flush removes the
old builder; a durable Commit publishes the complete transaction. Recovered session
references retain the existing RecoverDone barrier until resolved. TxnBuffer also
checks the persisted nonzero Commit body count before assembling a transaction.

Flush has no wait-for-source gate, pending counter, FIFO queue or promotion lock.
Broadcaster uses its existing resource keys and worker pool for all operations.
ForcePromote retains ExclusiveCluster, successful-append rollback, salvage
checkpoints and callback ordering. It needs no separate execution lane to rescue
Flush from a disconnected source. Coordinator resource keys serialize local APIs;
replicated DDL still relies on WAL ordering and consuming ACKs.

## Operational behavior

Flushing the primary no longer flushes a secondary. Before a backup or other
operation that requires local persisted data on a secondary, call Flush/FlushAll
on that secondary explicitly. This does not wait for source replication catch-up;
replication progress must be checked separately when that is required.

## Compatibility boundary

The Commit count is additive. Missing fields decode to zero, and zero explicitly
skips all count checks even for nonempty transactions. New producers set nonzero
counts for nonempty transactions; CDC fills old zero-count commits from the
assembled source transaction. Historical unmarked flushes retain their old behavior.

Operators must upgrade **all participating source CDC, Proxy, StreamingNode and
consumer processes before invoking local Flush on a secondary**. Coordinating this
cross-cluster rollout is an operational responsibility; no version probing or
feature gate is introduced. The new abort guarantee is not promised during a
mixed-version deployment. Before downgrade, drain local-flush tasks and replication
and arrange a compatible checkpoint/recovery boundary; an old consumer does not
understand the durable local-flush abort boundary.

## Scope of other control operations

| Operation | Scope and entrance | Change here |
| --- | --- | --- |
| Flush / FlushAll | Local WAL, either role, `_ur`, AckSyncUp | Enabled on secondary with abort and full-txn replay |
| Resource-group create/update/drop and node transfer | Local CChannel, either role, `_ur` | Existing behavior retained |
| ForcePromote | Secondary-only topology transition | Existing resource keys and explicit txn rollback retained |
| Snapshot/manifest/external-collection controls marked `_ur` | Concrete messages stay local; ordinary producers still check primary | Role admission unchanged |
| Collection/partition DDL, DML, load configuration and TransferReplica | Existing logical replication path | Remain replicated; no message-type blacklist |
| WAL-generated CreateSegment/Flush/TimeTick | Self-controlled local messages | Unchanged |

## Key Packages

- `internal/datacoord/` — local producers and resource keys
- `internal/cdc/replication/replicatestream/` — full transaction forwarding and legacy Commit count decoration
- `pkg/streaming/util/message/` — Commit body-count format and assembly validation
- `internal/streamingnode/server/wal/interceptors/replicate/` — deduplicated body counting and durable-flush abort
- `internal/streamingnode/server/wal/interceptors/txn/` — scoped session cleanup
- `internal/streamingnode/server/wal/interceptors/timetick/` — Commit outcome handling
- `internal/streamingnode/server/wal/adaptor/` — backend retry and failure fencing
- `internal/streamingnode/server/wal/utility/` — consuming and recovering the abort boundary
