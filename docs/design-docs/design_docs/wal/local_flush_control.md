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

CDC forwards the source Commit unchanged, including a zero count. Historical
transactions deliberately remain unchecked: CDC does not backfill a count from
the assembled bodies. Each replay preserves the assembled source order and the
same immutable source body IDs.
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

Ordinary backend failures retry indefinitely inside the WAL adaptor with the same
local TimeTick; physical retry copies are deduplicated by the scanner. Caller RPC
cancellation is removed from the append context. WAL context cancellation denotes
failure recovery, and fencing ends the old writer's authority. Neither permits a
new attempt against the old in-memory state: WAL replay resolves the durable outcome
before writes resume. See [Append retry and writer lifetime](../../../agent_guides/streaming-system/walbackend/walbackend.md#append-retry-and-writer-lifetime).

The retry boundary checks the outer WAL append context, not just a backend's returned
cancel/timeout error. Append timestamp allocation and barrier advancement likewise retry in
`AckManager.AllocateForAppend` until success or WAL-context cancellation; an individual TSO RPC failure
cannot leave a live writer with interceptor state that has no corresponding WAL record.

There is no RetryCommit state restoration or extra failure-handler callback.
A replicated Commit interrupted after preparation remains unresolved with its
session references retained for teardown/recovery. A body-count mismatch is a
definite rejection before backend append; on a replica it is checked before Commit
preparation, allowing the missing source bodies to be supplied by replay.

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

## Preconditions that keep the protocol small

- **Use transaction scope:** a transaction is confined to one VChannel. ManualFlush
  needs only that scope; an unrelated VChannel's transaction is not a reason to wait.
- **Use the complete source transaction:** CDC retains and replays Begin/body/Commit
  in source transaction order. Source MessageIDs remain stable across attempts and
  restarts, so the accepted-ID set plus `body_cnt` proves completeness without a
  per-attempt term, owner key or full body-ID list in Commit. See
  [Transaction ordering and replay identity](../../../agent_guides/streaming-system/replication/replicate.md#transaction-ordering-and-replay-identity).
- **Use existing serialization:** ReplicatesManager serializes replica append through
  acker completion. This permits one current receiving slot; it does not imply that
  primary transactions or their concurrent bodies can drop their locks.
- **Use existing resource locks:** local collection and cluster operations already
  have their required mutual exclusion. Independent collections need no Flush FIFO;
  replicated DDL needs the separate WAL/ACK protocol described above.
- **Prefer abort and replay to waiting:** an incomplete receiving transaction may be
  discarded at durable Flush and retried by CDC. The operation does not promise to
  include that incomplete transaction or catch up to the source. This concession
  removes the source-dependent wait and promotion rescue machinery; it does not
  weaken AckSyncUp or permit success without the required local persistence.
- **Use WAL recovery:** a cancelled writer is reconstructed from WAL. Do not add
  live-writer retry or failure callbacks based on a cancellation that already ends
  that writer's lifecycle.

## Operational behavior

Flushing the primary no longer flushes a secondary. Before a backup or other
operation that requires local persisted data on a secondary, call Flush/FlushAll
on that secondary explicitly. This does not wait for source replication catch-up;
replication progress must be checked separately when that is required.

## Compatibility boundary

The Commit count is additive. Missing fields decode to zero, and zero explicitly
skips all count checks even for nonempty transactions. New producers set nonzero
counts for nonempty transactions; CDC preserves historical zero-count commits
without adding validation. Upgrading all nodes does not retrofit counts onto old
WAL records, so their replication also skips the completeness check. Historical
unmarked flushes retain their old behavior.

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
- `internal/cdc/replication/replicatestream/` — full transaction forwarding with the original Commit count
- `pkg/streaming/util/message/` — Commit body-count format and assembly validation
- `internal/streamingnode/server/wal/interceptors/replicate/` — deduplicated body counting and durable-flush abort
- `internal/streamingnode/server/wal/interceptors/txn/` — scoped session cleanup
- `internal/streamingnode/server/wal/interceptors/timetick/` — Commit outcome handling
- `internal/streamingnode/server/wal/adaptor/` — backend retry and writer lifecycle
- `internal/streamingnode/server/wal/utility/` — consuming and recovering the abort boundary
