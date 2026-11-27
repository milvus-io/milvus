# Shard Management

The Shard interceptor manages per-PChannel collection/partition/segment in-memory metadata and assigns each incoming DML message to a growing segment. All state is purely in-memory, should always keep consistent with underlying-WAL; on WAL open, it is recovered from the [RecoveryStorage](recovery-storage.md) snapshot.

See [Collection Messages](../message/message-semantic-collection.md) for per-message semantics and the messages handled by this interceptor.

Every schema-relevant AlterCollection mask updates the current schema snapshot,
including collection/database names, description, properties and external
source/spec. Only the `schema` mask flushes/fences segment allocation; ordinary
metadata updates keep SchemaVersion unchanged. RecoveryStorage applies the same
mask semantics. `collection.on.truncating` remains coordinator/DataCoord state.

The CreateSegment append interceptor sets the encoding SchemaVersion and applies
the schema's storage format requirements while holding the VChannel lock.
An asynchronous allocation started before a schema DDL therefore uses the
schema in effect when its CreateSegment enters the WAL.

## Seal Policies

Growing segments are sealed when any registered policy triggers. See `internal/streamingnode/server/wal/interceptors/shard/policy/seal_policy.go` for the full list of policies.

StreamingNode does not seal by binlog file count. Segment capacity, lifetime,
idle time, memory pressure, blocking L0 and explicit WAL operations trigger
sealing. DataCoord does not seal growing segments: it only registers the
segments that StreamingNode allocates (`AllocSegment`) and records their flush
(`SaveBinlogPaths`).

Recovery restores only NORMAL partitions as writable. Dropped/tombstoned
partitions remain in recovery metadata until Summary retirement, without
re-entering the allocation path.

## Shard Split

`SplitShard` is handled by `shard_interceptor_split.go`. The role of a copy is
decided by `messageutil.SplitShardRoleOf`.

- **Source copy (fence).** `ShardManager.FenceForSplit` seals every growing
  segment and marks the collection registration as fenced. The fence is
  installed before the append and is kept if the append reports an error,
  because a failed append can still be persisted. A fenced VChannel refuses
  Insert, Delete and CreateSegment. Insert and Delete get
  `STREAMING_CODE_SHARD_FENCED`, which is not an unrecoverable code: the caller
  refreshes its routing and retries on the new shards. Transactions: the
  fence is an exclusive message, so the Lock interceptor ends every
  transaction of the VChannel once it is appended, while it still holds the
  VChannel lock; a Commit holds the shared lock through its append, so no
  Commit of the VChannel is appended after the fence. A later body of an
  ended transaction gets `STREAMING_CODE_SHARD_FENCED`, and so does a
  BeginTxn on a fenced VChannel, whose session is ended at once. A Commit
  may reroute only while the transaction manager retains proof that this
  source fence aborted it before Commit began. A missing Commit session
  alone does not prove an abort: cleanup also removes committed sessions.
  The redo interceptor therefore returns
  `STREAMING_CODE_INVALID_TRANSACTION_STATE` with an unknown-outcome reason
  for that case, preventing automatic replay. With a proven abort, the
  writer places the whole transaction on the new shards. The registration is
  kept, so ManualFlush, AlterCollection, SchemaChange and Flush are still
  admitted. A second copy of the same split is appended and changes nothing. A
  copy of another split is refused.
- **Target copy (genesis).** After the append the copy is projected into a
  CreateCollection message (`messageutil.SplitShardGenesisMessage`) and takes
  the CreateCollection path: shard manager registration, function runners,
  VChannel recovery module, MVCC frontiers. A genesis that is appended again
  changes nothing.
- A PChannel keeps one registration per collection. A target on a PChannel
  that already holds a VChannel of the collection is refused. The issuer
  checks this before the broadcast (`routing.CheckSplitChannels`).

RecoveryStorage records the split in `VChannelMeta.split_lineage` (target
VChannels, switch TimeTick, split id). The record is written once. The VChannel
stays NORMAL. On WAL open the write path reads the record through
`VChannelWritePathRecoveryState.SplitTaskID` and starts fenced.

## Key Packages

- `internal/streamingnode/server/wal/interceptors/shard/` — Shard interceptor, `ShardManager`, seal policies, segment stats

DropPartition retires its target partition with the `partition_removed` seal
policy before fencing the surviving partitions. Flush metrics are recorded only
when a growing segment first becomes flushed; a later fence/drop also removes
segments with pending async Flush messages without recording them again.
