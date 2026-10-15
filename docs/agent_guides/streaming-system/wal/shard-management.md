# Shard Management

The Shard interceptor manages per-PChannel collection/partition/segment in-memory metadata and assigns each incoming DML message to a growing segment. All state is purely in-memory, should always keep consistent with underlying-WAL; on WAL open, it is recovered from the [RecoveryStorage](recovery-storage.md) snapshot.

See [Collection Messages](../message/message-semantic-collection.md) for per-message semantics and the messages handled by this interceptor.

## Seal Policies

Growing segments are sealed when any registered policy triggers. See `internal/streamingnode/server/wal/interceptors/shard/policy/seal_policy.go` for the full list of policies.

StreamingNode no longer seals by binlog file count or consumes
`dataCoord.segment.maxBinlogFileNumber`. Segment capacity, lifetime, idle time,
memory pressure, blocking L0 and explicit WAL operations still trigger sealing.
The DataCoord configuration remains in use by the legacy DataCoord path.

Recovery restores only NORMAL partitions as writable. Dropped/tombstoned
partitions remain in recovery metadata until Summary retirement, without
re-entering the allocation path.

## Key Packages

- `internal/streamingnode/server/wal/interceptors/shard/` — Shard interceptor, `ShardManager`, seal policies, segment stats

DropPartition retires its target partition with the `partition_removed` seal
policy before fencing the surviving partitions. Flush metrics are recorded only
when a growing segment first becomes flushed; a later fence/drop also removes
segments with pending async Flush messages without recording them again.
