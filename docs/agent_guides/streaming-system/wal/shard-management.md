# Shard Management

The Shard interceptor manages per-PChannel collection/partition/segment in-memory metadata and assigns each incoming DML message to a growing segment. All state is purely in-memory, should always keep consistent with underlying-WAL; on WAL open, it is recovered from the [RecoveryStorage](recovery-storage.md) snapshot.

See [Collection Messages](../message/message-semantic-collection.md) for per-message semantics and the messages handled by this interceptor.

## Seal Policies

Growing segments are sealed when any registered policy triggers. See `internal/streamingnode/server/wal/interceptors/shard/policy/seal_policy.go` for the full list of policies.

Note: L1 (normal insert) segments seal on **size only** — the row threshold is disabled (`SegmentRows = math.MaxUint64` in `segment_limitation_policy.go`); only L0 (delete) segments enforce both a row and a size limit (`FlushL0MaxRowNum` / `FlushL0MaxSize`).

## Key Packages

- `internal/streamingnode/server/wal/interceptors/shard/` — Shard interceptor, `ShardManager`, seal policies, segment stats
