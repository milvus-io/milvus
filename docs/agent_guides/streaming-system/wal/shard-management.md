# Shard Management

The Shard interceptor manages per-PChannel collection/partition/segment in-memory metadata and assigns each incoming DML message to a growing segment. All state is purely in-memory, should always keep consistent with underlying-WAL; on WAL open, it is recovered from the [RecoveryStorage](recovery-storage.md) snapshot.

See [Collection Messages](../message/message-semantic-collection.md) for per-message semantics and the messages handled by this interceptor.

The CreateSegment append interceptor sets the encoding SchemaVersion and applies
the schema's storage format requirements while holding the VChannel lock.
An asynchronous allocation started before a schema DDL therefore uses the
schema in effect when its CreateSegment enters the WAL.

## Seal Policies

Growing segments are sealed when any registered policy triggers. See `internal/streamingnode/server/wal/interceptors/shard/policy/seal_policy.go` for the full list of policies.

## Key Packages

- `internal/streamingnode/server/wal/interceptors/shard/` — Shard interceptor, `ShardManager`, seal policies, segment stats
