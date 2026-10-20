# Lock

The Lock interceptor enforces exclusive/shared access on VChannel or PChannel scope during append, preventing concurrent conflicting operations.

## Lock Levels

- **PChannel-level**: A global RWMutex. Exclusive lock blocks all appends on the entire PChannel; shared lock allows concurrent appends.
- **VChannel-level**: A per-key RWMutex. Exclusive lock blocks appends on a specific VChannel; shared lock allows concurrent appends to the same VChannel.

For collection-scoped broadcasts, exclusive locking applies to the data VChannel copies. The CChannel copy uses shared
locking so DDL with non-conflicting ResourceKeys can append concurrently; conflicting DDL remains serialized by the
Broadcaster's ResourceKey lock. PChannel-level messages sent through the CChannel still acquire the global exclusive lock.

See [Message Semantic Docs](../message/message.md) for per-message lock requirements.

## Local flush and replication

Every successfully appended exclusive operation aborts transactions in its lock
scope, regardless of their local or replicated origin. The same guard releases
references and then unlocks; failed or deduplicated operations do not abort.
An ignored AlterReplicateConfig record is also a no-op. ManualFlush affects one
business VChannel per copy; FlushAll and topology changes affect the PChannel.
Replication prefix cleanup completes under the same outer WAL lock before replay
can enter the affected scope. There is no separate Flush or ForcePromote cleanup API.
See [Local Flush Control](local-flush.md).

## Key Packages

- `internal/streamingnode/server/wal/interceptors/lock/` — `lockAppendInterceptor`, builder
