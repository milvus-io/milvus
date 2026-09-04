# Shard Management

The Shard interceptor manages per-PChannel collection/partition/segment in-memory metadata and assigns each incoming DML message to a growing segment. All state is purely in-memory, should always keep consistent with underlying-WAL; on WAL open, it is recovered from the [RecoveryStorage](recovery-storage.md) snapshot.

See [Collection Messages](../message/message-semantic-collection.md) for per-message semantics and the messages handled by this interceptor.

## Seal Policies

Growing segments are sealed when any registered policy triggers. See `internal/streamingnode/server/wal/interceptors/shard/policy/seal_policy.go` for the full list of policies.

## VChannel Registration

The manager's registration map is keyed by **collection id**, one entry per collection per PChannel, and the entry names the VChannel it describes. Every operation that consults it — the DML fence check, the split fence, the teardown — must match on that name, not just the collection id: after a shard split retires a source and the coordinator reclaims its slot, a later VChannel of the same collection can hold the entry, and an operation that ignored the name would answer for, fence, or tear down the wrong shard.

Registration admission is enforced here rather than assumed of the caller:

- `CheckIfVChannelCanBeCreated` — `ErrCollectionExists` for an idempotent replay of the same VChannel; `ErrVChannelConflict` when another VChannel of the collection holds the entry, which the interceptor turns into a rejected append.
- `CheckIfVChannelCanBeDropped` — `ErrVChannelNotFenced` when the named VChannel is registered here and no split has fenced it, so a live shard is never torn down.
- `CheckIfVChannelCanBeWritten` — `ErrVChannelFenced` once a split has fenced the VChannel; `ErrCollectionNotFound` when this PChannel does not hold it.

On WAL open the map is rebuilt from the RecoveryStorage snapshot. If that snapshot contains two VChannels of one collection (a fenced source whose teardown has not been observed yet, plus its successor), the collision is resolved deterministically — the VChannel that can still take writes wins, ties broken by name — and logged, so a restart cannot leave a live shard unwritable at random.

## Key Packages

- `internal/streamingnode/server/wal/interceptors/shard/` — Shard interceptor, `ShardManager`, seal policies, segment stats
