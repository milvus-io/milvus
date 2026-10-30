# Local Flush Control

Flush and FlushAll operate locally on either replication role using `_ur`.
Proxy sends ManualFlush to each collection VChannel via RawAppend; the internal
release preparer also marks its ManualFlush `_ur`. FlushAll uses the unreplicable
broadcast API with ExclusiveCluster. These operations abort incomplete
transactions in their WAL lock scope without waiting for source catch-up.

3.0 retains the independent flusher and GetFlushState/GetFlushAllState completion
checks; FlushAll append does not use AckSyncUp. The release preparer retains its
handoff rollback, segment-ID union and GetGrowingFlushProgress checks. A local
ManualFlush overtaken by collection Drop appends an empty best-effort WAL record;
subsequent metadata/progress queries retain their errors and no extra Drop wait is added.

FlushAll returns per-PChannel `ImmutableMessage` copies without `_ur`, so Backup
can explicitly replicate them to align positions on a secondary. Message IDs,
TimeTicks and LastConfirmedMessageIDs are preserved. The persisted WAL broadcast
still carries `_ur` and is skipped by ordinary CDC replication.

CDC replays complete transactions. The persisted Commit `body_cnt` is checked against distinct successfully appended
source body IDs, preventing stale Commit from accepting missing bodies. Zero skips
validation for compatibility. Recovery
reconstructs the accepted prefix or observes the durable Flush abort boundary.

See [Local Flush Control design](../../../design-docs/design_docs/wal/local_flush_control.md)
for the protocol, Drop race, final append failures and operational upgrade boundary.

## Key Packages

- `internal/proxy/` — local ManualFlush producer
- `internal/streamingnode/server/service/` — release handoff ManualFlush
- `internal/datacoord/` — FlushAll broadcast and state queries
- `internal/cdc/replication/replicatestream/` — complete transaction replay
- `pkg/streaming/util/message/` — Commit body-count format and transaction assembly
- `internal/streamingnode/server/wal/interceptors/replicate/` — prefix and abort handling
- `internal/streamingnode/server/wal/interceptors/txn/` — scoped reference cleanup
- `internal/streamingnode/server/wal/utility/` — transaction consumption and recovery
