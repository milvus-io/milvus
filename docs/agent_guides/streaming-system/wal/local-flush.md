# Local Flush Control

Flush and FlushAll operate locally on either replication role, using `_ur` and the
existing resource keys. They abort incomplete transactions in their WAL lock scope
instead of waiting for source replication. AckSyncUp still proves local L1/L0
completion; this operation does not promise source catch-up.

CDC replays complete transactions. The persisted Commit `body_cnt` is checked against distinct successfully appended
source body IDs, preventing stale Commit from accepting missing bodies. Zero skips
validation for compatibility. Recovery
reconstructs the accepted prefix or observes the durable Flush abort boundary.

See [Local Flush Control design](../../../design-docs/design_docs/wal/local_flush_control.md)
for the protocol, Drop race, final append failures and operational upgrade boundary.

## Key Packages

- `internal/datacoord/` — local broadcast producers
- `internal/cdc/replication/replicatestream/` — complete transaction replay
- `pkg/streaming/util/message/` — Commit body-count format and assembly checks
- `internal/streamingnode/server/wal/interceptors/replicate/` — prefix and abort handling
- `internal/streamingnode/server/wal/interceptors/txn/` — scoped reference cleanup
- `internal/streamingnode/server/wal/utility/` — transaction consumption and recovery
