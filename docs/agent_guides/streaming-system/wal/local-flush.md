# Local Flush Control

Flush and FlushAll use the unreplicable broadcast entrance on either replication
role. Their WAL admission order is `current replicated Txn → local Flush → next
Txn`; consuming AckSyncUp still proves local L1/L0 completion.

See [Local Flush Control design](../../../design-docs/design_docs/wal/local_flush_control.md)
for dispatch, transaction admission, retry, recovery, ForcePromote, upgrade order
and the scope of other control operations.

## Key Packages

- `internal/datacoord/` — local broadcast producers
- `internal/streamingcoord/server/broadcaster/` — resource locks and promotion admission
- `internal/streamingnode/server/wal/interceptors/lock/` — replication transaction boundary
- `internal/streamingnode/server/wal/interceptors/replicate/replicates/` — commit tracking
- `internal/streamingnode/server/wal/interceptors/txn/` — origin and commit retry
- `internal/streamingnode/server/wal/interceptors/timetick/` — commit outcome handling
