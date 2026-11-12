# Shard Management

The Shard interceptor manages per-PChannel collection/partition/segment in-memory metadata and assigns incoming row writes to growing segments. All state is purely in-memory, should always keep consistent with underlying-WAL; on WAL open, it is recovered from the [RecoveryStorage](recovery-storage.md) snapshot.

See [Collection Messages](../message/message-semantic-collection.md) for per-message semantics and the messages handled by this interceptor.

Every schema-relevant AlterCollection mask updates the current schema snapshot,
including collection/database names, description, properties and external
source/spec. Only the `schema` mask flushes/fences segment allocation; ordinary
metadata updates keep SchemaVersion unchanged. RecoveryStorage applies the same
mask semantics. `collection.on.truncating` remains coordinator/DataCoord state.

## Upsert assignment

Insert V2 and Upsert carry a partition table for one collection/VChannel and
share nested InsertRows. Legacy Insert v0/V1 keeps its existing path. Schema mismatch is checked
before schema-dependent content checks. Shard admission checks ownership,
namespace support, compound body shape, and CAS proof shape; producers validate
ordinary column values. Function preparation retains its existing append path.

Single-target and batch assignment use the same selection and accounting path.
All target partitions must be ready before any rows or bytes are charged.
Ack references precede batch accounting and capacity-triggered sealing; accounting
failure releases them. A later target requiring redo leaves no charged prefix.
Per-partition binary size covers only that partition's inserted rows, PKs, and
RowIDs. The receiver supplies local segment IDs, including for CDC writes.

Producers construct ordinary, persisted, replicable business-VChannel messages.
There is no separate Upsert flags validator. Ordinary CChannel messages skip
shard effects; the existing PChannel-level exception remains.
See [Unified Insert and Upsert WAL format](../../../design-docs/design_docs/wal/upsert_message.md).

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

## Key Packages

- `internal/streamingnode/server/wal/interceptors/shard/` — Shard interceptor, `ShardManager`, seal policies, segment stats

DropPartition retires its target partition with the `partition_removed` seal
policy before fencing the surviving partitions. Flush metrics are recorded only
when a growing segment first becomes flushed; a later fence/drop also removes
segments with pending async Flush messages without recording them again.

Proxy incrementally calculates per-partition logical size during row packing,
then finalizes protobuf framing without another row scan or sizing selections. Insert V2/Upsert
admission independently verifies/recomputes size: unchanged single-partition
writes use projection column framing plus logical PK/RowID sizes; sparse-vector and nullable-mask
normalization, multi-partition writes and changed function outputs retain exact
decoded sizing. When decoding is needed, admission reuses one body within each
attempt for function materialization, writeback and partition sizing. This is
local ownership, not a mutable body cache: redo starts a fresh attempt. Header-only segment assignment
does not change the rows. Function outputs are encoded only when changed.
