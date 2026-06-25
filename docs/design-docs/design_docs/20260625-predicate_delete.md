# Predicate Delete Design

Related issue: [milvus-io/milvus#50433](https://github.com/milvus-io/milvus/issues/50433)

## Background

Milvus traditionally deletes entities by **primary key**: a `Delete` request is first
resolved to a concrete PK list, those PKs are persisted as delta logs, and a deletion
bitmap is built from them at query time.

Resolving a filter to PKs up front is expensive and sometimes impossible to do eagerly
(the matching set may be huge or span data not yet loaded). **Predicate delete** lets a
user delete by an arbitrary boolean **filter expression** (e.g. `age > 30 && city ==
"NY"`) directly: the expression itself is persisted and evaluated lazily, instead of
materializing the matching primary keys.

This document describes the end-to-end design of predicate delete: how a predicate
`Delete` is produced, persisted, recovered, and applied.

## Goals

- Allow `Delete` by an arbitrary boolean filter expression, not just by primary key.
- Persist predicate deletes durably so they survive restart, flush, and compaction.
- Apply predicate deletes with the same visibility/MVCC semantics as PK deletes.
- Be fully backward compatible: PK-delete behavior and on-disk layout are unchanged, and
  predicate-delete metadata is isolated so a rollback to an older binary is safe.

## Architecture Overview

Predicate delete reuses the existing delete pipeline (proxy → WAL → DataNode → object
storage → QueryNode) and adds a parallel, expression-carrying channel alongside PK
deletes. The work is staged in three phases:

1. **Produce** — Proxy encodes a predicate `Delete` as a delete message carrying a
   serialized expression plan, and writes it to the WAL.
2. **Persist** — DataNode buffers predicate deletes into L0 segments and flushes them as
   dedicated **predicate deltalog** files, recording their metadata separately from PK
   deltalogs.
3. **Recover & apply** — On load, predicate deltalogs are read back and the expressions
   are evaluated against segment data to mask deleted rows at query time.

```
                 serialized expr plan
 Delete(expr) ───────────────────────────► WAL ──► DataNode L0 buffer
   (Proxy)                                            │
                                                      ▼
                                       predicate deltalog file (StorageV1)
                                                      │
                              SegmentInfo.predicate_deltalogs (etcd)
                                                      │
                                                      ▼
                          QueryNode: load deltalog → evaluate expr → delete bitmap
```

## Data Model

A predicate delete is **not** a `(pk, ts)` pair; it is a `(serialized_expr_plan, ts)`
pair. Predicate-delete entries therefore use their own deltalog schema, distinct from the
PK delta `(pk, ts)` schema.

### Predicate deltalog file format

A predicate deltalog file uses the Milvus binlog container (magic number + descriptor
event + `DeleteEventType` event header/data) with a **Parquet payload** of two columns:

| col | name | type | field id |
|-----|------|------|----------|
| 0 | `delete_ts` | `INT64` | `predicateDeleteTimestampField` |
| 1 | `serialized_expr_plan` | `BINARY` | `predicateSerializedExprPlanField` |

Each row is one predicate delete: the serialized expression plan and the timestamp at
which it takes effect. Columns are identified by their `PARQUET:field_id` metadata
(`internal/storage/delta_data.go: PredicateDeleteArrowSchema`).

## Phase 1 — Produce (Proxy → WAL)

`DeleteRequest` carries an optional predicate payload (serialized expression plan). When
present, Proxy does **not** resolve PKs; it forwards the predicate as a delete message
into the WAL. Downstream consumers reconstruct the predicate delete message from the
message header on replay (`recoverDeleteMsgFromHeader`), so older datanodes/querynodes
that predate the feature are unaffected because they never emit predicate deltas.

## Phase 2 — Persist (DataNode)

Predicate deletes, like PK deletes, are buffered into **L0 segments** (delete-only
segments). On sync/flush, `BulkPackWriter.writePredicateDelta`
(`internal/flushcommon/syncmgr/pack_writer.go`) writes a predicate deltalog file via the
legacy predicate deltalog writer and returns a `FieldBinlog` summary, which
`SaveBinlogPaths` persists onto `SegmentInfo.PredicateDeltalogs`.

### Storage version scope (V1 / L0 only)

L0 segments are always **StorageV1**, so predicate deltalogs are written only in the
StorageV1 (legacy) format:

- `storage.NewPredicateDeltalogWriter` supports `StorageV1` only; other versions error.
- The packed/manifest (StorageV2/V3) sync writers (`BulkPackWriterV2/V3`) do **not**
  produce predicate deltalogs; `SyncTask` leaves `predicateDeltaBinlog` nil for those
  branches. (An earlier prototype wired a packed predicate writer into the V3 manifest;
  it was removed because predicate deletes never reach an L1/data-segment sync.)

### Batch consistency

A predicate-delete batch carries parallel slices of timestamps and serialized expression
plans. They MUST be the same length; a mismatch (`len(tss) != len(serializedExprPlans)`)
is rejected with an error instead of silently truncating to the shorter slice.

### Metadata

Predicate deltalogs are tracked as a **separate channel** from PK deltalogs (rather than
mixed into the untyped `deltalogs` slice) so a reader can distinguish them without
inspecting file contents.

```proto
// data_coord.proto
message SegmentInfo {
  // predicate_deltalogs records predicate delete log summaries separately
  // from deltalogs.
  repeated FieldBinlog predicate_deltalogs = 37;
}
message SaveBinlogPathsRequest {
  repeated FieldBinlog predicate_deltalogs = 19;
}
```

PK deltalog metadata keys are field-scoped
(`datacoord-meta/deltalog/<coll>/<part>/<seg>/<pkFieldID>`). Predicate deltalogs do not
belong to any collection field, so they use a **segment-scoped key with a fixed semantic
postfix** instead of overloading a field id:

```
datacoord-meta/predicate-deltalog/<coll>/<part>/<seg>/predicate
```

(`SegmentPredicateDeltalogPathPrefix` + `PredicateDeltalogPathPostfix = "predicate"`).
The real object-storage paths are reconstructed from the `LogID`s inside the stored
`FieldBinlog`, not from the etcd key, so the semantic postfix is safe for V1/V2 path
reconstruction.

## Phase 3 — Recover & Apply (QueryNode)

On load, predicate deltalog files are read back into `(serialized_expr_plan, delete_ts)`
records. At query time the expression is evaluated against the segment to mask the rows
it matches whose `delete_ts` precedes the query timestamp, producing a deletion bitmap
with the same MVCC semantics as PK deletes.

## Compatibility & Rollback

- **Forward compatible**: a cluster that issues no predicate deletes writes no predicate
  deltalogs; `predicate_deltalogs` is empty and ignored everywhere.
- **Rollback safe**: predicate deltalogs live in dedicated proto fields and a dedicated
  etcd prefix, fully separate from PK deltalogs. A pre-feature binary simply ignores the
  extra metadata; PK delete behavior is unchanged. (A simplification that folded
  predicate deltalogs into the shared `deltalogs` slice via a sentinel field id was
  rejected for this rollback-compatibility reason.)

## Key Code

| Concern | Location |
|---|---|
| Predicate deltalog schema | `internal/storage/delta_data.go` (`PredicateDeleteArrowSchema`) |
| Predicate deltalog writer | `internal/storage/rw.go` (`NewPredicateDeltalogWriter`), `internal/storage/serde_delta.go` (`NewLegacyPredicateDeltalogWriter`) |
| Sync write | `internal/flushcommon/syncmgr/pack_writer.go` (`writePredicateDelta`), `task.go` |
| L0 buffering | `internal/flushcommon/writebuffer/l0_write_buffer.go` |
| Metadata keys | `internal/metastore/kv/datacoord/constant.go`, `util.go` |
| Proto | `pkg/proto/data_coord.proto` (`SegmentInfo.predicate_deltalogs`, `SaveBinlogPathsRequest.predicate_deltalogs`) |
