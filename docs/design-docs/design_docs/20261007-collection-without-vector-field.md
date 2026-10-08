# Collections Without a Vector Field

- **Created:** 2026-10-07
- **Status:** Draft
- **Author(s):** @czs007
- **Component:** Proxy (DDL / load validation, REST), DataCoord (compaction trigger, index meta, import, segment sizing), StreamingNode (segment sealing), DataNode (import, compaction writers), pymilvus ORM
- **Related issues:** #52065 (scalar-only collection, scope of this design), #33853 (server-side "schema does not contain vector field" check added in 2.4.4), #51185
- **Related work:**
  - `docs/design-docs/design_docs/20260708-vector-anchored-join.md` (MEP: the scalar-only table is the data-model half of its Phase 0; this design is a prerequisite subset, see Non-goals)
  - `docs/design-docs/design_docs/20260909-datacoord-segment-change-staging-design.md` (Readiness Predicate, `segmentIndexed` term; the pure-scalar edge case it names is resolved here)
  - `docs/design-docs/design_docs/20260413-drop-collection-field-design.md` and `20260715-online-schema-evolution.md` ("last vector field" rule; unchanged by this design, clarified)

## Summary

Milvus requires every collection schema to declare at least one vector field
(dense or sparse). The requirement is enforced in two places at `CreateCollection`
and is assumed, mostly implicitly, by a small number of code paths downstream.
Everything else in the system (segcore, QueryNode, QueryCoord, storage, WAL,
DataNode flush, compaction execution) is written per field or per index and
works unchanged on a schema with no vector field.

This design removes the requirement. A collection may be created with scalar,
JSON, array, text and struct fields only. Such a collection supports insert,
upsert, delete, query, get, count, iterators, scalar indexes, partition keys,
clustering keys, bulk import, compaction, load/release, field-level load lists,
and schema evolution (add field, add function). `search`, `hybrid_search` and
`search_iterator` return one uniform error. No new schema-level proto field,
collection property or "table type" is introduced: "has no vector field" is
derived from the schema. The only proto addition is `CompactionPlan.max_rows`,
internal to DataCoord / DataNode.

The change set is:

1. one predicate, `typeutil.HasVectorField(schema)`, that every "does this
   collection have vectors" decision routes through;
2. a feature gate, `common.enableCollectionWithoutVector` (default `true`), that
   guards only the `CreateCollection` entry;
3. eleven call sites fixed (five hard rejections, two always-true rejections,
   three silent misbehaviours, one row-count hazard);
4. a new per-segment row cap, `dataCoord.segment.maxRows` (default 4,000,000),
   enforced at the three places that already enforce the byte cap. It applies
   to every collection; today only collections with very narrow rows reach it.

The row cap is the only part of the design that is not a pure removal of an
assumption. It is justified by measurement: delete application inside one
sealed segment is serial at roughly 8 µs per primary key, so a 1 GB segment of
13-byte rows (44 M rows) stalls the channel's tsafe for 28 s on a 10 % delete,
while eight segments of 5.5 M rows stall 5.8 s under the same delete.

## Motivation

- **#52065** asks for a scalar-only collection: schema validation lifted,
  `search` fails with a clear error, query / get / delete / upsert / iterator
  unchanged, load skips vector-index requirements, resource estimation
  accounts for pure-scalar segments, scalar indexes / partition key /
  clustering key unchanged, usable as a sub-query or join inner table.
- **Vector-anchored join MEP, Phase 0** ships join operators on top of the
  existing access paths and needs a collection with no vector field as the
  scalar side. The MEP defers its own storage engine and Global KV Index; the
  schema relaxation is the part it can build on today.
- **External tables** and metadata / lookup tables are rejected today for the
  same reason even though nothing in their pipeline uses a vector.
- Users work around the restriction with a placeholder `BINARY_VECTOR(dim=8)`
  column (1 byte per row) plus a `BIN_FLAT` index. The placeholder costs an
  index build per segment and a load-time index, and every tool that reads the
  schema sees a vector field that is not one.

## Goals / Non-goals

### Goals

- A schema with zero vector fields (counting top-level fields, struct sub-fields
  and function output fields) is accepted by `CreateCollection` over gRPC and
  REST, by `AlterCollection` paths that re-validate the schema, and by
  `milvus-backup`-style restore that replays `CreateCollection`.
- Insert / upsert / delete / query / get / count / iterators / bulk import /
  flush / compaction (mix, sort, L0, clustering, force-merge) / load / release /
  field-level load lists / add field / add function / drop field / drop
  function behave as on a vector collection, with no vector-specific
  precondition.
- `search`, `hybrid_search`, `search_iterator` and search-by-primary-key
  fail with one user-facing error at the Proxy entry, before `anns_field`
  resolution.
- Segment sealing, compaction output sizing and import segment assignment
  bound both bytes and rows per segment.
- The predicate and the gate are the only new concepts. `DescribeCollection`
  output is unchanged; old clients observe nothing new.

### Non-goals

- A "table" object, a join key, a mutability-first (LSM) store or a Global KV
  Index. Those belong to the vector-anchored join MEP. If the MEP later needs an
  explicit type marker, it is an attribute layered on top of this predicate.
- Allowing a collection to drop its last vector field or its last
  vector-producing function. The rule stays as documented in the drop-field and
  schema-evolution designs; this design only fixes the check so that it no
  longer fires on collections that have no vector field to begin with.
- Making the quick-setup paths (`MilvusClient.create_collection(dimension=...)`,
  Go SDK `SimpleCreateCollectionOptions`, REST `quickCreate`) produce a
  collection without a vector. Their semantics are "vector collection".
- Parallelising delete application inside a segment, or replacing the schema
  based `EstimateSizePerRecord` callers. Both are independent follow-ups and
  are listed at the end.

## Definitions

**HasVectorField.** A schema *has a vector field* if any of the following is a
vector type (`typeutil.IsVectorType`, which includes `ArrayOfVector`):

- a top-level field in `schema.Fields`;
- a sub-field of any `schema.StructArrayFields[*].Fields`;
- an output field of any `schema.Functions[*]` (function outputs are always
  vector typed today, `function/validator/validator.go:171`, so this clause is
  redundant with the first two but stated so that the predicate stays correct
  if that changes).

`typeutil.GetVectorFieldSchemas` already enumerates the first two sets. The new
predicate is:

```go
// HasVectorField reports whether the schema declares at least one vector
// typed field (top-level, struct sub-field or function output).
func HasVectorField(schema *schemapb.CollectionSchema) bool {
    return len(GetVectorFieldSchemas(schema)) > 0
}
```

**Collection without a vector field** (also "scalar-only collection" in
#52065): a collection whose current schema satisfies `!HasVectorField`.
Because `add_field` and `add_function` can introduce a vector field later, the
property is a property of the schema version, not of the collection's
identity. All consumers evaluate it on the schema they hold.

## Design

### 1. Feature gate

```go
p.EnableCollectionWithoutVector = ParamItem{
    Key:          "common.enableCollectionWithoutVector",
    Version:      "2.7.0",
    DefaultValue: "true",
    Doc:          "Allow CreateCollection with a schema that declares no vector field. " +
                  "Only the CreateCollection entry is gated; every other path handles such collections unconditionally.",
    Export:       true,
}
```

Scope of the gate: `createCollectionTask.PreExecute` only. When `false`, the
Proxy returns the existing error `schema does not contain vector field`. All
other changes in this design are unconditional because they are either
bug fixes on code that is always-true / always-false for a zero-vector schema,
or they only take effect when such a collection already exists.

Why a gate at all: during a rolling upgrade a new Proxy can create a collection
that an old DataCoord cannot compact (C1 below) and an old Proxy cannot load
(A4 below). Operators keep the gate off until the control plane is upgraded.
The default is `true` on master so that CI and new deployments exercise the
feature; release branches may ship it `false` initially.

### 2. Touchpoints

The survey classified every code site that assumes a vector field. The table
is the full list; file:line refer to master `6140f5f55ca`.

#### 2.1 Hard rejections (must change)

| # | Site | Today | Change |
|---|------|-------|--------|
| A1 | `internal/proxy/task.go:432` `createCollectionTask.PreExecute` | `vectorFields == 0` → `schema does not contain vector field` | Keep the check only when the gate is off. The `MaxVectorFieldNum` upper bound stays. |
| A2 | `internal/proxy/util.go:1048` `validateLoadFieldsList` (called from `CreateCollection` at `task.go:567`) | Counts vector fields whose `skip_load` is false; zero → `cannot config all vector field(s) skip loading` | Condition the count on `HasVectorField(schema)`: "if the schema has vector fields, at least one of them must be loaded". The primary / partition / clustering key rules stay. |
| A3 | `internal/proxy/metacache/meta_cache.go:298` `SchemaInfo.validateLoadFields` | A user-supplied `load_fields` list must contain a vector field | Same conditioning as A2. Without `load_fields` the list comes from `common.GetCollectionLoadFields`, which has no such check. |
| A4 | `internal/proxy/task.go:3141` (`loadCollectionTask.Execute`) and `:3410` (`loadPartitionsTask.Execute`) | `DescribeIndex` returning `ErrIndexNotFound` is converted into `WrapErrIndexNotFoundForCollection` and the load fails | Fold the "collection has no index" case into the check that follows it, `unindexedVecFields`: a load fails only if a loaded vector field has no index. With no vector fields, no index is required. DataCoord's `DescribeIndex` keeps returning `NotFound` for a collection with no index; the Proxy treats that as an empty list here. |
| A5 | `internal/distributed/proxy/httpserver/utils.go:3061` `anyToColumns` | `no vector field && no function && !partialUpdate` → reject | Delete the condition. Row count already comes from the request body; the gRPC insert path has no equivalent check. |

#### 2.2 Always-true rejections (bug fixes)

| # | Site | Today | Change |
|---|------|-------|--------|
| B1 | `internal/proxy/task.go:1297` `validateDropStructArrayField` | `removedVectors >= len(GetVectorFieldSchemas(schema))`; `0 >= 0` is true, so every struct-array drop on a zero-vector collection is rejected with "it would leave no vector field" | `removedVectors > 0 && removedVectors >= len(...)`, matching RootCoord's `ddl_callbacks_alter_collection_schema.go:612`. |
| B2 | `internal/proxy/task.go:1337` `validateDropFunction` | Same expression, same effect for every function drop | Same fix. |

The "cannot drop the last vector field" rule itself is unchanged
(`task.go:1244`, `schemautil/schema_evolution.go:375`, RootCoord `:612`); it
already only fires when the dropped field is a vector.

#### 2.3 Silent misbehaviour

| # | Site | Today | Change |
|---|------|-------|--------|
| C1 | `internal/datacoord/util.go:77` `FilterInIndexedSegments` with `index_meta.go:750` `GetIndexedSegments` | `targetFieldIds` = vector fields (+ scalar-indexed fields when `DVForceAllIndexReady`). With `indexBasedCompaction=true` (default) and `skipNoIndexCollection=false` (periodic trigger, single-segment policy), a collection with no entry in `indexMeta.indexes` makes `GetIndexedSegments` return `nil` and every segment is filtered out: no L1 mix compaction and no delete-ratio compaction ever run. With only scalar indexes, `fieldIDSet` is empty so `checkSegmentState` is vacuously true for any segment that has a `segmentIndex` record (even one still building) and false for segments without a record. | In `FilterInIndexedSegments`, after computing `targetFieldIds`: if it is empty, return every flushed / dropped-state segment of that collection without consulting `GetIndexedSegments`. Rationale: `indexBasedCompaction` exists to avoid rebuilding a vector index after compaction; with no vector index to wait for there is nothing to gate on. This equals today's behaviour for a vector collection with `DVForceAllIndexReady=false`, where scalar indexes do not block compaction. The staging design's `segmentIndexed` term is updated accordingly (its text currently says "must explicitly degrade to all scalar indexes Finished"; the agreed semantics is "empty target set → term is true"). |
| C2 | `internal/datacoord/index_meta.go:1497` `AllDenseWithDiskIndex` | `len(dense) == len(denseWithDiskIndex)`; `0 == 0` is true, so `getExpectedSegmentSize` (`compaction_trigger_v2.go:872`) returns `diskSegmentMaxSize` (2048 MB) for a collection with no dense vector. Mix / force-merge / import outputs become twice the streaming seal size. Sparse-only collections already show this. | `len(dense) > 0 && len(dense) == len(denseWithDiskIndex)`. |
| C3 | `internal/datanode/importv2/util.go:713` `GetInsertDataRowCount` | Comment states "each collection must contain at least one vector field, there must be one field whose row number is not 0". The function returns the first non-zero row count; with `auto_id` primary key and every remaining field eligible for zero-row fill (nullable / default / dynamic / function output) it returns 0 and the batch is skipped as "0 row was imported". | Remove the assumption from the comment. Keep the current algorithm (any non-zero field decides the row count) and, when all present fields have zero rows, return the row count the reader reported for the batch instead of 0. Add a unit test with a schema of `auto_id` pk + nullable fields only. The reader-side question (whether JSON / Parquet / CSV readers materialise rows for columns absent from the file) is part of Verification. |

#### 2.4 Search entry (behaviour kept, message unified)

Today four messages describe the same situation:
`anns_field not found in schema` (`proxy/dql/task_search.go:1321`),
`field (x) to search is not of vector data type` (`planparserv2`),
`no vector field found in schema` (search by primary key, `proxy/impl.go:3626`),
`cannot find a vector field` (REST `handler_v2.go:1971`). pymilvus raises
`there should be at least one vector field` before the request leaves the
client for `search_iterator`.

Change: at the Proxy entry of `Search`, `HybridSearch` (each sub-request),
`SearchIterator` and search-by-primary-key, before `anns_field` resolution,
return

```
collection %s has no vector field, search is not supported
```

with `ErrParameterInvalid`. REST handlers return the same text. The deeper
checks stay as defensive guards.

#### 2.5 REST and clients

- REST `describe` (`handler_v2.go:868`, `handler_v1.go:406`) passes the first
  vector field name to `DescribeIndex`, or an empty string when there is none,
  which lists all indexes. The behaviour is already right; the comment is
  updated.
- pymilvus ORM `check_schema` (`orm/schema.py:1330`) rejects a schema without a
  vector field on the client. It is removed (it also fails to count vector
  sub-fields of struct arrays today). `MilvusClient.create_collection(schema=)`
  has no such check. Go SDK has none.
- Quick-setup paths are not changed (Non-goals).

### 3. Per-segment row cap

#### 3.1 Why bytes alone are not enough

Segment size is bounded by bytes everywhere that matters on master:
StreamingNode seals L1 segments by the accumulated insert-message
`BinarySize` against `dataCoord.segment.maxSize × sealProportion`
(`segment_limitation_policy.go`, `SegmentRows` is `MaxUint64` for L1);
compaction targets and candidate buckets use binlog `MemorySize`
(`getExpectedSegmentSize`, `SegmentInfo.getSegmentSize`); import splits
by preimport-measured memory size (`AssignSegments`); QueryCoord admission
and the QueryNode loader use `MemorySize`. The schema-based
`EstimateSizePerRecord` no longer determines segment size (its remaining
callers are batching heuristics; see Follow-ups).

A vector field makes the byte cap also an implicit row cap: at 3 KB per row
(dim 768 float32) a 1 GB segment holds about 350 k rows; at 512 B (dim 128)
about 2 M rows. Without a vector field the same 1 GB holds 44 M rows of
`int64 + int32` or 4.9 M rows of `varchar(avg 64) + JSON(avg 115 B)`. The
per-segment paths that are linear in rows and serial per segment are then
exposed. Measured on a single-node master build (aarch64, 20 cores), one
44 M-row sealed, sorted segment versus eight 5.5 M-row segments of the same
rows:

| Path | 1 × 44 M rows | 8 × 5.5 M rows | Note |
|------|---------------|----------------|------|
| Delete 1 % by expression, tsafe stall | 3.8 s | 0.7 s | strong-consistency queries fail above the 3 s `tsafe` lag limit |
| Delete 10 % by expression, tsafe stall | 28.6 s | 5.8 s | ~8 µs per primary key, serial inside a segment, parallel across segments |
| INVERTED index build on one int32 field | 28.6 s | 8.5 s wall, 8 tasks in parallel | linear in rows, one task per segment |
| Load, RSS delta | +5.3 GB | +5.3 GB | memory is per row, not per segment |
| Point / filter / count query latency | 2–4 ms | 2–4 ms | unaffected |

Delete application is the item that affects availability: the delegator does
not advance a channel's tsafe until the delete is applied to every affected
segment, and inside a sealed segment the primary-key lookup is serial.
Memory and query latency do not need a cap.

Comparable systems bound both bytes and rows per scheduling unit: Druid
segments (`maxRowsPerSegment` default 5,000,000, about 100 B per row at its
typical 300–700 MB), TiKV regions (96 MB or 960,000 keys, whichever first),
Elasticsearch shards (10–50 GB and ≤ 200 M documents). ClickHouse parts are
bytes-only because no per-row serial path exists at the part level.

#### 3.2 Parameter

```go
p.SegmentMaxRows = ParamItem{
    Key:          "dataCoord.segment.maxRows",
    Version:      "2.7.0",
    DefaultValue: "4000000",
    Doc:          "The maximum number of rows in a sealed segment. A segment is sealed, a compaction output " +
                  "is split, and an import segment is assigned when either this or dataCoord.segment.maxSize " +
                  "is reached. 0 disables the row cap.",
    Export:       true,
}
```

Default 4,000,000: 250 B per row at 1 GB, the same ratio Elasticsearch
recommends, and the point where a 10 % single-request delete on one segment
(400 k keys × 8 µs = 3.2 s) sits at the tsafe lag limit. 5,000,000 (Druid)
is equally defensible; the difference is at the margin.

The cap applies to every collection. Vector collections with dim ≥ 32 float32
do not reach it before the byte cap (Compatibility lists the ones that do).

#### 3.3 Enforcement points

All three places already carry a row field; the change is to populate it.

1. **StreamingNode seal (growing → sealed).**
   `jitterSegmentLimitationPolicy.generateL1Limitation` sets
   `SegmentRows: math.MaxUint64`. Set it to
   `jitterRatio × maxRows × sealProportion` when `maxRows > 0` (same jitter and
   proportion as the byte limit, so the two caps scatter seals the same way).
   The value already travels in `CreateSegmentMessageHeader.MaxRows` into
   `SegmentStats.MaxRows`, where the allocation path compares assigned rows
   against it; L0 segments use this path today with `FlushL0MaxRowNum`.
2. **Compaction outputs.** `CompactionPlan.max_size` bounds mix / sort /
   force-merge outputs on the DataNode; `MultiSegmentWriter` already receives a
   `maxRows` argument but uses it only to size the bloom filter. Add
   `CompactionPlan.max_rows` (populated from the parameter by DataCoord next to
   `max_size`) and make the writer roll to a new output segment when either
   bound is reached. Clustering compaction already carries
   `MaxSegmentRows` / `PreferSegmentRows`; cap both with `maxRows`. The
   DataCoord trigger's bucket packing (`squeezeSmallSegmentsToBuckets`,
   `compaction_trigger.go:704`) sums `getSegmentSize`; add the same check on
   `NumOfRows` so that a plan does not merge more rows than one output may
   hold.
3. **Import.** `AssignSegments` (`import_util.go:165`) allocates
   `ceil(size / segmentMaxSize)` segments per vchannel/partition from the
   preimport hashed sizes. Preimport also reports row counts
   (`ImportFileStats.TotalRows`, hashed per vchannel/partition); allocate
   `max(ceil(size / maxSize), ceil(rows / maxRows))` segments. The DataNode
   side picks a segment per batch (`PickSegment`) by vchannel/partition; it
   needs no change because the number of segments already bounds the average,
   and the sort compaction that follows import re-splits by the compaction
   rule above.

Import V3 (PR #53334, open) changes the import pipeline but keeps the
preimport-measured rows and sizes and the sorted-fragment writer; the same rule
applies to its fragment target.

### 4. Resource estimation

The QueryNode loader estimate (`loadresource.EstimateSegmentLoadingResource`)
is built from binlog `MemorySize` with per-type factors. Measured against
process RSS on the same single-node build:

| Schema | binlog MemorySize | loader estimate | RSS delta | Notes |
|--------|------------------:|----------------:|----------:|-------|
| `int64 + int32` (+1 B placeholder vector) | 29 B/row | 41 B/row | 120 B/row | fixed per-row costs (bloom filter 3 B, timestamp index, offsets, arrow decode buffers) dominate |
| `int64 + int32 + varchar(avg 64) + JSON(avg 115)` | 217 B/row | 423 B/row | 265–422 B/row | varchar / JSON are counted twice by the loader (`doubleMemoryDataType`), which covers the fixed costs |

The estimate is adequate for variable-length schemas and low by about 3× for
narrow fixed-width schemas. This design does not change the estimator; the
per-row constant for fixed-width columns is listed under Follow-ups with the
measured numbers so that it can be calibrated separately. With `maxRows`
in place, the absolute error per segment is bounded (4 M rows × ~80 B =
320 MB).

## Per-path adaptation and edge cases

- **External collections.** `IsExternalCollection` schemas pass through A1 / A2
  and become creatable without a vector. Their refresh / add-column pipeline
  does not use vectors. The manifest index publication path shares the C1
  readiness chain and is covered by the C1 unit test.
- **Becoming a vector collection later.** `add_field` with a dense vector
  (requires `dim`, `nullable`) and `add_function` (BM25 / text embedding /
  MinHash) are allowed today and turn a zero-vector collection into a vector
  collection. From that schema version on, `search` works once an index exists
  and load requires the index as usual. Existing sealed segments have no data
  for the new field; the nullable / backfill rules of online schema evolution
  apply unchanged.
- **Dropping the last vector field / function** stays rejected (Non-goals).
- **`GetQueryVChanPositions` (`handler.go:168`)** uses the same
  `FilterInIndexedSegments`; with C1 it regains the compaction-parent fallback
  for zero-vector collections.
- **Interim index.** `ComposeIndexMeta` computes `MaxIndexRowCount` from
  `EstimateSizePerRecord`; with no vector field no `VecIndexConfig` is
  constructed and the value is unused.
- **Metrics.** `ProxyInsertVectors`, `ProxyUpsertVectors`,
  `DataCoordBulkVectors` count rows and will count rows of zero-vector
  collections. Names are kept.
- **Quick-setup paths** keep requiring `dimension` (Non-goals).
- **Sparse-only collections** are vector collections for every predicate here;
  C2 changes nothing for them (`len(dense) == 0` already made the expression
  true, now it is explicitly false: their compaction target drops from 2048 MB
  to 1024 MB, aligning with the streaming seal size). This is a behaviour
  change for sparse-only collections and is listed under Compatibility.

## Compatibility

- **Wire / meta.** No proto change except `CompactionPlan.max_rows` (new
  optional int64; a DataNode that does not know it ignores it and splits by
  bytes only). No change to `CollectionSchema`, `DescribeCollection`, etcd
  layout or binlog format.
- **Rolling upgrade.** Keep `common.enableCollectionWithoutVector=false` until
  Proxy, DataCoord and QueryCoord are on the new version. Risk if enabled
  early: old DataCoord never compacts the new collection (C1), old Proxy
  rejects its load (A4). Downgrade with such a collection present re-introduces
  both symptoms; the collection remains readable and droppable.
- **Clients.** pymilvus ORM below the fixed version refuses the schema on the
  client; `MilvusClient` works. Java / Go / Node SDKs have no client check.
- **Backup / restore.** `milvus-backup` replays `CreateCollection`; restoring
  such a collection into an older server fails with the old A1 error. The
  backup tool should surface the server version in that message (out of this
  repository).
- **CDC.** Replicating `CreateCollection` of a zero-vector collection to an
  older target cluster fails at A1 on the target. Same mitigation.
- **Row cap on existing collections.** Collections whose rows are narrower than
  `maxSize / maxRows` = 268 B at defaults will seal, compact and import smaller
  segments after the upgrade: `BINARY_VECTOR` with dim ≤ 2048, `FLOAT16` /
  `BFLOAT16` with dim ≤ 128, `FLOAT_VECTOR` with dim ≤ 64, and sparse-only
  collections with few non-zeros. Existing segments are not re-split; new
  segments are smaller. Operators can restore the old behaviour with
  `dataCoord.segment.maxRows=0`.
- **Sparse-only collections** get a 1024 MB compaction / import target instead
  of 2048 MB (C2).
- **Staging design.** `segmentIndexed` for an empty target set is defined as
  true. The staging document is amended in the same PR series.

## Verification

### Unit tests

- `typeutil.HasVectorField`: top-level only, struct sub-field only, function
  output only, none, `ArrayOfVector`.
- A1 with gate on / off; A2 / A3 with and without vector fields and with
  `skip_load` on every vector; A4 load of a collection with no index and no
  vector (succeeds), with no index and a vector (fails as today), with a
  scalar index only; A5 REST insert / upsert without vector.
- B1 / B2: drop struct field and drop function on a zero-vector collection
  succeed; dropping the last vector still fails.
- C1: `FilterInIndexedSegments` with no index meta entry and with scalar
  indexes only returns all flushed segments; with a vector index behaves as
  today. C2: `AllDenseWithDiskIndex` false for zero dense fields. C3:
  `GetInsertDataRowCount` with `auto_id` pk and nullable-only fields.
- Search entry: one error text across `Search`, `HybridSearch`,
  `SearchIterator`, search-by-pk, REST.
- Row cap: StreamingNode limitation with `maxRows` set / 0; `MultiSegmentWriter`
  rolls at `max_rows`; `AssignSegments` allocates by rows; clustering caps.

### End-to-end (`tests/python_client/milvus_client/`)

1. Create (int64 pk, int32, varchar, JSON, array, nullable fields; with and
   without partition key and clustering key) → insert → flush → load without
   any index → query / count / get / iterator / delete / upsert → release.
2. Same with INVERTED + BITMAP scalar indexes; `load_fields` subset without a
   vector.
3. Bulk import (Parquet, JSON) → segments sorted → compaction triggered with
   `indexBasedCompaction=true` and no index → segment count decreases.
4. `search` / `hybrid_search` / `search_iterator` return the unified error.
5. `add_field` dense vector + index → `search` works; drop that field is
   rejected.
6. Gate off → create rejected with the legacy error.
7. REST v2 create / insert / upsert / query / delete.
8. Row cap: import 12 M narrow rows → ≥ 3 segments; streaming insert past
   `maxRows × sealProportion` → seal observed.

### Measurements already done (2026-10-07, master `036bef9e9f`, single node)

Reported in Section 3.1 and Section 4. Remaining measurements, none blocking:
growing-segment (streaming insert) path on a narrow schema; `STL_SORT` /
`BITMAP` build time per segment; delete by primary-key list (different
batching from delete by expression); x86 delete-apply cost per key (8 µs is
aarch64).

## Alternatives considered

- **Explicit collection type / proto flag ("scalar table").** Rejected: adds a
  field every client and tool must learn, and the property is already
  derivable from the schema. The join MEP can layer a marker later.
- **Stricter C1 (require all scalar indexes Finished).** Rejected: it would make
  zero-vector collections wait on scalar indexes that vector collections do not
  wait on, and scalar indexes are rebuilt per segment after compaction either
  way.
- **Fold a per-row overhead into "segment size"** (size = bytes + rows ×
  constant) instead of a separate row cap. Rejected: it changes the meaning of
  `maxSize` for every collection (a dim-768 segment shrinks by ~3 %), while a
  separate cap is a no-op below the threshold.
- **No row cap, rely on delete parallelisation.** Rejected for now: the
  parallelisation is a segcore change with its own review; the cap is a
  configuration-level guard that can be relaxed when the former lands.
- **Allow dropping the last vector field.** Deferred to the schema-evolution
  document.

## Follow-ups (out of scope, tracked separately)

- Parallelise delete application inside a sealed segment (split the sorted
  primary-key column into ranges), or replace per-key binary search with a
  single merge of the sorted delete keys against the sorted primary-key
  column. With either, `maxRows` can be raised.
- Loader estimate: add a per-row constant for fixed-width columns (measured
  gap ~80 B per row on `int64 + int32`), not for variable-length columns
  (already covered by the 2× factor).
- Remove the deprecated `SegmentInfo.MaxRowNum` computation
  (`calBySchemaPolicy`) and the four row-based seal policies; their only entry
  (`AssignSegmentID`) has no caller.
- Import file readers: derive the per-step row count from file metadata
  (Parquet row-group sizes, NumPy header) with per-step adaptation instead of
  `EstimateMaxSizePerRecord`; keep the actual-memory outer loop.

## References

- Issue #52065, #33853, #51185
- PR #53334 `feat: implement Import V3 reshard` (open)
- Druid segment optimization: https://druid.apache.org/docs/latest/operations/segment-optimization
- TiKV coprocessor configuration: https://tikv.org/docs/4.0/tasks/configure/coprocessor/
- Elasticsearch shard sizing: https://www.elastic.co/blog/how-many-shards-should-i-have-in-my-elasticsearch-cluster
