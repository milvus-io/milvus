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
derived from the schema. There is no proto change.

The change set is:

1. one predicate, `typeutil.HasVectorField(schema)`, that every "does this
   collection have vectors" decision routes through;
2. a feature gate, `common.enableCollectionWithoutVector` (default `true`), that
   guards only the `CreateCollection` entry;
3. eleven call sites fixed (five hard rejections, two always-true rejections,
   three silent misbehaviours, one row-count hazard);
4. two index-readiness predicates in DataCoord: handoff (data view publication)
   waits for every index defined on the collection, compaction input waits only
   for vector indexes; a collection with no index defined, or no vector field,
   satisfies them without a special case;
5. a per-collection segment size target derived from a new row target,
   `dataCoord.segment.maxRows` (default 4,000,000):
   `expectedSize = min(maxSize, maxRows × estimated bytes per row)` (schema
   estimate plus the two system fields, the same bytes that binlog
   `MemorySize` counts), computed
   in the existing `getExpectedSegmentSize` that every DataCoord size
   consumer already reads, and in the StreamingNode seal limitation. The
   planner, the output writer and import stay byte-only. Collections whose
   rows are wider than `maxSize / maxRows` (268 B at defaults) are unaffected.

The size target is the only part of the design that is not a pure removal of
an assumption. It is justified by measurement: delete application inside one
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
  use a per-collection byte target that bounds the rows per segment of a
  narrow-row collection.
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
- Parallelising delete application inside a segment, or removing the
  remaining batching-heuristic callers of the schema-based
  `EstimateSizePerRecord`. Both are independent follow-ups and are listed at
  the end.

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
| C1 | `internal/datacoord/util.go:77` `FilterInIndexedSegments` with `index_meta.go:750` `GetIndexedSegments` | `targetFieldIds` = all vector fields (+ all indexed fields when `dataCoord.dataview.forceAllIndexReady`). `GetIndexedSegments` returns `nil` when any target field has no index, so with `indexBasedCompaction=true` (default) and `skipNoIndexCollection=false` (periodic trigger, single-segment policy, `GetQueryVChanPositions`) every segment is filtered out until an index exists on every vector field. For a vector collection that is a wait; for a collection with no vector field it is permanent: no L1 mix compaction and no delete-ratio compaction ever run, and compaction children never become visible. | Split the one filter into the two predicates of Section 2.6. Compaction input: when the vector target set is empty, every flushed / dropped-state segment passes. Handoff: wait for all indexes defined on the collection. |
| C2 | `internal/datacoord/index_meta.go:1497` `AllDenseWithDiskIndex` | `len(dense) == len(denseWithDiskIndex)`; `0 == 0` is true, so `getExpectedSegmentSize` (`compaction_trigger_v2.go:872`) returns `diskSegmentMaxSize` (2048 MB) for a collection with no dense vector. Mix / force-merge / import outputs become twice the streaming seal size. Sparse-only collections already show this. | `len(dense) > 0 && len(dense) == len(denseWithDiskIndex)`. |
| C3 | `internal/datanode/importv2/util.go:713` `GetInsertDataRowCount` and the four file readers | The comment states "each collection must contain at least one vector field, there must be one field whose row number is not 0". With `auto_id` primary key and every remaining field eligible for zero-row fill (nullable / default / dynamic / function output), a file that contains none of the schema's columns is accepted by schema validation (`parquet/util.go:isSchemaEqual` skips nullable / default / dynamic fields; `numpy/util.go:CreateReaders` ignores paths that name no field). The reader then has an empty column-reader set: Parquet `Read()` never executes its inner loop and never reaches `io.EOF`, NumPy `Read()` returns a zero-row batch on every call, and `importFile` logs "0 row was imported" and calls `Read()` again. The task spins until cancelled. | Reject at reader construction, before any row is produced: Parquet and NumPy fail with `ErrImportFailed("no importable field found in <file>")` when the matched column set is empty; CSV applies the same rule to its header; JSON rejects a row object that supplies no schema field and no dynamic key. Files that supply a matching column whose values are all null remain importable. `GetInsertDataRowCount` keeps its algorithm (any populated field decides the row count); the comment is corrected, and the zero result is treated as an internal error (`importFile` returns it instead of continuing) because the readers now guarantee at least one populated column per batch. |

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

#### 2.6 Index readiness: handoff versus compaction input

`FilterInIndexedSegments` serves two different questions with one target set:

- **Handoff** (`handler.go:168` `GetQueryVChanPositions`, and the
  `segmentIndexed` term of the staging design): may this segment replace its
  compaction parents in the data view?
- **Compaction input** (`compaction_trigger.go:365`,
  `compaction_policy_single.go:290`): may this segment be consumed by another
  compaction?

The design separates them. Both are evaluated against the indexes defined in
`indexMeta` for the collection, not against the schema, so neither needs a
vector-field special case:

| Predicate | Target set | Empty target set | Used by |
|-----------|-----------|------------------|---------|
| `handoffReady(segment)` | every non-deleted index of the collection, scalar and vector (`GetIndexesForCollection`) | true (no index defined) | `GetQueryVChanPositions`, staging `segmentIndexed` |
| `compactionInputReady(segment)` | indexes on vector fields only | true (no vector field, or no vector index defined) | periodic trigger, single-segment policy |

`handoffReady` is the current behaviour of `dataCoord.dataview.forceAllIndexReady=true`
made unconditional; the parameter is removed. The `smallExempt` term of the
staging design (`NumOfRows < indexCoord.segment.minSegmentNumRowsToEnableIndex`,
default 1024, sorted segments only) stays in front of it.

`compactionInputReady` keeps today's semantics for vector collections (a vector
field whose index is still building is not compacted again) and drops the gate
when there is nothing to wait for. Scalar indexes are not part of it because
they are rebuilt per output segment after every compaction, so waiting for them
on the input side delays the compaction without saving work. This matches what
a vector collection does with its scalar indexes today.

Consequences of waiting for scalar indexes at handoff, for every collection:

- **Publication latency.** A compaction output becomes visible after its scalar
  indexes finish instead of after its vector index. Measured on the test build:
  `INVERTED` on one `int32` field builds in 8.5 s for 5.5 M rows and 28.6 s for
  44 M rows; at the 4 M-row target this adds about 6 s per segment, and scalar
  index tasks for sibling segments run in parallel. Until publication the
  parents keep serving, so the cost is index freshness of the children, not
  availability.
- **Small segments.** Segments below `minSegmentNumRowsToEnableIndex` are not
  indexed and are exempt, as today.
- **Index on a field added later.** Segments written before `add_field` have
  no binlog for the field; the index task for them is finished without a
  build (`task_index.go`, "stale schema" handling), so `handoffReady` is not
  blocked by an index whose field the segment predates.
- **Index failure.** A segment whose scalar index build fails terminally never
  satisfies `handoffReady`. Under the staging design the group moves to
  `FAILED` (transition 4) and the parents stay published; without staging the
  parents are already retired and the children stay invisible, which is the
  existing behaviour for a failed vector index. The failure mode is therefore
  unchanged in kind and now has a visible terminal state.
- **Vector collection without any index.** Today such a collection's compaction
  children are invisible until an index is created on every vector field (data
  "temporarily vanishes", as the staging design notes). With `handoffReady`
  they publish as soon as data is complete. The staging design already
  specifies this as a behaviour fix.

### 3. Per-collection segment size target

#### 3.1 Why bytes alone are not enough

Segment size is bounded by bytes everywhere that matters on master:
StreamingNode seals L1 segments by the accumulated insert-message
`BinarySize` against `dataCoord.segment.maxSize × sealProportion`
(`segment_limitation_policy.go`, `SegmentRows` is `MaxUint64` for L1);
compaction targets and candidate buckets use binlog `MemorySize`
(`getExpectedSegmentSize`, `SegmentInfo.getSegmentSize`); import splits
by preimport-measured memory size (`AssignSegments`); QueryCoord admission
and the QueryNode loader use `MemorySize`. The schema-based
`EstimateSizePerRecord` no longer determines segment size on master (its
remaining callers are batching heuristics); Section 3.3 uses it for one
purpose only, to derive the per-collection target from the schema.

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
    Doc:          "Target number of rows per sealed segment. The per-collection segment size target is " +
                  "min(dataCoord.segment.maxSize, maxRows * estimated bytes per row of the schema including " +
                  "the RowID and Timestamp system fields); seal, " +
                  "compaction and import all use that target. Collections whose rows are wider than " +
                  "maxSize / maxRows are unaffected. 0 disables the row target.",
    Export:       true,
}
```

Default 4,000,000: 250 B per row at 1 GB, the same ratio Elasticsearch
recommends, and the point where a 10 % single-request delete on one segment
(400 k keys × 8 µs = 3.2 s) sits at the tsafe lag limit. 5,000,000 (Druid)
is equally defensible; the difference is at the margin.

The target applies to every collection. Vector collections with dim > 61
float32 have an estimated row width above 268 B and keep `maxSize`
(Compatibility lists the ones that do not).

#### 3.3 Row-derived segment size target

DataCoord already computes the segment size target per collection:
`getExpectedSegmentSize(meta, collectionID, schema)`
(`compaction_trigger_v2.go:872`) returns `diskSegmentMaxSize` when every
dense vector field has a DiskANN index and `maxSize` otherwise. Every size
consumer in DataCoord reads it: the legacy and two-tier planners, force-merge,
clustering (`calculateClusteringCompactionConfig` takes it as an argument),
import (`GetSegmentMaxSize`, `import_util.go:143`) and `CompactionPlan.max_size`.
The row target enters through this one function:

```
rowWidth(c)      = EstimateSizePerRecord(schema(c)) + 16     // pkg/util/typeutil/schema.go:134, plus RowID and Timestamp (int64 each)
expectedSize(c)  = min( baseSize(c), maxRows × rowWidth(c) )   // baseSize = maxSize or diskSegmentMaxSize
                                                                // maxRows = 0 → baseSize
```

The 16 B are added because the planner's bytes are binlog `MemorySize`, which
includes the two system-field binlogs: the measured 29 B per row of Section 4
for `int64 + int32` plus a 1 B placeholder vector is 13 B of user data plus
16 B. Without them the target for the narrowest schemas would be about 2×
tighter than intended (simulated: 1.7 M rows per segment instead of 4 M).

At defaults: `int64 + int32` (28 B) → 112 MB; `int64 + 4 × int64` (56 B) →
224 MB; `int64 + varchar(max 256) + JSON` → the estimate exceeds 268 B and the
target stays 1 GiB; `FLOAT_VECTOR` dim 128 (536 B) → 1 GiB. The boundary is
268 B per row, that is 252 B of user data next to the system fields.

Properties:

- **One number per collection.** Classification (`isFullSegment`,
  `isFragmentSegment`, `isSmallSegment`), `Knapsack` capacity, the output
  writer's `max_size`, import assignment and the seal limitation all compare
  bytes against `expectedSize(c)`. There is no second dimension, no change to
  `knapsack.go`, `MultiSegmentWriter` or the proto. A compaction output of
  about `expectedSize(c)` bytes is full by the same measure that selected its
  inputs, so it is never re-selected: the merge-then-split cycle that a row cap
  on outputs alone would cause (Alternatives) cannot occur.
- **Wide rows are unchanged.** `maxRows × rowWidth ≥ maxSize` whenever
  `rowWidth ≥ 268 B`, so every existing collection with a dense vector of
  dim > 61 float32 keeps today's target and today's plans.
- **Estimation error is bounded in both directions and never cycles.**
  `EstimateSizePerRecord` is exact for fixed-width fields (the null bitmap of
  a nullable column is the only omission, 1 bit per row); variable-length
  fields use the `custom` length policy, measured at 3.68× the stored size on
  the `varchar + JSON` schema of Section 4; sparse vectors are counted at 150
  non-zeros. An over-estimate raises `expectedSize` towards `maxSize` and
  weakens the target; the collections where the estimate is loose
  (variable-length) are the ones that hold at most about 5 M rows per GiB
  (Section 4) and do not need it. An under-estimate tightens the target; its
  floor is a primary key alone, `(8 + 16) × 4 M = 96 MB`. Both directions
  were simulated (estimate 0.43× to 30× the stored width): rows per segment
  scale with the ratio and no plan re-merges a full output, because the
  planner and the writer still share one number.
- **Schema change.** Adding a field raises `rowWidth` and `expectedSize`;
  segments that were full under the old target are classified small under the
  new one and merged once, as happens today when `maxSize` is raised. Rows
  ingested before the change can then share an output with a byte target
  derived from the new width, so such an output holds up to
  `newWidth / oldWidth × maxRows` of the old rows. The bound is by bytes, not a
  hard row count; the conservative variant that avoids this is listed under
  Alternatives.
- **Configuration change** of `maxRows` behaves like a change of `maxSize`
  today: one re-merge, then steady state.

A simulation of both planners (legacy and two-tier, replicated from
`knapsack.go`, `compaction_tier.go` and `compaction_trigger.go`; seal at
`0.12 × target` with jitter, outputs split by the writer, delete-ratio single
compaction, 80 trigger intervals) compares three variants: "byte-only +
output row cap" (today's planner with the row cap applied only at seal and in
the writer), "two-dimensional fullness" (the previous revision of this
document: `max(size / maxSize, rows / maxRows)` inside the planner) and this
section. Write amplification is rows rewritten by compaction divided by rows
ingested, over the last 20 intervals; a churn plan is one that re-merges a
compaction output that is already full.

| Scenario | Byte-only + output row cap | Two-dimensional fullness | This section |
|----------|---------------------------:|-------------------------:|-------------:|
| 13, 50, 134, 200 B rows, 2 M rows / interval, both planners | amplification 70, 115–1160 churn plans | 1.0, 0 | 1.0, 0; plans identical to two-dimensional fullness in every interval |
| 268 B (two-tier), ≥ 300 B (both) | identical to today | identical to today | identical to today |
| 50 B, ingest 0.7 M – 9 M rows / interval | 64 – 70 | 1.0 – 1.6 | 0.9 – 1.6, 0 churn, no segment above `maxRows` |
| 50 B, estimate k× actual (k = 2, 4, 10, 30) | — | — | 0 churn; rows per segment rise linearly to `k × maxRows`, capped at `maxSize / width` |
| 28 B, estimate 0.43× and 0.6× actual (system fields omitted) | — | — | 0 churn; 1.7 M and 2.0–2.4 M rows per segment |
| 13 B → 50 B schema change at interval 40 | — | — | all 80 M pre-change rows merged exactly once, 0 churn after; old rows per output up to 14 M |
| 50 B, 30 % deleted at interval 40 | 58, 273–280 churn plans | 1.35 (legacy) / 1.69 (two-tier), 0 | same as two-dimensional fullness |

With an exact estimate the byte target and two-dimensional fullness produce
the same plan in every interval, for every width and both planners: for a
narrow row `rows / maxRows` equals `size / (maxRows × rowWidth)` =
`size / expectedSize`. The byte target reaches the result without a second
dimension. One property of the legacy planner is unchanged in kind: outputs
that land between `segmentSmallProportion` and `compactionFillRate` of the
target are not merged again.

#### 3.4 Enforcement points

1. **DataCoord `getExpectedSegmentSize`.** Apply the formula above. Its
   consumers need no change: `generatePlansLegacy`, `generatePlansTwoTier`,
   `squeezeSmallSegmentsToBuckets`, force-merge, `calculateClusteringCompactionConfig`
   (`MaxSegmentRows` / `PreferSegmentRows` are already derived from the byte
   target and the observed bytes per row), `AssignSegments`
   (`ceil(size / expectedSize)` segments per vchannel / partition) and
   `CompactionPlan.max_size`. On the DataNode, `MultiSegmentWriter` already
   rolls to a new output at `max_size`; mix, sort, force-merge, bump-schema,
   namespace, backfill, copy-segment and the Import V3 reshard writer
   (PR #53334) inherit the smaller target, and the sort compaction that follows
   an import re-splits import outputs if the assignment under-estimated.
2. **StreamingNode seal.** `jitterSegmentLimitationPolicy.generateL1Limitation`
   (`segment_limitation_policy.go`) reads the global `maxSize`; the code
   carries a TODO that the parameter does not belong there. The shard manager
   holds the collection schema (`CollectionSchemaOfVChannel`,
   `shard_manager.go`) and the allocation request carries the schema version,
   so the policy takes the schema and sets
   `SegmentSize = jitterRatio × expectedSize(c) × sealProportion`.
   `SegmentRows` stays `MaxUint64`; L0 keeps `FlushL0MaxRowNum`. The formula
   lives in one helper (`pkg/util/segmentutil` or similar) shared with
   DataCoord so that the two components cannot diverge; the DiskANN term is
   DataCoord-only and the StreamingNode uses `maxSize` as `baseSize`, which
   only matters above 268 B where the row term is inactive.
3. **Clustering analysis.** `task_analyze.go:208` derives `numClusters` from
   the global `maxSize`; it takes `expectedSize(c)` instead. This also fixes
   the DiskANN case, which is inconsistent today.
4. **L0 segments** are excluded: their rows are delete records applied per
   target segment.
5. **Observability.** The seal and plan-generation logs print
   `expectedSize(c)` when it differs from `maxSize`, so that operators can see
   when the row target is the binding limit for a collection.

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
measured numbers so that it can be calibrated separately. With the row-derived
size target in place, the absolute error per segment of a fixed-width schema
is bounded (4 M rows × ~80 B = 320 MB).

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
- **QueryCoord** already tolerates a collection with no index: the load
  executor turns `ErrIndexNotFound` into an empty index list
  (`querycoordv2/task/executor.go:764`), and the autoscale precheck skips it
  (`autoscale_precheck.go:378`). RootCoord has no vector check at create time;
  its only one is the drop-function rule at
  `ddl_callbacks_alter_collection_schema.go:621`, already guarded by
  `removedVectors > 0`. Both verified on master, no change.
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

- **Wire / meta.** No proto change. No change to `CollectionSchema`,
  `DescribeCollection`, etcd layout or binlog format.
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
- **Smaller segments for narrow-row collections.** Collections whose
  estimated row width is below `maxSize / maxRows` = 268 B at defaults will
  seal, compact and import smaller segments after the upgrade:
  with an `int64` primary key and nothing else, a vector of at most 244 B:
  `FLOAT_VECTOR` dim ≤ 61, `FLOAT16` / `BFLOAT16` dim ≤ 122, `INT8_VECTOR`
  dim ≤ 244, `BINARY_VECTOR` dim ≤ 1952; any further field raises the
  estimate and narrows the list. Sparse-only
  collections are estimated at 150 non-zeros per row, above 268 B, and are
  unaffected whatever their actual density. Existing segments are not re-split; new
  segments are smaller, and existing full segments are not re-merged because
  the target only shrinks. Operators can restore the old behaviour with
  `dataCoord.segment.maxRows=0`.
- **Sparse-only collections** get a 1024 MB compaction / import target instead
  of 2048 MB (C2).
- **Handoff waits for scalar indexes** on every collection (Section 2.6).
  Compaction children become visible after their scalar indexes finish; the
  parents serve until then. `dataCoord.dataview.forceAllIndexReady` is
  removed; a configuration that sets it is ignored with a deprecation log.
- **Compaction planner** code is unchanged; only its size target moves
  (Section 3.3). Results are identical for any collection whose estimated row
  width is above `maxSize / maxRows`.
- **Bulk import** rejects a file that contains none of the schema's importable
  columns (C3). Today such a file makes the import task loop without progress;
  no completed import changes outcome.
- **Staging design.** `segmentIndexed` is defined as `handoffReady` of
  Section 2.6. The staging document is amended in the same PR.

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
- C1 / Section 2.6: `compactionInputReady` with no index meta entry, with
  scalar indexes only, with a vector index building, with a vector index
  finished; `handoffReady` with a scalar index building / finished / failed,
  with no index defined, below `minSegmentNumRowsToEnableIndex`. C2:
  `AllDenseWithDiskIndex` false for zero dense fields. C3: Parquet, NumPy and
  CSV readers reject a file with no importable column at construction; JSON
  rejects a row with no schema key and no dynamic key; a Parquet column of
  all nulls on a nullable field imports; `importFile` returns an error on a
  zero-row batch instead of continuing.
- Section 3.3: `getExpectedSegmentSize` on a fixed-width narrow schema returns
  `maxRows × rowWidth`, on a wide schema `maxSize`, with DiskANN
  `min(diskSegmentMaxSize, maxRows × rowWidth)`, with `maxRows=0` the base
  size; the StreamingNode limitation and DataCoord return the same value for
  the same schema; `AssignSegments` allocates `ceil(size / expectedSize)`;
  `numClusters` uses the target; `generatePlansTwoTier` and
  `generatePlansLegacy` on a channel of outputs at `expectedSize` produce no
  merge plan and still produce a single-segment plan for a segment over the
  delete-ratio threshold. The planner simulation
  (`planner_sim.py` / `target_sim.py`, attached to the implementation PR)
  provides the expected plan sequences per width.
- Search entry: one error text across `Search`, `HybridSearch`,
  `SearchIterator`, search-by-pk, REST.
- Size target: StreamingNode limitation with `maxRows` set / 0 and with a
  schema whose estimate is above / below 268 B.

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
8. Size target: import 12 M rows of `int64 + int32` → ≥ 3 segments;
   streaming insert past `expectedSize × sealProportion` (about 13 MB) →
   seal observed; after the periodic trigger has run, outputs at
   `expectedSize` are not re-compacted (segment IDs stable across two trigger
   intervals); the same schema with a dense vector of dim 128 added seals at
   the 1 GiB-derived size.
9. Import of a Parquet file with none of the schema's columns → job fails with
   the C3 error within the preimport / import timeout, not a hung task.

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
  constant) instead of a row-derived target. Rejected: it changes the meaning
  of `maxSize` for every collection (a dim-768 segment shrinks by ~3 %), while
  the `min` form is a no-op above 268 B per row.
- **Row cap on outputs only (seal, writer, import assignment), planner
  unchanged.** Rejected: the planner would classify every row-full narrow
  segment as small, merge it and have the writer split it again on each
  trigger interval; simulated write amplification about 70× (Section 3.3).
- **Bytes and rows as two dimensions of segment fullness inside the planner**
  (`fullness = max(size / maxSize, rows / maxRows)` in classification,
  `Knapsack`, the writer and import; the previous revision of this document).
  Superseded: with an exact row-width estimate it produces the same plan as the
  byte target in every simulated interval (Section 3.3), while touching
  `knapsack.go`, both planners, `squeezeSmallSegmentsToBuckets`,
  `MultiSegmentWriter`, import assignment, the seal limitation and a new
  `CompactionPlan.max_rows` field. Its one advantage is a hard row bound that
  survives a schema change; the byte target bounds rows through the estimate
  instead. It remains the fallback if a hard bound turns out to be needed.
- **Observed bytes per row** (`Σ MemorySize / Σ NumOfRows` over the
  collection's sealed segments) instead of the schema estimate, or the
  conservative `min(estimate, observed minimum width)` that keeps old narrow
  rows under `maxRows` after a schema change. Rejected for the first version:
  the observed value differs between DataCoord and the StreamingNode and
  across restarts, needs sealed segments to exist, and the schema estimate's
  error is already in the safe direction. The conservative variant costs
  `newWidth / oldWidth` times more segments for all post-change data.
- **One readiness predicate for handoff and compaction input.** Rejected:
  handoff wants every defined index so that the published segment is
  queryable without a brute-force fallback; compaction input only needs to
  avoid consuming a segment whose vector index is still building. Using the
  handoff rule for compaction input would delay compaction by a scalar index
  build that is discarded with the input segment.
- **No row target, rely on delete parallelisation.** Rejected for now: the
  parallelisation is a segcore change with its own review (Follow-ups); the
  target is a configuration-level guard that can be relaxed when the former
  lands.
- **Allow dropping the last vector field.** Deferred to the schema-evolution
  document.

## Follow-ups (out of scope, tracked separately)

- Delete application inside a sealed segment: `search_batch_pks`
  (`ChunkedSegmentSealedImpl.cpp`) runs one `lower_bound` per delete key per
  chunk of the sorted primary-key column and inserts hits one by one; the
  query-side `search_pks` already has a single-pass two-pointer variant
  (`search_pks_with_two_pointers_impl`, PR #45328) that the delete path does
  not use. Reuse it and apply hits per chunk range in parallel. With that,
  `maxRows` can be raised or removed.
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
- Review comments on PR #53983 (xiaofan-luan, 2026-10-10): handoff readiness,
  row-aware compaction planning, import reader validation. The row-aware
  planning comment led first to the two-dimensional fullness revision and then
  to the per-collection size target of Section 3.3.
- PR #53334 `feat: implement Import V3 reshard` (open)
- Druid segment optimization: https://druid.apache.org/docs/latest/operations/segment-optimization
- TiKV coprocessor configuration: https://tikv.org/docs/4.0/tasks/configure/coprocessor/
- Elasticsearch shard sizing: https://www.elastic.co/blog/how-many-shards-should-i-have-in-my-elasticsearch-cluster
