# Collections Without a Vector Field

- **Created:** 2026-10-07
- **Status:** Draft
- **Author(s):** @czs007
- **Related issues:** #52065, #33853, #51185
- **Related designs:** `20260708-vector-anchored-join.md` (Phase 0 needs a scalar-only collection), `20260909-datacoord-segment-change-staging-design.md` (`segmentIndexed`), `20260413-drop-collection-field-design.md`

## Summary

Milvus rejects a collection schema that has no vector field. Outside
`CreateCollection` and a few DataCoord and Proxy checks, nothing in the system
depends on a vector field: segcore, QueryNode, storage, WAL, flush and
compaction execution all work per field.

Phase 1 lets a collection be created without a vector field and keeps it that
way for its lifetime: adding a vector field or a vector-producing function to
it is rejected, as dropping the last vector field of a vector collection
already is. Whether a collection has vectors is therefore fixed at creation and
read from the schema. There is no new collection type, property or proto
field.

Such a collection supports everything except search: insert, upsert, delete,
query, get, count, iterators, scalar indexes, partition and clustering keys,
bulk import, compaction, load and release, and adding scalar fields. `search`,
`hybrid_search` and `search_iterator` return one error.

Two parts are more than removing a check:

- DataCoord's "is this segment indexed" filter is split into two predicates,
  one for handoff and one for compaction input (Section 3).
- Without a vector a 1 GB segment holds tens of millions of rows, and the
  per-segment serial paths are exposed. Delete application, the one that
  affects availability, is fixed in segcore (PR #54543, a dependency of this
  phase). Narrow-row collections additionally get a row-derived segment size
  target that bounds index build and load replay per segment (Section 4).

Phase 2, converting between zero and non-zero vector fields, is left until a
use case needs it.

## Background

#52065 asks for a scalar-only collection with search rejected and everything
else unchanged. The vector-anchored join MEP needs one as the scalar side of a
join. Users work around the restriction today with a `BINARY_VECTOR(dim=8)`
placeholder and a `BIN_FLAT` index, paying an index build per segment and
exposing a fake vector field to every tool that reads the schema.

## 1. Scope

| | Phase 1 (this document) | Later |
|---|---|---|
| Create without a vector field | yes | |
| Add a scalar field / drop a scalar field | yes | |
| Add a vector field or BM25 / embedding / MinHash function to a collection created without one | rejected | Phase 2 |
| Drop the last vector field of a vector collection | rejected (unchanged) | Phase 2 |
| Quick-setup paths (`create_collection(dimension=...)`, REST `quickCreate`) | unchanged, still create a vector collection | |
| Table type, join key, LSM store | no | join MEP |

Fixing the property at creation removes the cases a transition would raise:
load starting to require an index on an already loaded collection, the
compaction-input rule changing from "nothing to wait for" to "wait for the
vector index", the segment size target jumping and re-merging every existing
segment, and old segments with no data for the new vector field.

## 2. Predicate, gate and entry checks

```go
// HasVectorField reports whether the schema declares a vector typed field
// (top-level, struct sub-field or function output).
func HasVectorField(schema *schemapb.CollectionSchema) bool {
    return len(GetVectorFieldSchemas(schema)) > 0
}
```

Every "does this collection have vectors" decision goes through it.

`common.enableCollectionWithoutVector` (default `true` on master) gates only
`CreateCollection`. When `false` the Proxy returns the existing
`schema does not contain vector field`. Operators keep it off during a rolling
upgrade until Proxy, DataCoord and QueryCoord are upgraded: an old DataCoord
never compacts such a collection and an old Proxy rejects its load.

The code changes are small and listed in Appendix A. In short:

- **Create and load.** The vector-count checks at create time and in
  `load_fields` validation apply only when the schema has vector fields. Load
  fails only when a loaded vector field has no index, so a collection with no
  vector needs no index. QueryCoord already treats "no index" as an empty
  index list.
- **Schema evolution.** RootCoord's single schema-evolution entry
  (`prepareAlterSchemaAddField` and the function-add path) rejects any change
  that turns `HasVectorField` from false to true: a vector field, a struct
  array field with a vector sub-field, or a function with a vector output.
  The Proxy repeats the check in `addCollectionFieldTask` and
  `alterCollectionSchemaTask` for an early error, but RootCoord is
  authoritative so that no entry can bypass it. Two Proxy drop checks that
  compute `removed >= vectorCount` are always true when the count is 0 and
  block every struct-field and function drop; they get the `removed > 0`
  guard that RootCoord already has.
- **Search.** `Search`, `HybridSearch`, `SearchIterator` and search by primary
  key return `collection %s has no vector field, search is not supported`
  before `anns_field` is resolved. Today four different messages cover this
  case.
- **REST and pymilvus.** The REST insert check and the pymilvus ORM
  `check_schema` check that require a vector are removed.
- **Import.** A file that matches none of the schema's columns currently makes
  the import task loop forever, because the reader never produces a row and
  never reaches EOF. A vector field always prevented this. Readers now reject
  such a file at construction.
- **Disk segment size.** `AllDenseWithDiskIndex` returns true for zero dense
  fields, which gives sparse-only and scalar-only collections the 2 GB DiskANN
  segment target. It gets a `len(dense) > 0` guard.

## 3. Index readiness

DataCoord uses one filter, `FilterInIndexedSegments`, for two questions:

- may a compaction output replace its parents in the query view (handoff)?
- may a segment be fed into another compaction?

The filter's target set is the vector fields. It returns nothing until every
target field has an index. For a collection with no vector field this is
permanent: L1 mix compaction and delete-ratio compaction never run, and
compaction outputs never become visible.

The filter is split into two predicates, both evaluated against the indexes
defined in index meta rather than against the schema:

| Predicate | Waits for | Nothing defined |
|---|---|---|
| `handoffReady` | every index of the collection, scalar and vector | ready |
| `compactionInputReady` | vector indexes only | ready |

`compactionInputReady` keeps today's behaviour for vector collections. Scalar
indexes are not part of it, because they are rebuilt for each output segment
and waiting for them on the input side only delays compaction.

`handoffReady` is today's `dataCoord.dataview.forceAllIndexReady=true` made
the default, and the parameter is removed. **This changes vector collections
too:** a compaction output becomes visible after its scalar indexes finish,
not only its vector index. The cost is index build time per segment; for
`INVERTED` on one `int32` field it is 8.5 s at 5.5 M rows. The parent segments
keep serving until then. The upside is that a published segment never falls
back to brute-force scalar filtering. Segments below
`minSegmentNumRowsToEnableIndex` stay exempt.

Two further consequences for vector collections, which are why
`forceAllIndexReady` was opt-in, need the DataCoord owner's agreement:

- A scalar index whose build fails terminally keeps the compaction output
  invisible. Under the segment change staging design the group goes to
  `FAILED` and the parents stay published; without staging the parents are
  already retired and the rows are unavailable until the index build is
  retried. Today only a failed vector index has this effect.
- Creating a scalar index while compaction outputs are still being indexed
  delays their publication by one more build.

## 4. Segment size target

### 4.1 Problem

Segments are sized in bytes everywhere: StreamingNode seal, compaction targets,
import assignment and QueryNode admission. A vector field also caps the row
count: 1 GB holds about 350 k rows at dim 768 and 2 M rows at dim 128. Without
a vector field, 1 GB holds 44 M rows of `int64 + int32`.

Delete application is the path that breaks. The delegator does not advance a
channel's tsafe until a delete is applied to every affected segment, and
inside one sealed segment each primary key is looked up with an independent
binary search over the sorted primary-key column. Once that column no longer
fits in cache, each search is about 25 cache misses: 6–13 µs per `int64` key
and about 45 µs per `varchar` key on the two test machines. On a single
44 M-row segment, a 10 % delete stalls tsafe for 28.6 s on aarch64 and 55 s on
x86. With the same rows in eight segments of 5.5 M rows, the stall is 5.8 s
and 8.1 s. Strong-consistency queries fail once the lag passes 3 s. Query
latency and memory do not depend on segment count (Appendix B).

### 4.2 Rule

A new parameter `dataCoord.segment.maxRows` (default 4,000,000; 0 disables
it) caps the per-collection size target:

```
rowWidth(c)     = EstimateSizePerRecord(schema(c)) + 16   // + RowID and Timestamp
expectedSize(c) = min(baseSize(c), maxRows × rowWidth(c))  // baseSize = maxSize, or the DiskANN size
```

It is computed in `getExpectedSegmentSize`, which every DataCoord size consumer
already reads: both planners, force-merge, clustering, import assignment and
`CompactionPlan.max_size`. The StreamingNode seal limitation computes the same
value through a shared helper. The planner, the compaction writer and import
code are unchanged, and there is no proto change.

A collection whose estimated row is at least 268 B (`maxSize / maxRows`) keeps
its current target. That covers every float vector with dim > 61. For
`int64 + int32` the target is 112 MB, about 4 M rows.

The estimate over-states variable-length fields: a `varchar` counts as
`min(max_length, 256)` (`common.estimate.varCharLengthAvg`) and JSON, Array,
Text and Geometry count as 512 B (`common.estimate.dynamicFieldLengthAvg`).
So the target binds only for schemas of fixed-width fields and `varchar`
fields with a short declared `max_length`. A `varchar` primary key declared
with `max_length ≥ 252`, or any JSON or Array field, puts the estimate above
268 B and leaves the collection at `maxSize`, although its real rows may be
40 B wide (a 16-byte key: 26 M rows per GB). This is why the delete path
cannot rely on the target and is fixed in segcore instead (Section 4.3); the
target is kept for what it does cover and for the per-segment costs below.

The rule cannot cause repeated re-compaction: planner classification and the
writer compare bytes against the same `expectedSize(c)`, so an output that the
writer fills is "full" to the planner and is not selected again. A simulation
of both planners over 80 trigger intervals confirms this: write amplification
is 0.9–1.6 and no full output is re-merged, for row widths from 13 B to 300 B,
for estimates from 0.43× to 30× the real width, and across a schema change.
Capping rows only at the writer, without changing the planner's target, gives
a write amplification of 70.

Adding a scalar field raises `rowWidth`. Segments that were full under the old
target are merged once, the same as when an operator raises `maxSize`.

### 4.3 Delete-path fix (PR #54543, dependency of this phase)

The fix for delete application is in segcore: sort the delete batch by
primary key and merge it against the sorted primary-key column, instead of
running one binary search per key. On the 44 M-row segment a 1.5 M-key delete
drops from 18.2 s to 0.6 s (`int64`) and from 76.6 s to 1.1 s (`varchar`), a
4.4 M-key delete in 44 requests stalls tsafe for 0.25 s instead of 43.6 s
(`int64`) and 0.08 s instead of 209.8 s (`varchar`), and the load-time replay
of 6.34 M deletes drops from 381 s to 11 s (`varchar`). With it, a 44 M-row
segment has no availability problem on delete, whatever the key type, and
the schemas the size target does not cover (Section 4.2) are safe.

PR #54543 therefore ships before or with this phase. What the size target
still bounds afterwards is per-segment serial work that is latency, not
availability: scalar index builds (`INVERTED` on one `int32` field, 28.6 s on
44 M rows against 8.5 s wall for eight 5.5 M-row segments built in parallel;
`BITMAP` 48.6 s against 8.5 s) and the load-time delete replay (about 10 s
for 6 M deletes on one segment). `maxRows=0` turns the target off.

## 5. Compatibility

- No change to proto, etcd layout, binlog format or `DescribeCollection`
  output.
- Rolling upgrade: keep the gate off until the control plane is upgraded
  (Section 2). After a downgrade, such a collection can still be read and
  dropped but is not compacted.
- Older pymilvus ORM rejects the schema on the client; `MilvusClient` and the
  Java, Go and Node SDKs work. Restore or CDC into an older cluster fails at
  `CreateCollection` with the old error.
- Collections with an estimated row below 268 B seal, compact and import
  smaller segments after upgrade. With only an `int64` primary key next to
  the vector, that is `FLOAT_VECTOR` dim ≤ 61, `FLOAT16` / `BFLOAT16`
  dim ≤ 122, `INT8_VECTOR` dim ≤ 244 and `BINARY_VECTOR` dim ≤ 1952; any
  further field narrows the range.
  Existing segments are not re-split. `maxRows=0` restores the old behaviour.
- Sparse-only collections get a 1 GB compaction and import target instead of
  2 GB (the `AllDenseWithDiskIndex` fix).
- Handoff waits for scalar indexes on every collection (Section 3).
- `dataCoord.dataview.forceAllIndexReady` is removed; a config that sets it
  logs a deprecation warning.

## 6. Testing

- Unit tests:
  - the predicate;
  - each touchpoint in Appendix A, with and without vector fields;
  - the two readiness predicates with no index, a scalar index only, and a
    vector index building or finished;
  - `getExpectedSegmentSize` for narrow, wide and DiskANN schemas and for
    `maxRows=0`;
  - the planners producing no merge plan on outputs at `expectedSize`.
- E2E tests in `tests/python_client/milvus_client/`:
  - create without a vector, load without any index, then run query, get,
    count, iterator, delete and upsert;
  - with scalar indexes, partition key and clustering key;
  - bulk import followed by compaction;
  - search returns the unified error;
  - `add_field` with a vector is rejected and `add_field` with a scalar
    succeeds;
  - gate off;
  - REST v2;
  - an import of 12 M narrow rows produces at least 3 segments, whose IDs stay
    stable across two trigger intervals.

## 7. Alternatives

- **An explicit table type or proto flag.** Rejected. Every client and tool
  would have to learn it, and with Phase 1's rule the schema already
  determines the property.
- **Bytes and rows as two fullness dimensions inside the planner.** Rejected.
  It produces the same plans as the size target in every simulated interval
  while changing knapsack, both planners, the writer, import and the proto.
  It remains the fallback if a hard row bound is ever needed.
- **No row target at all, since PR #54543 removes the availability problem.**
  Not taken: scalar index build and load replay remain serial per segment,
  and the target costs no code outside `getExpectedSegmentSize` and the seal
  limitation. It can be disabled with `maxRows=0`.

## 8. Follow-ups

- Phase 2: adding a vector field to, or dropping the last vector field from,
  an existing collection.
- The loader memory estimate is about 3× low for narrow fixed-width rows
  (41 B per row estimated against 120 B per row of RSS on `int64 + int32`).
  It needs a per-row constant for fixed-width columns.
- Remove the dead row-based seal policies (`calBySchemaPolicy` and the
  `AssignSegmentID` path, which has no caller).

## Appendix A. Touchpoints

File and line numbers are from master `6140f5f55ca`, except the schema
evolution rows, which are from `3df0cbcdd68`.

| Site | Today | Change |
|---|---|---|
| `proxy/task.go:432` `createCollectionTask.PreExecute` | no vector → reject | reject only when the gate is off |
| `proxy/util.go:1048` `validateLoadFieldsList`, `metacache/meta_cache.go:298` | at least one vector must be loaded | only if the schema has vectors |
| `proxy/task.go:3141`, `:3410` load collection / partitions | `ErrIndexNotFound` fails the load | fail only if a loaded vector field has no index |
| `httpserver/utils.go:3061` `anyToColumns` | REST insert needs a vector or function | check removed |
| RootCoord `prepareAlterSchemaAddField` and the function-add path; Proxy `task.go:644` `addCollectionFieldTask`, `:1033` `alterCollectionSchemaTask` | a vector field, struct vector sub-field or vector-output function may be added | rejected when `HasVectorField` would turn from false to true (RootCoord authoritative, Proxy early check) |
| `proxy/task.go:1297`, `:1337` drop struct field / function | `removed >= count` is true at 0 | `removed > 0 &&` |
| `datacoord/util.go:77` `FilterInIndexedSegments` | waits forever with no vector | split into `handoffReady` / `compactionInputReady` |
| `datacoord/index_meta.go:1497` `AllDenseWithDiskIndex` | true at 0 dense fields | `len(dense) > 0 &&` |
| `datanode/importv2` Parquet / NumPy / CSV / JSON readers | file with no schema column loops | rejected at construction |
| `compaction_trigger_v2.go:872` `getExpectedSegmentSize`, `segment_limitation_policy.go`, `task_analyze.go:208` | global `maxSize` | `expectedSize(c)` |
| Search entries, REST handlers | four different errors | one error |
| pymilvus `orm/schema.py:1330` `check_schema` | client-side reject | removed |

## Appendix B. Measurements

Measured on a single-node standalone with autocompaction off and one shard,
on aarch64 (20 cores, master `036bef9e9f`) and on an x86 AMD Ryzen 9 7945HX
(master `179225cd09`). Schema `int64` pk + `int32` (+ the 1 B placeholder
vector of today's workaround), 44 M rows.

Delete and index build, one 44 M-row segment against eight 5.5 M-row
segments (aarch64):

| Operation | 1 × 44 M | 8 × 5.5 M |
|---|---:|---:|
| Delete 1 % by expression, tsafe stall | 3.8 s | 0.7 s |
| Delete 10 % by expression, tsafe stall | 28.6 s | 5.8 s |
| Delete 10 % by pk list, 44 requests of 100 k, stall | 24 s | 0.15 s |
| Same with `varchar` pk, stall | 186 s | 14.5 s |
| `INVERTED` build on `int32` | 28.6 s | 8.5 s (parallel) |
| Load RSS delta | +5.3 GB | +5.3 GB |
| Point / filter / count query | 2–4 ms | 2–4 ms |

On x86 the same deletes cost 11–12.5 µs per `int64` key. A CPU profile of a
1.5 M-key delete attributes 81–85 % of `ChunkedSegmentSealedImpl::Delete` to
`std::lower_bound` in the primary-key search. The skip-list insert of the
delete records accounts for under 2 %. With a `varchar` key, 95 % is in
`StringChunk::binary_search_string`.

Loader estimate against RSS: `int64 + int32` 29 B per row `MemorySize`, 41 B
estimated, 120 B RSS; `int64 + int32 + varchar(64) + JSON(115)` 217 B, 423 B
and 265–422 B.

Streaming insert of the narrow schema seals at 4.85–4.9 M rows (137 MB)
against a 123 MB seal threshold. The wire size per row (26 B) is lower than `MemorySize`
(29.6 B), the same ratio that applies to `maxSize` today.

## References

- Review comments on PR #53983 (xiaofan-luan, 2026-10-10) and review feedback from Li Liu (2026-10-21, phase split)
- PR #54543 `enhance: merge sorted delete batches against the sorted primary-key column`
- Druid `maxRowsPerSegment` (5 M), TiKV region (96 MB or 960 k keys), Elasticsearch shard guidance (≤ 200 M documents): comparable systems cap both bytes and rows per unit.
