# Collections Without a Vector Field

- **Created:** 2026-10-07
- **Status:** Draft
- **Author(s):** @czs007
- **Related issues:** #52065, #33853, #51185
- **Related designs:** `20260708-vector-anchored-join.md` (Phase 0 needs a scalar-only collection), `20260909-datacoord-segment-change-staging-design.md` (`segmentIndexed`), `20260413-drop-collection-field-design.md`

## Summary

Today a collection must have at least one vector field. The check lives in
`CreateCollection` and a handful of Proxy and DataCoord places. Nothing below
that depends on a vector field: segcore, QueryNode, storage, WAL, flush and
compaction all work field by field.

Phase 1 removes the check at creation. A collection created without a vector
field stays that way: adding a vector field or a vector-producing function to
it is rejected, just as dropping the last vector field is rejected today. So
"does this collection have vectors" is decided once, at creation, and read
from the schema. There is no new collection type, property or proto field.

Such a collection can do everything except search: insert, upsert, delete,
query, get, count, iterators, scalar indexes, partition and clustering keys,
bulk import, compaction, load, release, and adding scalar fields. `search`,
`hybrid_search` and `search_iterator` return one error.

Most of the change is removing checks. Two things are more than that:

- DataCoord asks "is this segment indexed" in two places with one filter,
  and with no vector field that filter waits forever. It is split into two
  predicates (Section 3).
- Without a vector, a 1 GB segment holds tens of millions of rows. Three
  things are done per segment on one thread and get slow at that size:
  applying deletes, building scalar indexes, and replaying deletes at load.
  Applying deletes is the one that blocks reads. It is fixed in segcore by
  PR #54543, which this phase depends on. For the other two, narrow-row
  collections get a smaller segment size target (Section 4).

Phase 2, converting a collection between zero and non-zero vector fields,
waits for a use case.

## Background

#52065 asks for a scalar-only collection with search rejected and everything
else unchanged. The vector-anchored join MEP needs one as the scalar side of a
join. Users work around the restriction with a `BINARY_VECTOR(dim=8)`
placeholder and a `BIN_FLAT` index. They pay an index build per segment, and
every tool that reads the schema sees a fake vector field.

## 1. Scope

| | Phase 1 (this document) | Later |
|---|---|---|
| Create without a vector field | yes | |
| Add a scalar field / drop a scalar field | yes | |
| Add a vector field or BM25 / embedding / MinHash function to a collection created without one | rejected | Phase 2 |
| Drop the last vector field of a vector collection | rejected (unchanged) | Phase 2 |
| Quick-setup paths (`create_collection(dimension=...)`, REST `quickCreate`) | unchanged, still create a vector collection | |
| Table type, join key, LSM store | no | join MEP |

Deciding the property at creation avoids four problems that a transition
would bring:

- A collection that is already loaded would suddenly need an index.
- The compaction-input rule would switch from "nothing to wait for" to "wait
  for the vector index".
- The segment size target would jump, and every existing segment would be
  merged again.
- Old segments would have no data for the new vector field.

Phase 2 can take these on when something needs it.

## 2. What changes

One predicate answers "does this collection have vectors":

```go
// HasVectorField reports whether the schema declares a vector typed field
// (top-level, struct sub-field or function output).
func HasVectorField(schema *schemapb.CollectionSchema) bool {
    return len(GetVectorFieldSchemas(schema)) > 0
}
```

One config, `common.enableCollectionWithoutVector` (default `true` on
master), gates `CreateCollection` only. With it off, the Proxy returns the
existing `schema does not contain vector field`. Operators keep it off during
a rolling upgrade until Proxy, DataCoord and QueryCoord are all new. An old
DataCoord would never compact such a collection, and an old Proxy would reject
its load.

With the predicate in place, the behaviour changes are:

- **Create** accepts a schema with no vector field when the gate is on.
- **Load** no longer requires an index. The Proxy fails a load only when a
  loaded vector field has no index, and the QueryNode accepts a channel watch
  or segment load with an empty index list when the schema has no vector
  field, so such a collection loads with no index at all.
- **Adding a vector** to such a collection is rejected. RootCoord's schema
  evolution entry makes the decision, for a vector field, a struct field with
  a vector sub-field, and a function with a vector output alike. The Proxy
  repeats the check so the user gets the error early.
- **Search** returns `collection %s has no vector field, search is not
  supported` on every search entry. Today four different messages cover this
  case.

Everything else is a check that assumed a vector exists and is wrong at zero:
two Proxy drop checks, a REST insert check, a pymilvus check, the DiskANN
segment size rule, and an import reader that loops forever on a file with no
matching column. Appendix A lists each with its file and line.

## 3. Index readiness

DataCoord uses one filter, `FilterInIndexedSegments`, to answer two questions:

- May a compaction output replace its parents in the query view (handoff)?
- May a segment be fed into another compaction?

The filter looks at the vector fields and returns nothing until every one of
them has an index. With no vector field it never returns anything. L1 mix
compaction and delete-ratio compaction never run, and compaction outputs
never become visible.

The filter is split into two predicates. Both look at the indexes defined in
index meta, not at the schema:

| Predicate | Waits for | Nothing defined |
|---|---|---|
| `handoffReady` | every index of the collection, scalar and vector | ready |
| `compactionInputReady` | vector indexes only | ready |

`compactionInputReady` keeps today's behaviour for vector collections. It does
not wait for scalar indexes, because each output segment rebuilds them anyway
and waiting on the input side only delays the compaction.

`handoffReady` is what `dataCoord.dataview.forceAllIndexReady=true` does
today. It becomes the only behaviour and the parameter goes away. **This
changes vector collections too.** A compaction output becomes visible after
its scalar indexes finish, not just its vector index. The cost is one scalar
index build per segment before publication, 8.5 s for `INVERTED` on one
`int32` field at 5.5 M rows. The parents keep serving in the meantime. The
gain is that a published segment never falls back to brute-force scalar
filtering. Segments below `minSegmentNumRowsToEnableIndex` are exempt as
before.

This split follows xiaofan-luan's review of 2026-10-10, which asked for
the impact on publication delay, small segments and index failures to be
evaluated. The delay and the small-segment exemption are covered above.
Two consequences for vector collections remain, and they are the reason
`forceAllIndexReady` was opt-in. They are listed here for that review to
confirm the trade-off:

- A scalar index whose build fails for good keeps the compaction output
  invisible. With the segment change staging design, the group goes to
  `FAILED` and the parents stay published. Without staging, the parents are
  already gone and the rows are unavailable until the build is retried.
  Today only a failed vector index does this.
- Creating a scalar index while compaction outputs are still being indexed
  delays their publication by one more build.

## 4. Segment size

### 4.1 What a segment looks like without a vector

Segments are sized in bytes everywhere: StreamingNode seal, compaction
targets, import assignment and QueryNode admission. A vector field also caps
the row count as a side effect. 1 GB holds about 350 k rows at dim 768 and
2 M rows at dim 128. Without a vector field, 1 GB of `int64 + int32` is 44 M
rows.

Three things run per segment on a single thread, and at 44 M rows they are
slow. Measured on one 44 M-row segment against eight 5.5 M-row segments with
the same data (aarch64, details in Appendix B):

| Per-segment work | 1 × 44 M | 8 × 5.5 M |
|---|---:|---:|
| Apply a 10 % delete by expression (tsafe stall) | 28.6 s | 5.8 s |
| Apply a 10 % delete by `varchar` pk list, 44 requests (tsafe stall) | 186 s | 14.5 s |
| Build `INVERTED` on `int32` | 28.6 s | 8.5 s (parallel) |

The third cost, replaying deletes when the segment is loaded, was measured
on x86 only: loading the 44 M-row segment with 6.34 M deleted keys takes
93 s with an `int64` key and 381 s with a `varchar` key.

Query latency and memory do not depend on segment count (Appendix B).

The delete stall is the one that matters. The delegator does not advance a
channel's tsafe until a delete is applied to every affected segment, and a
strong-consistency query fails once the lag passes 3 s. Inside a sealed
segment each primary key was looked up with its own binary search over the
sorted primary-key column. Once that column is bigger than the cache, each
lookup is about 25 cache misses: 6–13 µs per `int64` key and about 45 µs per
`varchar` key.

### 4.2 Deletes: fixed in segcore (PR #54543, a dependency)

PR #54543 sorts the delete batch by primary key and merges it against the
sorted primary-key column in one pass, instead of one binary search per key.
On the 44 M-row segment (x86):

| Delete | Before | After |
|---|---:|---:|
| 1.5 M `int64` keys, apply time | 18.2 s | 0.6 s |
| 1.5 M `varchar` keys, apply time | 76.6 s | 1.1 s |
| 4.4 M `int64` keys in 44 requests, tsafe stall | 43.6 s | 0.25 s |
| 4.4 M `varchar` keys in 44 requests, tsafe stall | 209.8 s | 0.08 s |
| Load with 6.34 M `int64` deletes to replay | 93 s | 10 s |
| Load with 6.34 M `varchar` deletes to replay | 381 s | 11 s |

With this, a 44 M-row segment has no availability problem on delete, for any
key type. PR #54543 ships before or with this phase.

### 4.3 A row-derived size target for the rest

What remains per segment after that fix is latency, not availability: the
scalar index build (28.6 s on 44 M rows against 8.5 s for eight segments
built in parallel) and the load-time replay (about 10 s for 6.34 M deletes
on one segment). For these, a new parameter
`dataCoord.segment.maxRows` (default 4,000,000; 0 disables it) caps the
per-collection size target:

```
rowWidth(c)     = EstimateSizePerRecord(schema(c)) + 16   // + RowID and Timestamp
expectedSize(c) = min(baseSize(c), maxRows × rowWidth(c))  // baseSize = maxSize, or the DiskANN size
```

It is computed in `getExpectedSegmentSize`, which every DataCoord size
consumer already reads: both planners, force-merge, clustering, import
assignment and `CompactionPlan.max_size`. The StreamingNode seal limitation
computes the same value through a shared helper. The planner, the compaction
writer and the import code do not change. There is no proto change.

A collection whose estimated row is at least 268 B (`maxSize / maxRows`)
keeps its current target. That covers every float vector with dim > 61. For
`int64 + int32` the target is 112 MB.

**`maxRows` is a byte target, not a row count.** The parameter sets the
target through the estimated row width, and each path then measures bytes in
its own unit: import and compaction count binlog bytes, the StreamingNode
seal counts the bytes of the insert messages. For `int64 + int32` a binlog
row is about 10.6 B and an insert row 12 B, against the 28 B estimate, so a
112 MB target gives import segments of 11–13 M rows, compaction outputs of
up to 10 M rows and growing segments that seal at about 1 M rows (measured in
Appendix B). The rows per segment are therefore
`maxRows × rowWidth(c) / (bytes per row in that path)`, 2.5–3× `maxRows`
for this schema. This is accepted: the per-segment costs the target bounds
were measured at these sizes and are within budget, and the same byte rule
on both sides is what keeps compaction from oscillating (next paragraph).
A row count enforced as such would need the planner's fullness test, the
writer's split and import assignment to count rows as well (Section 7).

**What the target covers.** The estimate rounds variable-length fields up: a
`varchar` counts as `min(max_length, 256)` (`common.estimate.varCharLengthAvg`),
and JSON, Array, Text and Geometry count as 512 B
(`common.estimate.dynamicFieldLengthAvg`). So the target only takes effect
for schemas made of fixed-width fields and `varchar` fields with a short
declared `max_length`. A `varchar` primary key declared with
`max_length ≥ 252`, or any JSON or Array field, pushes the estimate over
268 B and leaves the collection at `maxSize`, even if its real rows are 40 B
wide (a 16-byte key, 26 M rows per GB). This is why the delete path is fixed
in segcore and not by the target. The target is kept for the schemas it does
cover and for the two costs above, and `maxRows=0` turns it off.

**It does not cause repeated re-compaction.** The planner's classification
and the writer compare bytes against the same `expectedSize(c)`, so an output
the writer fills is "full" to the planner and is not picked again. A
simulation of both planners over 80 trigger intervals confirms it: write
amplification is 0.9–1.6 and no full output is merged again, for row widths
from 13 B to 300 B, for estimates from 0.43× to 30× the real width, and
across a schema change. Capping rows only at the writer, without changing the
planner's target, gives a write amplification of 70.

Adding a scalar field raises `rowWidth`. Segments that were full under the
old target are merged once, the same as when an operator raises `maxSize`.

## 5. Compatibility

- No change to proto, etcd layout, binlog format or `DescribeCollection`
  output.
- Rolling upgrade: keep the gate off until the control plane is upgraded
  (Section 2). After a downgrade, such a collection can still be read and
  dropped, but it is not compacted.
- Older pymilvus ORM rejects the schema on the client. `MilvusClient` and the
  Java, Go and Node SDKs work. Restore or CDC into an older cluster fails at
  `CreateCollection` with the old error.
- Collections with an estimated row below 268 B seal, compact and import
  smaller segments after the upgrade. With only an `int64` primary key next
  to the vector, that is `FLOAT_VECTOR` dim ≤ 61, `FLOAT16` / `BFLOAT16`
  dim ≤ 122, `INT8_VECTOR` dim ≤ 244 and `BINARY_VECTOR` dim ≤ 1952. Any
  further field narrows the range. Existing segments are not re-split.
  `maxRows=0` restores the old behaviour.
- Sparse-only collections get a 1 GB compaction and import target instead of
  2 GB (the `AllDenseWithDiskIndex` fix in Appendix A).
- Handoff waits for scalar indexes on every collection (Section 3).
- `dataCoord.dataview.forceAllIndexReady` is removed. A config that sets it
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

- **An explicit table type or proto flag.** Every client and tool would have
  to learn it, and with the Phase 1 rule the schema already carries the
  property. Not needed.
- **Bytes and rows as two fullness dimensions inside the planner.** It
  produces the same plans as the size target in every simulated interval,
  and it touches knapsack, both planners, the writer, import and the proto.
  It stays as the fallback if a hard row bound is ever needed. It would also
  be safe against oscillation, since planner and writer would share the row
  rule; the oscillation (write amplification 70) only appears when the writer
  splits by rows while the planner still judges fullness by bytes.
- **No row target, since PR #54543 removes the availability problem.** Index
  build and load replay are still serial per segment, and the target costs
  nothing outside `getExpectedSegmentSize` and the seal limitation. It can be
  turned off with `maxRows=0`. Kept.

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
| `proxy/task.go:3141`, `:3410` load collection / partitions | `ErrIndexNotFound` fails the load | fail only if a loaded vector field has no index; QueryCoord already treats "no index" as an empty index list |
| `querynodev2/services.go:206` `WatchDmChannels`, `:497` `LoadSegments` | an empty `IndexInfoList` returns `ErrIndexNotFound`, so the channel watch retries forever and the load times out after `loadTimeoutSeconds` | the check applies only when the schema has vector fields |
| `httpserver/utils.go:3061` `anyToColumns` | REST insert needs a vector or function | check removed |
| RootCoord `prepareAlterSchemaAddField` and the function-add path; Proxy `task.go:644` `addCollectionFieldTask`, `:1033` `alterCollectionSchemaTask` | a vector field, struct vector sub-field or vector-output function may be added | rejected when `HasVectorField` would turn from false to true (RootCoord decides, Proxy checks early) |
| `proxy/task.go:1297`, `:1337` drop struct field / function | `removed >= vectorCount` blocks the drop; always true at 0, so every struct-field and function drop is blocked | `removed > 0 &&`, as RootCoord already does |
| `datacoord/util.go:77` `FilterInIndexedSegments` | waits forever with no vector | split into `handoffReady` / `compactionInputReady` |
| `datacoord/index_meta.go:1497` `AllDenseWithDiskIndex` | true at 0 dense fields, so sparse-only and scalar-only collections get the 2 GB DiskANN target | `len(dense) > 0 &&` |
| `datanode/importv2` Parquet / NumPy / CSV / JSON readers | a file that matches no schema column makes the task loop forever: the reader produces no row and never reaches EOF; a vector field used to prevent this | rejected when the reader is constructed |
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
against a 123 MB seal threshold. The wire size per row (26 B) is lower than
`MemorySize` (29.6 B), the same ratio that applies to `maxSize` today.

**Phase-1 stack, 2026-10-23 (aarch64, same machine).** Branch
`nonvec-phase1-stack` on master `f0f7677664`: the four phase-1 changes plus
PR #54543. Collection `int64` pk + `int32`, no vector field, 44 M rows
imported from one parquet file, auto compaction on, `maxRows` 4 M.

| Step | Result |
|---|---:|
| Import (48 s), segments after sort compaction | 5 segments: 12.59 M, 11.82 M, 11.19 M, 4.20 M, 4.20 M rows; binlog 10.6 B per row |
| `INVERTED` on `int32`, 44 M rows | 10.6 s (28.6 s on one 44 M segment, 8.5 s on eight) |
| Load with no index | 3.5 s, RSS +3.6 GB (+5.3 GB before) |
| Delete 1 % by expression, tsafe stall | 0.38 s (3.8 s before) |
| Delete 10 % by expression, tsafe stall | 1.62 s (28.6 s before) |
| Delete 10 % by pk list, 44 requests of 100 k, stall | 0.07 s (24 s before) |
| Delete-ratio compaction after 20 % deleted | ran within 2 min, outputs visible, 8 segments, largest 7.19 M rows |
| Release and load again, 9.24 M deletes replayed | 4.9 s |
| Point / filter / count query | 1–4 ms throughout |
| `search`, `hybrid_search`, add vector field | rejected with the unified error; adding a scalar field succeeds |

Growing segments of the same schema seal at 0.5–1.0 M rows (112 MB × 0.12
seal proportion, counted in 12 B insert rows); on a 6 M-row insert the
compaction outputs stayed at or below 2.8 M rows.

## References

- Review comments on PR #53983 (xiaofan-luan, 2026-10-10) and review feedback from Li Liu (2026-10-21, phase split)
- PR #54543 `enhance: merge sorted delete batches against the sorted primary-key column`
- Druid `maxRowsPerSegment` (5 M), TiKV region (96 MB or 960 k keys), Elasticsearch shard guidance (≤ 200 M documents): comparable systems cap both bytes and rows per unit.
