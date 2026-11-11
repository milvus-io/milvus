# Collections Without a Vector Field

- **Created:** 2026-10-07
- **Status:** Draft
- **Author(s):** @czs007
- **Related issues:** #52065, #33853, #51185
- **Related designs:** `20260708-vector-anchored-join.md` (its Phase 0 needs a scalar-only collection), `20260909-datacoord-segment-change-staging-design.md` (`segmentIndexed`), `20260413-drop-collection-field-design.md`

Implementation: Draft PR #54604. In this document `M` means million (10^6)
rows and `k` thousand rows. The Summary and Sections 1 to 5 are written for
product and engineering readers; Appendices A to C are the list of
touchpoints, the measurements and the verification record.

## Summary

Today a collection must have at least one vector field. This document lifts
the restriction: a collection can be created without a vector field, and
apart from vector search it behaves like any other collection. Section 2
describes what the user sees. Two internal rules have to change for it: index
readiness (Section 3) and segment size (Section 4). Both changes apply to
collections without a vector field only: index readiness and the segment size
rules of collections with vectors do not change (Section 5).

## Background

#52065 asks for a scalar-only collection with search rejected and everything
else unchanged. The vector-anchored join MEP (Milvus Enhancement Proposal)
needs one as the scalar side of a join. Users work around the restriction
with a `BINARY_VECTOR(dim=8)` placeholder and a `BIN_FLAT` index. They pay an
index build per segment, and every tool that reads the schema sees a fake
vector field.

## 1. Scope

| Operation | Behaviour |
|---|---|
| Create without a vector field | supported |
| Add a scalar field / drop a scalar field | supported |
| Add a vector field or a BM25 / embedding / MinHash function to a collection created without one | rejected |
| Drop the last vector field of a vector collection | rejected (unchanged) |
| Quick-setup paths (`create_collection(dimension=...)`, REST `quickCreate`) | unchanged, still create a vector collection |

Whether a collection has vectors is decided at creation and cannot be
converted afterwards. So the rules for load, compaction and the segment row
cap do not change during the life of a collection.

## 2. What changes

**Behaviour**

- **Create** accepts a schema with no vector field (a config can turn this
  off, see below). Such a collection supports insert, upsert, delete, query,
  get, count, iterators, scalar indexes, partition key and clustering key,
  bulk import, compaction, load, release, and adding scalar fields.
- **Load**: a collection without a vector field loads with no index at all.
  A scalar field needs no index to be loaded today, and a field without one
  is filtered by brute force at query time; load only requires an index on
  the vector fields being loaded, and that rule stays. A collection without
  a vector field has no index it must build. The QueryNode rejects an empty
  index list today; the check now applies only when the schema has a vector
  field.
- **Adding a vector** to such a collection is rejected. RootCoord's schema
  evolution entry makes the decision, for a vector field, a struct field
  with a vector sub-field, and a function with a vector output alike. The
  Proxy repeats the check so the user gets the error early.
- **Search**: `search`, `hybrid_search`, `search_iterator` and their REST
  counterparts return one error, `collection %s has no vector field, search
  is not supported`.

**Config**

`common.enableCollectionWithoutVector` controls whether a collection without
a vector field may be created; it is on by default. RootCoord enforces it in
the create flow (`validateSchema`); the Proxy repeats the check so the
client gets the existing `schema does not contain vector field` error before
the request leaves it. No path other than creation reads the switch, so
turning it off does not affect collections that already exist: they keep
loading, serving and compacting (verified in Appendix B.4).

The switch is a permanent operator switch with two uses. It lets a
deployment disallow such collections as a matter of policy, and it stops new
ones from being created if the feature misbehaves in production, without a
rollback and without touching the collections that already exist. It is not
an upgrade mechanism: left at its default it gives no protection during a
rolling upgrade. A collection without a vector field that is created while
an old DataCoord or Proxy is still running is only not compacted and not
loadable until every role is upgraded, after which it works. Turning the
switch off before the upgrade avoids even that, but it is optional.

## 3. Index readiness

### 3.1 Two questions

DataCoord has one filter, `FilterInIndexedSegments`: given a set of
segments, it returns those that are "index ready", meaning the indexes on
some set of fields are built on the segment. It has six call sites, which
ask two different questions:

| Question | Call sites | When a segment is judged not ready |
|---|---|---|
| **Handoff**: may the output of a compaction replace its parents and become visible to queries | the channel view QueryCoord pulls, the serving frontier behind `DescribeIndex`, GC of compacted parents | the output stays invisible, queries keep reading the parents, the parents are not collected |
| **Compaction input**: may a segment be selected as the input of the next compaction | the compaction trigger, the single segment policy, the target reconciler, all when `indexBasedCompaction=true` (the default) | the segment does not take part in compaction for now |

### 3.2 Today's rule, and why it fails without a vector field

Today both questions share one rule: the fields to wait for are all the
vector fields of the schema, and a segment is ready when the indexes on all
of them are built. The config `dataCoord.dataview.forceAllIndexReady` is off
by default; when on, the fields to wait for also include every field of the
collection that has an index defined.

Without a vector field the list of fields to wait for is empty. The code
does not define a result for an empty list; it passes the list on to the
index metadata lookup, and what comes back depends on unrelated details:

- With no index defined on the collection, no segment is ready. Compaction
  outputs never become visible, parents are never collected, and mix and
  delete-ratio compaction never have an eligible input.
- With a scalar index defined, every segment that has had an index task
  issued counts as ready, whether the index is finished or not.

Neither result is a rule anybody designed.

DataCoord has a second handoff path: segment change staging (#54198, merged
2026-10-23). The output of a compaction is first registered as a STAGED
change group, and `PrepareReadiness` decides readiness on the write side
before the group is published in one step. Its index clause already defines
a rule for collections without a vector field: wait for every vector index;
when the collection has no vector field, wait for every field that has an
index defined instead; with `forceAllIndexReady` on, wait for all of them for
any collection. The infrastructure of this path is in master, but no
production code creates a change group yet, so handoff is still decided by
the read-time filter above.

### 3.3 The new rule

The filter is split into two predicates, one per question of 3.1. Each
defines which fields it waits for, and both state explicitly that nothing to
wait for means "no waiting": every flushed segment is returned.

| Predicate | Where the fields to wait for come from | Vector collection | Collection without a vector field |
|---|---|---|---|
| `handoffReady` | the vector fields of the schema; when there is none, every field of the collection that has an index defined, from the index metadata | all vector indexes (today's rule); with `forceAllIndexReady` on, all scalar indexes as well (today's rule) | all scalar indexes that are defined; ready immediately when none is |
| `compactionInputReady` | the vector fields of the schema | the indexes of all vector fields, not the scalar indexes (today's rule) | nothing to wait for, ready immediately |

The rule of `handoffReady` is the one the staging path already defines at the
end of 3.2; this document brings it to the read-time path. The set of fields
to wait for is computed by one shared function, `HandoffIndexTargetFields`,
which both the read-time filter and the staging `PrepareReadiness` call, so
the two paths give the same answer for the same segment.
`compactionInputReady` looks at vector fields only. A collection without a
vector field has no vector index, so its compaction inputs do not wait.
Segments below `minSegmentNumRowsToEnableIndex` stay exempt in both
predicates, as today.

### 3.4 What it means for a vector collection

**`compactionInputReady` does not change.** A freshly flushed segment has to
wait for its vector index before it can be merged. A vector index takes
minutes to build, and without one search is brute force. If a new segment
were merged as soon as it is flushed, the output would have to build the
vector index again, and the data would spend longer without a vector index;
under continuous writes some segments might never get their index. Building
first and merging afterwards is what lets the served data converge to an
indexed state.

**`handoffReady` does not change.** It waits for the vector indexes by
default and for every defined index when `forceAllIndexReady` is on, exactly
as today. A review suggested that handoff wait for scalar indexes by default
too; that is the same as changing the default of `forceAllIndexReady` to
true and needs no code change. This document does not change the default,
for the reasons in Section 7; whether to flip it is an open item in
Section 8.

So index readiness has no behaviour change for a vector collection.

### 3.5 What it means for a collection without a vector field

| | No index defined | Scalar indexes defined |
|---|---|---|
| `handoffReady` | The output is visible as soon as it is flushed. There is no index to wait for, and queries are brute force anyway. | The output becomes visible when these scalar indexes are built on it; the parents keep serving meanwhile. |
| `compactionInputReady` | Any flushed segment can be an input | The same; scalar indexes are not waited for |

**Why handoff waits for scalar indexes.** If it waited for vector indexes
only, like today's default for vector collections, a collection without a
vector field would wait for nothing and a compaction output would replace
its parents as soon as it is flushed. At that moment the parents have their
scalar indexes and the new segment does not, so queries fall back from the
index to brute force until the new index is built. The scalar indexes that
are defined are the only thing such a collection can wait for.

**The cost** mirrors what a vector collection pays for its vector index.
Publication is later than the flush by one scalar index build (about 8.5 s
for `INVERTED` on `int32` over 5.5 M rows). A scalar index created while the
output is building its indexes delays publication by one more build. If a
scalar index fails permanently on the output, the output stays invisible and
the parents keep serving. On the staging path that last case is a change
group in `FAILED` with the parents still published; on the read-time path
the parents are marked dropped but are not collected, and keep serving. A
vector collection behaves this way today when a vector index fails.

**Why compaction input does not wait for scalar indexes.**

- **No benefit to queries.** When an input is merged its indexes are
  deleted with it, and the output rebuilds every scalar index from scratch.
  `handoffReady` already guarantees that queries read the parents until the
  output's indexes are built, so waiting once more on the input side changes
  nothing a query sees.
- **No wasted builds.** When an input is compacted away while its index is
  half built, DataCoord aborts that index task. The extra cost is the half
  build.
- **Indexes cannot fall behind.** A scalar index builds in seconds. Once a
  compaction output reaches the byte target or the row cap (Section 4) it is
  full and is not merged again, unless there are deletes or TTL to clean up.
  So the situation of a vector collection, where data keeps being merged and
  the index never catches up, does not arise.
- **Waiting would block compaction.** If inputs waited for scalar indexes
  too, one permanently failing scalar index would keep that segment out of
  compaction for good, delete-ratio compaction included, and deleted rows
  would never be reclaimed. Creating a scalar index on a collection that
  already holds a lot of data would stop compaction of the whole collection
  until every segment has built it.
- **Consistent with vector collections.** A vector collection does not wait
  for scalar indexes on the input side today either; `forceAllIndexReady`
  affects handoff only.

### 3.6 Taken together

| | Vector collection | Collection without a vector field |
|---|---|---|
| When a segment can be merged | after its vector index is built | as soon as it is flushed |
| When the merged result becomes visible | after its vector index is built (unchanged); after all indexes are built with `forceAllIndexReady` on (unchanged) | after its scalar indexes are built; immediately when none is defined |

## 4. Segment size

### 4.1 What a segment looks like without a vector

**Why a segment gets large without a vector**

Milvus sizes segments in bytes everywhere: StreamingNode seal, compaction
target, import allocation and QueryNode admission, with a default target of
1 GB. With a vector field the vector is most of a row, and the byte limit
bounds the row count as a side effect: 1 GB is about 350 k rows at dim 768
and 2 M rows at dim 128. Without one, a row of `int64 + int32` is a dozen
bytes and 1 GB is 44 M rows.

**What slows down with the row count**

Two operations run per segment on one thread, and their time grows with the
rows of that segment. The same 44 M rows in one segment against eight
(Appendix B.1):

- **Building a scalar index.** `INVERTED` on `int32` takes 28.6 s on one
  44 M-row segment and 8.6 s on eight 5.5 M-row segments built in parallel;
  on the same field `STL_SORT` is 6.5 s against 2.5 s and `BITMAP` (1 M
  distinct values) 48.6 s against 8.5 s (measured 2026-10-07 on the same
  machine). The sort compaction after an import is serial per segment in the
  same way: 20.3 s for one 44 M-row segment, 2.6–3.4 s for each 5.5 M-row
  segment with the eight in parallel.
- **Replaying deletes when a segment is loaded.** After a release and load,
  earlier deletes are applied again on every segment. Replaying 9.24 M
  `int64` deletes takes 10.8 s on one 44 M-row segment and 5.5 s on eight
  5.5 M-row segments; 9.24 M `varchar` deletes take 4.9 s on two segments
  (24.85 M and 19.15 M rows) and 3.5 s on eight (Appendix B.3).

Both are latency problems, not availability problems: the parents keep
serving while an index builds, and a slow load still completes.

**What does not change with the row count**

Applying a delete to a sealed segment is one merge against the sorted
primary key column (PR #54543), and its cost does not grow with the segment
size: on the same 44 M rows, the tsafe stall of a 10 % delete by expression
is 1.74 s on one segment against 1.69 s on eight, and 44 batches of 100 k
primary keys take 0.10 s against 0.11 s. This is the only per-segment cost
that must stay small, because the delegator does not advance the tsafe of
the channel until the delete is applied to every affected segment, and a
strong-consistency query fails once the lag exceeds 3 s.

With no deletes, query latency and memory do not change with the number of
segments. With 9.24 M deletes accumulated, point lookups, filters and counts
on one 44 M-row segment go from 3–4 ms to 5.5–7.5 ms, while eight segments
stay at 1.3–3 ms.

### 4.2 The row cap

#### The rule

Both costs above are bounded by the rows per segment, so rows are what has
to be limited. For a collection without a vector field this design adds a
limit counted in rows:

```
a segment is full  ⇔  bytes ≥ expectedSize  or  rows ≥ maxRows
```

- `expectedSize` is the byte target master has today (`maxSize`, or the
  DiskANN size). This design does not change it.
- `maxRows` is a new parameter, `dataCoord.segment.maxRows`, 4,000,000 by
  default; 0 means no limit, and it can be changed online.
- The cap applies only to collections without a vector field
  (`HasVectorField(schema) == false`). A collection with a vector field does
  not read the parameter and behaves exactly as today.
- Rows are counted. Nothing goes through an estimate of the row size. A JSON
  field, a dynamic field or a long `varchar` in the schema does not affect
  whether the cap applies, and adding a scalar field does not move it.

The previous version of this document converted `maxRows` into a byte target
with the row size estimated from the schema (`EstimateSizePerRecord`). That
estimate uses fixed values for variable-length fields (`varchar` counts as
`min(max_length, 256)`, JSON and Array as 512 B), and the row size of a
collection without a vector field is mostly its variable-length fields, so
the conversion was off from the real row count by several times and the cap
did not apply at all once the schema had one JSON field. Section 7 gives the
reasons for counting instead and the alternatives that were dropped.

#### Where the cap is applied

Segment size is managed in bytes at every step. The row cap lands at each
step as follows, with no proto change:

| Step | Before | How the row cap applies |
|---|---|---|
| StreamingNode sealing a growing segment | sealed at `maxSize × seal proportion` bytes; the segment's row limit field exists and is set to infinity for L1 | the row limit is set to `maxRows` (with the same jitter, and **without the seal proportion**); the segment is sealed on bytes or rows, whichever comes first |
| Mix compaction planners (two-tier and legacy) | judge a segment full by its bytes, and pack small segments up to `expectedSize` with a knapsack | every segment handed to a planner is sized as `max(bytes, rows × expectedSize / maxRows)`. The full check and the packing code do not change |
| Compaction writer (DataNode) | starts a new output segment when it has written the `max_size` bytes of the plan | the plan also carries `max_segment_rows` (an existing field that only clustering set), and the writer cuts on bytes or rows, whichever comes first |
| Single segment compaction (delete ratio, expiry, schema version bump) | does not select segments by size; the writer splits the output | the plan carries `max_segment_rows`; the number of pre-allocated output segment IDs is computed from the size as charged above |
| Clustering compaction | output row limit = `expectedSize` ÷ measured bytes per row of the inputs | bounded by `maxRows` |
| Import | allocates segments of `expectedSize` bytes per (channel, partition) | the byte target is `min(expectedSize, maxRows × measured bytes per row of this import)`, where the measured value is the total bytes ÷ total rows of the pre-scan |

Why the seal row limit does not take the seal proportion: the proportion
(0.12 by default) keeps a growing segment, which is held in memory, well
below `maxSize`, and is about bytes. Narrow rows are only about 100 MB at
`maxRows` rows, already below `maxSize × seal proportion` (123 MB);
multiplying again would only cut segments at 480 k rows and have compaction
merge them back. Without it, a growing segment of narrow rows is about the
size it has on master today (`int64 + int32` seals at about 4.9 M rows,
123 MB, on master), and a sealed segment is already full to the planners and
needs no merge.

What the planner row means: every row is charged at least
`expectedSize / maxRows` bytes (268 B by default). The charged size reaches
`expectedSize` exactly when the bytes or the rows reach their limit, so the
existing byte-based full check, fill-rate gate and knapsack need no second
dimension. The charge applies only to the copies of the segments a planner
works on; the segment size in meta, quota accounting and what the QueryNode
sees do not change.

Steps that do not apply the cap:

- **Force merge**: the caller gives the target size explicitly, which may be
  several times `maxSize`, and the row cap is not applied to it. Its output
  is full to the planners afterwards and is not split automatically.
- **Sort compaction after import and stats tasks**: one segment in, one
  segment out, no split.
- **QueryNode load admission**: decided by a memory estimate, not by the
  segment size target (Section 8).

#### Rows per segment

The steps differ in precision:

| Path | Precision |
|---|---|
| Growing segment seal | the StreamingNode counts the rows of every insert message and seals after `maxRows × jitter` (3.6–4.0 M rows by default). It is a soft limit: the insert message that crosses it is still written to the segment, so a segment can exceed it by the rows of one message |
| Compaction output | exact. The writer counts rows itself and splits the record batch that crosses the limit, so an output holds at most `maxRows` rows |
| Import | approximate. The **number** of segments is allocated as `total rows / maxRows`, but the DataNode writes each read batch into one of them at random, so the rows of a single segment drift from `maxRows` |

Measured (Appendix B.6, local x86 standalone): 10 M rows inserted in batches
were sealed directly into three segments of 3.75 M, 3.65 M and 2.60 M rows
(the last is the remainder at flush), with no merge, and a manual compaction
left the row counts unchanged. 12 M rows imported from one parquet file were
allocated 3 segments; two runs gave 7.80 M, 2.80 M, 1.40 M rows and 4.20 M,
4.20 M, 3.61 M rows, depending on which segment each read batch (about 1.4 M
rows) landed in at random. A manual compaction of the second run cut each
4.20 M-row segment into 4,000,000 and 197 k rows, and the three full
segments were not merged again during the following 200 s of automatic
trigger cycles. The uneven spread of an import
exists for every collection (in bytes too) and is not introduced by this
design; where it matters, a later compaction cuts the segment on rows.

Import uses the measured bytes per row instead of a count because it has to
decide, in bytes and before writing any data, how many segments to allocate,
and the pre-scan already gives the real bytes and rows of the data. The
conversion is done once per import job and is fixed for the job, so the
planner-against-writer mismatch described below cannot arise. The segments
an import produces are judged by their real row count by the planners
afterwards.

#### No repeated re-compaction

The case to avoid: the compaction writer splits its output by one rule and
the planner judges a segment full by another, so an output the writer just
filled looks small to the planner, is selected and merged again, is split
again, and so on without end. In simulation, cutting on rows in the writer
while the planner still looked at bytes only gave a write amplification
of 70.

Here the planner and the writer use the same rule and the same numbers:

- The writer cuts when the rows reach the plan's `max_segment_rows`. That
  value is `maxRows × the plan's max_size / expectedSize` and is fixed when
  the plan is generated.
- The planner charges that output `rows × expectedSize / maxRows`, which is
  not less than the plan's `max_size`, so the output is full to the planner
  and is not selected again.

The fragment tier of the two-tier planner uses `expectedSize / 4` as
`max_size`; its `max_segment_rows` shrinks by the same ratio to
`maxRows / 4`, and the equality above still holds.

After `maxRows` is raised online, segments that were full under the old cap
are merged once more, the same as when an operator raises `maxSize`.
Lowering it does not split existing segments.

## 5. Compatibility

- proto, etcd layout, binlog format and `DescribeCollection` output are
  unchanged.
- `common.enableCollectionWithoutVector` is a permanent switch (Section 2)
  with no plan to remove it. Rolling upgrade: turning it off before the
  control plane is upgraded is optional; a collection without a vector field
  created during the upgrade window is not compacted and not loadable until
  every role is upgraded, and works afterwards. After a downgrade such a
  collection can still be read and dropped, but is not compacted.
- The legacy pymilvus ORM (Object-Relational Mapping) rejects the schema on
  the client. `MilvusClient` and the Java, Go and Node SDKs work. Restore or
  CDC (Change Data Capture) into an old cluster fails at `CreateCollection`
  with the old error.
- Collections with a vector field are not affected by the row cap:
  `dataCoord.segment.maxRows` applies to collections without a vector field
  only, and their segment size target, seal, compaction and import are the
  same as today. Collections without a vector field are new, so no existing
  collection changes its segmentation.
- The row cap during a rolling upgrade: `max_segment_rows` on a mix
  compaction plan is a new use, and an old DataNode ignores it and cuts on
  bytes only, so an output may exceed `maxRows`; the new planner treats such
  an output as full and does not merge it again. An old StreamingNode seals
  on bytes only. Behaviour is uniform once every role is upgraded.
- Index readiness of vector collections is unchanged, including the DiskANN
  size rule of sparse-only collections. The meaning of
  `dataCoord.dataview.forceAllIndexReady` is unchanged; it has no effect on
  a collection without a vector field, whose handoff already waits for every
  defined index.

## 6. Testing

- Unit tests:
  - the predicate itself;
  - every touchpoint of Appendix A, with and without a vector field;
  - the two readiness predicates with no index, scalar indexes only, and a
    vector index building or finished;
  - the row cap applies to schemas without a vector field only, still
    applies with a JSON field, and is disabled by `maxRows=0`;
  - both mix planners produce no plan for segments holding `maxRows` rows
    and pack smaller segments up to at most `maxRows` rows;
  - the compaction writer cuts at `max_segment_rows`; the row limit of the
    StreamingNode seal; the clustering row limit; the conversion of the
    import byte target.
- End-to-end tests under `tests/python_client/milvus_client/`:
  - create without a vector, load with no index, then query, get, count,
    iterator, delete and upsert;
  - with scalar indexes, partition key and clustering key;
  - bulk import followed by compaction;
  - search returns the unified error;
  - `add_field` with a vector is rejected and with a scalar succeeds;
  - the switch turned off;
  - REST v2;
  - after 10 M narrow rows are inserted (dynamic field on), compaction
    outputs stay at or below `maxRows` rows and the IDs of full segments do
    not change over the following automatic trigger cycles; an import of
    12 M narrow rows produces at least 3 segments; a low-dimensional vector
    collection is not capped.

## 7. Alternatives

- **An explicit table type or proto flag.** Every client and tool would have
  to know it. Whether a collection has vectors is decided at creation and
  the schema already carries it, so it is not needed.
- **Handoff waits for every defined index for all collections.** This is a
  review suggestion and is the same as changing the default of
  `forceAllIndexReady` to true. Compaction outputs of a vector collection
  would become visible one scalar index build later; a permanently failing
  scalar index, or a scalar index created during the build, would keep the
  output invisible or delay it further, which only a vector index can cause
  today. What it removes is the window in which the output is visible, its
  scalar index is still building, and scalar filtering on that segment falls
  back to brute force. This document does not change the default: a
  collection without a vector field does not need it, and the trade-off for
  vector collections is for the DataCoord owners to decide separately, after
  which only the default has to be flipped.
- **Convert the row cap into a byte target with the row size estimated from
  the schema** (the previous version of this document):
  `expectedSize = min(maxSize, maxRows × estimated bytes per row)`, changing
  only `GetExpectedSegmentSize` and leaving every step comparing bytes.
  Dropped for three reasons. First, the estimate uses fixed values for
  variable-length fields (`varchar` as `min(max_length, 256)`, JSON and
  Array as 512 B): with one JSON field, a dynamic field or a `varchar` of
  `max_length ≥ 252` in the schema the estimate exceeds 268 B and the cap
  does not apply. Second, it is inaccurate where it does apply:
  `int64 + int32` is estimated at 28 B and is 11–12 B in reality, and the
  measured results were 5.6–12.6 M rows on import and 0.6–2.9 M rows at seal
  (Appendix B.2). Third, it added a dependency on the schema estimate in
  compaction, seal and analyze, while segment sizing on master is entirely
  based on measured bytes.
- **Convert with the measured bytes per row of the inputs of each compaction
  plan.** The measured value changes with the set of candidate segments: an
  output the writer cut by the average of one set may be judged not full
  when the next planner round measures it against the average of another
  set, and is merged again. That is the case Section 4.2 excludes. Only
  measuring a segment against itself is stable, and that is the same as
  comparing its row count. Import can convert this way because one import
  makes one static allocation (Section 4.2).
- **Apply the row cap to all collections.** A low-dimensional vector
  collection (`FLOAT_VECTOR` of dim 8, for example) also exceeds `maxRows`
  rows at 1 GB. This document caps collections without a vector field only,
  so that vector collections have no behaviour change; lifting the
  restriction is removing the one `HasVectorField` condition, and is listed
  in Section 8.
- **No row cap.** Index build and load replay are serial per segment and
  grow with the row count (Section 4.1); the cap can be turned off with
  `maxRows=0`. The cap stays.

## 8. Follow-ups

- Converting a collection between having and not having vectors: adding the
  first vector field to a collection without one, or dropping the last
  vector field of a vector collection. This document rejects both.
- The loader's memory estimate is about 3× too low for narrow fixed-width
  rows. The QueryNode uses it for admission before loading a segment; the
  formula adds up the `MemorySize` of each field's binlogs and multiplies by
  an expansion factor, with no term that grows with the row count. For 44 M
  rows of `int64 + int32` it estimates 41 B per row (about 1.8 GB) against a
  real RSS increase of 120 B per row (5.3 GB); for
  `int64 + int32 + varchar(64) + JSON(115)` it estimates 423 B against
  265–422 B. The missing 80 B or so is per-row fixed overhead: the primary
  key lookup structure, the timestamp column, the delete bitmap and chunk
  metadata. It is negligible for a vector collection with rows of several
  KB and several times the data itself for 12 B rows. The consequence is
  that a QueryNode close to its memory limit may admit a segment that does
  not fit. The formula needs a term "rows × per-row constant", calibrated on
  fixed-width schemas.
- Whether to change the default of `dataCoord.dataview.forceAllIndexReady`
  to true so that handoff of vector collections waits for scalar indexes too
  (Section 7).
- Whether to extend the row cap to collections with a vector field
  (Section 7).
- On import the DataNode writes each read batch into one of the allocated
  segments at random (`PickSegment`); with few batches the rows per segment
  differ by several times. Choosing the segment with the least data written
  would bring imported segments close to the target size, for every
  collection.
- Remove DataCoord's legacy row-based segment allocation: the
  `AssignSegmentID` RPC has no caller on master, and the `AllocSegment`,
  `calBySchemaPolicy` (which derives the maximum rows of a segment from the
  row size estimated from the schema) and `MaxRowNum`-based seal policies
  behind it serve that path only. This is the only code on master that uses
  a schema estimate to decide the rows of a segment. The last caller of the
  RPC was the 2.5.x Proxy (removed by #43651 in 2.6.1), and it has been kept
  for the 2.5 → 2.6 rolling upgrade; 3.x does not support a direct rolling
  upgrade from 2.5.x, so it can go. The removal has to rewrite the tests of
  the `datacoord/segment` package and is a PR of its own.

## Appendix A. Touchpoints

The "must have a vector field" restriction exists only at the create entry
and in a few Proxy and DataCoord checks. Below that, segcore, QueryNode,
storage, the WAL (Write-Ahead Log), flush and compaction all work field by
field and do not depend on a vector field, so most of the change is removing
checks. Every decision of Section 2 reads the schema through one function,
with no new collection type, property or proto field:

```go
// HasVectorField reports whether the schema declares a vector typed field
// (top-level, struct sub-field or function output).
func HasVectorField(schema *schemapb.CollectionSchema) bool {
    return len(GetVectorFieldSchemas(schema)) > 0
}
```

The remaining changes are checks that assumed at least one vector field and
misbehave at zero: two Proxy drop checks, a REST (Representational State
Transfer) insert check, a pymilvus check, the DiskANN segment size rule, and
an import reader that loops forever on a file with no matching column.

The table lists every check in the code that assumes "at least one vector
field", what it does today and how it changes. File and line numbers are
from master `6140f5f55ca`, except the two schema evolution rows, which are
from `3df0cbcdd68`.

| Site | Today | Change |
|---|---|---|
| `proxy/task.go:432` `createCollectionTask.PreExecute`; RootCoord `create_collection_task.go` `validateSchema` | no vector → reject (Proxy only) | reject only when the switch is off; RootCoord decides, the Proxy checks early |
| `proxy/util.go:1048` `validateLoadFieldsList`, `metacache/meta_cache.go:298` | at least one vector must be loaded | only if the schema has vectors |
| `proxy/task.go:3141`, `:3410` load collection / partitions | `ErrIndexNotFound` fails the load | fail only if a loaded vector field has no index; QueryCoord already treats "no index" as an empty index list |
| `querynodev2/services.go:206` `WatchDmChannels`, `:497` `LoadSegments` | an empty `IndexInfoList` returns `ErrIndexNotFound`, the channel watch retries forever and the load times out after `loadTimeoutSeconds` | the check applies only when the schema has vector fields |
| `httpserver/utils.go:3061` `anyToColumns` | REST insert needs a vector or function | check removed |
| RootCoord `prepareAlterSchemaAddField` and the function-add path; Proxy `task.go:644` `addCollectionFieldTask`, `:1033` `alterCollectionSchemaTask` | a vector field, struct vector sub-field or vector-output function may be added | rejected when `HasVectorField` would turn from false to true (RootCoord decides, the Proxy checks early) |
| `proxy/task.go:1297`, `:1337` drop struct field / function | `removed >= vectorCount` blocks the drop; always true at 0, so every struct-field and function drop is blocked | `removed > 0 &&`, as RootCoord already does |
| `datacoord/util.go:77` `FilterInIndexedSegments` (now in `datacoord/index/filter.go`) | with no vector the result depends on unrelated details (Section 3.2) | split into `handoffReady` / `compactionInputReady`; the handoff target fields are shared with the staging `PrepareReadiness` through `HandoffIndexTargetFields` |
| `datacoord/index_meta.go:1497` `AllDenseWithDiskIndex` (now in `datacoord/index/meta.go`) | true at 0 dense fields, so a scalar-only collection gets the 2 GB DiskANN target | returns false when there is no vector field at all; a sparse-only collection keeps today's result |
| `datanode/importv2` Parquet / NumPy / CSV / JSON readers | a file that matches no schema column makes the task loop forever: the reader produces no row and never reaches EOF; a vector field used to prevent this | rejected when the reader is constructed |
| StreamingNode `segment_limitation_policy.go`; DataCoord `compaction/trigger.go`, `trigger_v2.go`, `compaction/policy/helpers.go`, `clustering.go`, `bulkimport`; DataNode `compactor/segment_writer.go` | seal, full check, split and allocation on bytes only | collections without a vector field get the additional `maxRows` row cap (Section 4.2); `GetExpectedSegmentSize` is unchanged |
| Search entries, REST handlers | four different errors | one error |
| pymilvus `orm/schema.py:1330` `check_schema` | client-side reject | removed |

## Appendix B. Measurements

This appendix holds all the measured data the body refers to. B.1 answers
"what is the difference between the same data in one segment and in eight",
B.2 and B.3 are end-to-end results on `int64` and `varchar` primary keys,
B.4 is the distributed deployment and the switch flip, B.5 the bytes per row
of the same data under each accounting, and B.6 the segmentation produced by
the current implementation of the row cap. The **segment counts and rows per
segment** in B.2 and B.3 were measured with the implementation of the
previous version of this document (a byte target converted through the
schema estimate) and are kept as the evidence for dropping it in Section 7;
the timings measured at those segment sizes remain valid. All runs:
single-node standalone unless stated, one shard, 44 M rows of `int64` pk +
`int32`. Two machines: aarch64 (20 cores) and x86 (AMD Ryzen 9 7945HX).

### B.1 One segment against eight

Implementation branch at `5ee87283ba` (PR #54543 in master), aarch64,
`maxRows=0` so the target is 1 GB, automatic compaction off. The same 44 M
rows of `int64 + int32`: one parquet file imports as 1 segment, eight files
of 5.5 M rows import as 8. The two groups ran one after the other in one
process, the second after the first was released.

| Item | 1 × 44 M | 8 × 5.5 M | Note |
|---|---:|---:|---|
| **Write side** | | | |
| Import complete (sort compaction included) | 69.1 s | 27.2 s | one job is serial; eight jobs in parallel, 24–27 s each |
| Sort compaction | 20.3 s | 2.6–3.4 s per segment, eight in parallel | linear in rows, serial per segment |
| `INVERTED` build on `int32` | 28.6 s | 8.6 s (parallel) | one task per segment |
| insert_log on disk | 465 MiB | 8 × 58.1 = 465 MiB | 11 B per row, independent of segmentation |
| **Read side, no deletes** | | | |
| Load time | 7.1 s | 3.5 s | |
| RSS increase after load | +5.1 GB | not comparable | the second group reused memory the first did not return after release and shows only +1.1 GB; in fresh processes on 2026-10-07 both groups were +5.3 GB |
| Point / filter / count query | 3.0–4.7 ms | 1.4–2.9 ms | first cold query excluded |
| **Deletes** | | | |
| Delete 1 % by expression, tsafe stall | 0.18 s | 0.17 s | |
| Delete 10 % by expression, tsafe stall | 1.74 s | 1.69 s | one segcore `Delete` call 1.21 s against 1.14 s (slowest segment) |
| Delete 10 % by pk list, 44 requests of 100 k, stall | 0.10 s | 0.11 s | |
| Queries with 9.24 M deletes accumulated | 5.5–7.5 ms | 1.3–3.0 ms | |
| Release and load again with 9.24 M deletes | 10.8 s | 5.5 s | |

Loader estimate against RSS: `int64 + int32` is 29 B per row of
`MemorySize`, 41 B estimated, 120 B RSS;
`int64 + int32 + varchar(64) + JSON(115)` is 217 B, 423 B and 265–422 B.

### B.2 Previous implementation, `int64` primary key

Branch `nonvec-phase1-enable` at `5ee87283ba` on master `3f0e3b9a77` (PR
#54543 in master), aarch64. No vector field, 44 M rows imported from one
parquet file, automatic compaction on, `maxRows` 4 M, size target 112 MB.

| Step | Result |
|---|---:|
| Import (48 s), segments after sort compaction | 5 segments: 12.59 M, 10.42 M, 8.39 M, 7.00 M, 5.60 M rows (59–133 MiB of binlog each, 11 B per row) |
| `INVERTED` on `int32`, 44 M rows | 10.6 s (28.6 s on one segment, B.1) |
| Load with no index | 3.7 s, RSS +4.6 GB |
| Delete 1 % by expression, tsafe stall | 0.17 s |
| Delete 10 % by expression, tsafe stall | 1.32 s |
| Delete 10 % by pk list, 44 requests of 100 k, stall | 0.06 s |
| Delete-ratio compaction after 20 % deleted | ran within 2 min, outputs visible; 8 segments, outputs 0.96–4.79 M rows, the untouched 10.42 M import segment kept |
| Release and load again, 9.24 M deletes replayed | 3.5 s |
| Point / filter / count query | 1–4 ms throughout |
| `search`, `hybrid_search`, add vector field | rejected with the unified error; adding a scalar field succeeds |

Growing segments of the same schema (x86 standalone) sealed at 0.6–2.9 M
rows (the seal threshold was 112 MB × 0.12 ≈ 13 MB; the observed rows include
segments automatic compaction had already merged); on a
10 M-row insert the compaction outputs stayed at or below 3.45 M rows.

### B.3 Previous implementation, `varchar` primary key

Same branch and machine as B.2. 44 M rows with a 16-character zero-padded
`varchar` pk and an `int32`, imported from one parquet file. With
`max_length` 256 the estimate is 276 B per row, above the 268 B threshold,
so the target stayed at 1 GB: import wrote 2 segments of 24.85 M and 19.15 M
rows (241 MiB and 186 MiB of binlog). With `max_length` 32 the estimate is
52 B and the target 208 MB: import wrote 8 segments of 4.67–6.54 M rows
(45–63 MiB each), and the compaction outputs after the deletes were
3.74–5.14 M rows.

| Step | `max_length` 256 (no cap) | `max_length` 32 (208 MB) |
|---|---:|---:|
| Import | 69 s, 2 segments | 63 s, 8 segments |
| `INVERTED` on `int32` | 16.6 s | 8.6 s |
| Load with no index | 4.9 s, RSS +5.2 GB | 3.1 s, RSS +5.5 GB |
| Delete 1 % by expression, stall | 0.36 s | 0.35 s |
| Delete 10 % by expression, stall | 3.0 s | 2.74 s |
| Delete 10 % by pk list, 44 × 100 k, stall | 0.08 s | 0.07 s |
| Release and load again, 9.24 M deletes | 4.9 s | 3.5 s |
| Point / count query | 1–4 ms | 1–3 ms |

### B.4 Distributed deployment and switch flip

Same branch, x86, run as five processes (mixcoord, proxy, querynode,
datanode, streamingnode on Pulsar): create, insert, load with no index,
query, delete, scalar index, manual compaction with visible outputs, and the
`search` / add-vector-field rejections behave as in standalone. With a
collection without a vector field loaded, every role was restarted one at a
time with `common.enableCollectionWithoutVector=false`: the existing
collection kept serving count, insert, get, delete, filter queries and a
release-and-load cycle, a new `CreateCollection` without a vector field was
rejected with "schema does not contain vector field", and collections with a
vector field were created as before. Restarting the roles again with the
default restored the create.

### B.5 Bytes per row of the same data under each accounting

aarch64, 5 M rows, schema `int64` pk + `varchar` (average length 64,
`max_length` varied) + JSON (average 115 B), two collections: sv without a
vector, vv with one 512 B vector field. The last column is who uses each
accounting on master. This is the evidence for dropping the schema-estimate
conversion in Section 7 and for the loader estimate item of Section 8.

| Accounting (bytes per row) | sv: no vector | vv: 512 B vector | vv scalar part (minus 512 B) | Used by |
|---|---:|---:|---:|---|
| Real length of the user data | 191.8 | 703.8 | 191.8 | |
| Schema estimate, custom policy (`varchar` as `min(max_length, 256)`, JSON as 512) | 797 | 1309 | 797 | writebuffer batch, QueryNode interim vector index parameter |
| Schema estimate, max policy (`varchar` as `max_length`) | 1565 | 2077 | 1565 | import rows per read batch = 16 MiB / this value |
| binlog `MemorySize` (uncompressed arrow bytes written by storage v2) | 216.6 | 728.8 | 216.8 | StreamingNode seal (message BinarySize, same accounting), compaction target, import segmentation, QueryCoord |
| insert_log on disk (sorted segment, parquet compressed) | 105.0 | 618.6 | 106.6 | object storage, GC |
| QueryNode loader logical estimate | 423.1 | 424.5 (another 513 B per row counted as diskSize: vector and FLAT index) | 424.5 | load admission |
| Process RSS increase (before and after load) | 422.4 | 777.2 | 265.2 | real usage |

On variable-length fields the loader estimate is close to RSS (423 against
422); the estimate is off on narrow fixed-width rows (measured 2026-10-07:
41 B per row estimated, 120 B per row RSS). The schema estimate rounds
variable-length fields up to 797 B, 3.7× `MemorySize`; this is why, under
the schema-estimate conversion of Section 7, a `varchar` with a long
`max_length` or a JSON field made the cap not apply.

### B.6 Segmentation under the row cap

Branch `nonvec-phase1-enable` on master `54ff5d5266`, x86 standalone,
`maxRows` 4 M, seal proportion 0.12, jitter 0.1, everything else default.
Script `e2e_rowcap.py`.

| Scenario | Result |
|---|---|
| No vector field, `int64 + int32`, **dynamic field on**, 10 M rows inserted in batches of 50 k, then flush | 3 segments: 3.75 M, 3.65 M, 2.60 M rows, straight from sealing with no merge; `count(*)` is 10 M |
| One manual compaction of the collection of the previous row | still 3.75 M, 3.65 M, 2.60 M rows |
| No vector field, `int64 + int32`, 12 M rows imported from one parquet file | 3 segments allocated. Two runs: 7.80 M, 2.80 M, 1.40 M rows; 4.20 M, 4.20 M, 3.61 M rows |
| One manual compaction of the collection of the second import run | 5 segments: 4,000,000, 4,000,000, 3.61 M, 197 k, 197 k rows; a segment above `maxRows` is cut at `maxRows` and the remainder becomes a small segment |
| 200 s of automatic trigger cycles afterwards | the IDs of the three full segments do not change; they are not merged again |
| Control: `int64 + int32 + FLOAT_VECTOR(dim 8)`, 6 M rows inserted, then a manual compaction | 1 segment of 6 M rows; a vector collection is not capped |

The collection of the first row has a dynamic field (JSON). Under the
schema-estimate conversion its estimated row size exceeds 268 B, the cap
does not apply, and the 10 M rows are merged into one segment.

An earlier revision of the implementation set the seal row limit to
`maxRows × seal proportion` (480 k rows). The same 10 M-row insert first
sealed into about 20 small segments, which compaction then merged into 4
segments of 2.25–3.0 M rows. That was dropped for the reason given in
Section 4.2.

The final run on head `2cc7740f6e` (2026-11-09 and 11-10) repeated the
script twice, once with the default `maxRows` of 4 M and once with `maxRows`
set to 400 k and every row count divided by 10.

| Scenario | `maxRows` 4 M | `maxRows` 400 k |
|---|---|---|
| Insert (10 M / 1 M rows), then flush and a manual compaction | 3.95 M, 3.75 M, 2.30 M rows | 400 k, 400 k, 200 k rows |
| Import from one parquet file (12 M / 1.2 M rows) | 3 segments allocated; 5.60 M, 3.61 M, 2.80 M rows | 3 segments allocated; all 1.2 M rows in one of them, the two empty ones are dropped |
| One manual compaction of the imported collection | 4,000,000, 3.61 M, 2.80 M, 1.60 M rows | 400,000, 400,000, 400,000 rows |
| 200 s of automatic trigger cycles afterwards | unchanged | unchanged |
| Control: vector collection, dim 8 (6 M / 600 k rows) | not run, see Appendix C | 1 segment of 600 k rows |

In the 400 k import the file (1.2 M rows, 14.4 MB in memory, 12 B per row)
is smaller than one read batch of the import, so the whole file is one
hashed piece and `PickSegment` writes it into one of the three allocated
segments. The byte target after the conversion is 400 k × 12 B = 4.8 MB and
14.4 MB / 4.8 MB gives the three segments seen in the DataCoord log. The
segment count is right and the rows per segment are not, as Section 4.2
states for the import path; the following compaction cuts at `maxRows`.

## Appendix C. Verification record

Implementation branch `nonvec-phase1-enable`, head `2cc7740f6e` on master
`54ff5d5266` (including the DataCoord package split #54499 and QueryView
#52977), Draft PR #54604. Five commits: lifting the creation check and
fixing the checks, e2e case updates, the DataCoord readiness split, the row
cap, and the import reader validation. The measurements of B.1–B.5 were
taken on the earlier head `5ee87283ba`.

Everything below was run on head `2cc7740f6e`, 2026-11-09 and 11-10, x86
standalone, unless stated otherwise.

- **Build**: passes.
- **Unit tests**: the 17 groups of Go packages touched by the change pass,
  among them DataCoord `compaction` (with `policy`, `task`, `view`,
  `targets`), `bulkimport`, `meta`, `analyze` and the top-level package,
  DataNode `compactor`, StreamingNode `shard` and `internal/proxy`. New
  cases cover: the row cap applies to schemas without a vector field only
  and is not affected by a JSON field; both mix planners produce no plan for
  segments holding `maxRows` rows and pack smaller ones; the writer cuts on
  rows and splits the record batch that crosses the limit; the output count
  estimate of the mix compactor counts rows; the row limit of the
  StreamingNode seal; the clustering row limit; the conversion of the import
  byte target. `internal/proxy` timed out in the first run because the
  shared etcd and object storage namespace held state of earlier runs (log:
  `summary manifest term 60 supersedes writer term 2`); rerun with a fresh
  namespace the package passes in 88 s. Two cases of `pkg/util/paramtable`
  fail (`TestComponentParam_QueryViewConcurrency`,
  `TestComponentParam_QueryViewLiveDispatchConcurrency`); they fail on an
  unmodified master as well. `configs/milvus.yaml` was regenerated with the
  config tool; only the comment of `maxRows` changes.
- **Functional and compaction end-to-end scripts** on a local standalone:
  pass.
- **pytest**: the 4 new cases
  (`test_milvus_client_collection_without_vector.py` and one case of
  `test_milvus_client_collection.py`) pass.
- **Row cap end to end** (Appendix B.6), two runs. With `maxRows` 4 M the
  first 7 checks gave the expected output; the run then stopped in the
  eighth (the vector collection control) because the data disk crossed the
  0.90 hard watermark of woodpecker and writes were rejected. With `maxRows`
  400 k and all row counts divided by 10, 7 of 8 checks pass, the vector
  collection control among them. The one that fails is "import allocates at
  least rows / `maxRows` non-empty segments": the file is smaller than one
  read batch, which is a property of the reduced scale and of `PickSegment`
  (B.6), and the DataCoord log shows the expected three segments allocated.
  Each check has passed on this head in at least one of the two runs.
- **Verified on earlier heads and not rerun**: the 44 M-row aarch64 runs
  (B.1–B.3) and the five-process deployment and switch flip (B.4). Apart
  from segment sizing, the code on these paths did not change.
- **Not redone**: the planner simulation of the previous version (80 trigger
  cycles) was for the byte-target approach and has not been rerun against
  the current implementation; the stability of the current implementation
  rests on the equality of Section 4.2, the unit tests of both planners and
  the 200 s observation of B.6. Import plus compaction at 44 M rows has not
  been measured on the current implementation either.

## References

- Review comments on PR #53983: xiaofan-luan (2026-10-10, asked for the publication delay, small-segment and index-failure impact of the readiness split in Section 3) and Li Liu (2026-10-21, phase split)
- PR #54543 `enhance: merge sorted delete batches against the sorted primary-key column` (merged 2026-10-23)
- Druid `maxRowsPerSegment` (5 M), TiKV region (96 MB or 960 k keys), Elasticsearch shard guidance (≤ 200 M documents): comparable systems cap both bytes and rows per unit.
