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
readiness (Section 3) and segment size (Section 4). The index readiness
change applies to collections without a vector field only. The segment size
change is a row limit per segment that applies to every collection; it has an
effect only on collections of narrow rows, with or without a vector field
(Sections 4.2 and 5).

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
| Add a nullable vector field, or a struct array field with a vector sub-field, to a collection without one | supported: the collection becomes a vector collection (Section 2, "Conversion") |
| Drop the last vector field of a vector collection | supported while `common.enableCollectionWithoutVector` is on: the collection becomes one without a vector field |
| Quick-setup paths (`create_collection(dimension=...)`, REST `quickCreate`) | unchanged, still create a vector collection |

Whether a collection has vectors is read from its current schema, by one
predicate (`typeutil.HasVectorField`: top-level vector fields, vector
sub-fields of struct array fields, and function outputs). A collection can
move between the two states; the rules for load and compaction follow the
schema at the time they are evaluated, and the segment row cap does not
depend on the schema (Section 4.2). A struct array
field with only scalar sub-fields does not make a vector collection.

The implementation is Draft PR #54604, one PR for creating collections
without a vector field and for the conversion; its description lists the
commits.

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
- **Adding a vector** to such a collection, and dropping the last vector
  field of a vector collection, are described under "Conversion" below.
- **Search**: `search`, `hybrid_search`, `search_iterator` and their REST
  counterparts return one error, `collection %s has no vector field, search
  is not supported`.

**Conversion**

*No vector field → vector.* A vector field can be added to a collection
without one under the rule that already applies to adding a vector field to
a vector collection: the field must be nullable. The same holds for a struct
array field with a vector sub-field, and for a vector sub-field added to a
struct array field that has only scalar sub-fields; such a sub-field must be
element-nullable (existing rule), and an element-nullable vector array
supports only element-level metrics and search, not embedding-list search.
Nothing is backfilled:

- rows written before the field exists read it as null and are not returned
  by a search on it;
- a loaded collection stays loaded and queries are not interrupted. A search
  on the new field returns `field index of the field: <name> is not loaded,
  please reload the collection` until an index is built on it and the
  collection is loaded again;
- from the moment the field exists the collection is a vector collection:
  the next load needs an index on the field and is rejected without one
  (`index not found` when the collection has no index at all, `there is no
  vector index on field` when it has other indexes).

*Vector → no vector field.* The last vector field can be dropped, as a
top-level field, as a struct array field, as the vector sub-field of a
struct array field, or as a function field. Its index is dropped with it. A
loaded collection stays loaded; search returns the error of a collection
without a vector field; the next load needs no index.

Dropping the last vector field produces a collection that
`common.enableCollectionWithoutVector` forbids to create, so it follows the
switch: with the switch off the drop is rejected with the previous errors
(`cannot drop the last vector field`, `it would leave no vector field in
the collection`). Adding a vector field is not gated.

A field dropped and added again under the same name is a new field with a
new field ID: rows written before the drop read it as null, not their old
vector.

No rule other than the checks at the schema-change entry had to change,
because each of them reads the current schema:

| Rule | Where it reads the schema | Effect of a conversion |
|---|---|---|
| Load needs a vector index | Proxy load task, QueryCoord automatic load | applies from the next load after a vector field is added; stops applying after the last one is dropped |
| Search is rejected | Proxy search task | follows the schema immediately |
| Index readiness for handoff and compaction input (Section 3) | computed per evaluation from the schema and the index metadata | the fields to wait for change with the schema; a vector field with no index defined is not waited for |
| Segment row cap (Section 4) | not read from the schema | unchanged by a conversion: segments are sealed, compacted and imported at `maxSize` bytes or `maxRows` rows before and after it |

An earlier revision of #54604 applied the row cap to collections without a
vector field only. Adding a vector field then lifted the cap at once, while
the rows written earlier stayed narrow (there is no backfill): with
`maxRows` set to 400 k, three capped segments of 400 k rows were merged into
one of 1.2 M rows about 30 s after the field was added, and after another
1.2 M rows without a vector a compaction left one segment of 2.4 M rows. The
cap therefore no longer depends on the schema. Measured with `maxRows` set to
400 k (local standalone, 2026-11-11):

| Direction | Step | Segment rows |
|---|---|---|
| no vector → vector | 1.2 M rows before the change | 400 k, 400 k, 400 k |
| | vector field added, nothing written; then a manual compaction | 400 k, 400 k, 400 k: not merged |
| | 1.2 M rows without a vector after the change, flushed, compacted | six segments of 400 k; unchanged over the next 120 s |
| | (second run) 600 k rows with 8-dim vectors after the change | sealed at 400 k; no segment above `maxRows` after a compaction |
| vector → no vector | 1 M rows (dim 8) before the change | 391 k, 368 k, 241 k |
| | 500 k rows after the change, flushed | new segments sealed at `maxRows` |
| | one manual compaction | 400 k, 391 k, 368 k, 341 k; full segments keep their IDs over the next 120 s |

One defect was found and fixed: a collection loaded while it had no vector
field and no index has no index meta in segcore, and a search on a vector
field added afterwards failed an assertion (`index meta not exist`) instead
of returning the "not loaded" error above.

The conversion depends on QueryView (#52977), which is the only load path
on master and the one every test ran on. The QueryNode entries used before
it (`WatchDmChannels` / `LoadSegments`) reject an empty index list when the
schema has a vector field, the state of a loaded collection that gained a
vector field and has no index yet; they are not adapted and the conversion
is not supported on them.

Rolling upgrade. A collection without a vector field can only be created
through a Proxy that has this change: the Proxy before it rejects the schema
itself (`schema does not contain vector field`). The Proxy is the last role
to be upgraded, so by the time such a collection can exist every QueryNode
and DataNode already has the change, and a conversion never runs against an
older node. If a Proxy were upgraded before the QueryNodes, the one
difference on an older QueryNode would be the text of the search error on a
vector field added to a collection loaded with no index (a segcore assertion
message in place of "not loaded"); this was read from the code and not run.
The upgrade from a deployment that predates QueryView is the subject of
#52977, whose design leaves the migration of the earlier load metadata open;
it does not involve collections without a vector field, which cannot exist
before the upgrade.

Function fields. A function field whose output is a vector (BM25,
MinHash) added to a collection without a vector field also makes it a
vector collection. Unlike a nullable vector field, its output must be
computed for the rows written earlier. The existing rule for adding a
function field applies unchanged: it needs StorageV3
(`common.storage.useLoonFFI`), the schema-bump compaction
(`dataCoord.compaction.bumpSchemaVersion.enabled`) and the storage-version
upgrade compaction (`dataCoord.compaction.storageVersion.enabled`), and is
rejected otherwise. With them on, a BM25 function field is backfilled the
same way on a collection without a vector field as on one with a vector
field (Section 6.2); MinHash goes through the same path (Section 6.3).

Backfill and segment size. There are two ways a field added to an existing
collection gets values for the rows written earlier, and both write in
place: the segment keeps its ID and its row count, and its bytes grow by
rows × the size of the new value.

- **Function fields** (BM25 and MinHash only,
  `schemautil/function_backfill.go`): the schema-bump compaction appends the
  output column to each segment, one segment in and the same segment out.
- **External backfill** (`CommitBackfillResult`,
  `datacoord/services_commit_backfill.go`): a Spark job writes column files
  for existing segments and DataCoord commits them to the manifest of each
  segment. Any user field except the primary key, the partition key, a
  system field, a function output and the dynamic field can be written this
  way, a nullable vector field included. This path was read, not run.

So a segment of a collection without a vector field, sized by the row cap,
can exceed the byte limit after a backfill: 4 M rows are about 2.4 GB with a
128-hash MinHash (512 B per row), and about 2.1, 4.2 and 12.4 GB with an
externally backfilled `FLOAT_VECTOR` of dim 128, 256 and 768. The row cap
bounds this (without it a 1 GB segment of narrow rows holds tens of millions
of rows), but it does not prevent it. A segment above the byte limit is not
split in the background; a manual compaction splits it at the byte limit
(Section 6.3). After backfilling a vector into a collection that had none,
run a manual compaction. Splitting during or after a backfill automatically
is a follow-up (Section 8).

**Clients**

| Client | Create with an explicit schema that has no vector field | Change |
|---|---|---|
| pymilvus `MilvusClient` and `AsyncMilvusClient` | works | none |
| pymilvus ORM (Object-Relational Mapping) `Collection` | rejected on the client by `check_schema` (`No vector field is found.`) | none: the ORM interface is deprecated and stays as it is; use `MilvusClient` |
| Java SDK, v1 and v2 | works (release 3.0.12 run against the implementation branch: v2 create, insert, load, query, and search returns the server error; v1 create and load) | none |
| Go SDK | works | none |
| Node SDK | rejected on the client by `checkCollectionFieldsWithOptions` | the check is removed in milvus-io/milvus-sdk-node#626; the server already rejects such a schema when it does not accept it (the Proxy check in 2.4, 2.5, 2.6 and master, and the switch in this design) |
| RESTful v2 | works | done in the implementation PR (insert check, one search error) |
| RESTful v1 | has only the quick create by dimension | none |

Two rules are the same for every client and are the rules of a vector
collection:

- The quick-setup form of create (no schema, a dimension) still creates a
  vector collection.
- Create does not load the collection unless index parameters are passed
  with it. A collection without a vector field is usually created without
  index parameters, so the user calls load once after creating it. No client
  loads it automatically because it has no vector field.

`search` and `hybrid_search` reach the server and get the error above. The
V1 fallback of pymilvus `search_iterator` fails on the client with its own
message, `there should be at least one vector field in milvus collection`.
The Node SDK `search` failed on the client with `Target anns field not
found, please check your search parameters.`; with
milvus-io/milvus-sdk-node#626 it sends the request to the server, which
returns the error above.

**Tools and replication**

milvus-backup was run against the implementation branch; the other rows
come from reading the source of each tool and were not run.

| Tool | Result | Notes |
|---|---|---|
| milvus-backup (`2ec7cbd`) | no change needed, tested | Backup and restore of two collections without a vector field (one with no index, one with an `INVERTED` index; dynamic field on, 200 k rows, 1,000 rows deleted after flush, 500 rows not flushed) on a local standalone. Three restore paths give the source row count (199,500), no deleted row, equal sampled rows and the same index list: the snapshot format (the tool's default on this server), and the binlog format with the `per_seg` and `merged_seg` strategies. A binlog-format backup that contains L0 delete segments has to go through `milvus-backup l0compact` first: the server rejects L0 import by default (`dataCoord.import.enableL0Import=false`, #51194), for every collection, and without that step the restore fails and leaves the deleted rows visible |
| Replication built into Milvus (same version on both sides) | works | the secondary applies a replicated `CreateCollection` in the broadcast ack callback, below the Proxy check and RootCoord `validateSchema`. `common.enableCollectionWithoutVector=false` on the secondary therefore does not stop a replicated collection without a vector field: the switch applies to create requests received by that cluster, and a secondary mirrors its primary |
| Replication built into Milvus, older secondary | unknown | the old secondary would not reject the create message at the metadata layer; whether its QueryNode, DataNode and index code work with such a collection has not been examined. Replicating to an older version is not supported by this design |
| milvus-cdc (standalone tool, `3a4af3b`, built against 2.5 packages) | fails on a target that rejects the schema | it calls the target's public `CreateCollection`. A 2.5 or 2.6 target, or a target with the switch off, rejects it; the tool retries for about two days (17,280 attempts), during which the other collection events of that source and target pair wait, and then pauses the task. A task that lists its collections by name does not touch other collections |
| Attu | not examined | closed source since 2.6.0; the public repository has no code. Its server uses the Node SDK, so creating such a collection from Attu needs a Node SDK release with the change above |

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
to be limited. This design adds a limit counted in rows, for every
collection:

```
a segment is full  ⇔  bytes ≥ expectedSize  or  rows ≥ maxRows
```

- `expectedSize` is the byte target master has today (`maxSize`, or the
  DiskANN size). This design does not change it.
- `maxRows` is a new parameter, `dataCoord.segment.maxRows`, 4,000,000 by
  default; 0 means no limit, and it can be changed online. It is a
  cluster configuration and the only source of the value: there is no
  per-collection setting.
- The rule does not read the schema. Which of the two limits a segment
  reaches first is decided by its data: rows wider than
  `expectedSize / maxRows` (268 B by default) reach the byte limit first and
  behave exactly as today; narrower rows reach the row limit first.
- Rows are counted. Nothing goes through an estimate of the row size. A JSON
  field, a dynamic field, a long `varchar` or a vector field in the schema
  does not affect whether the cap applies, and adding or dropping a field
  does not move it.

The previous version of this document converted `maxRows` into a byte target
with the row size estimated from the schema (`EstimateSizePerRecord`). That
estimate uses fixed values for variable-length fields (`varchar` counts as
`min(max_length, 256)`, JSON and Array as 512 B), and the row size of a
collection without a vector field is mostly its variable-length fields, so
the conversion was off from the real row count by several times and the cap
did not apply at all once the schema had one JSON field. Section 7 gives the
reasons for counting instead and the alternatives that were dropped.

#### Which collections it affects

A collection without a vector field is the common case of narrow rows, and
the reason the cap exists. Collections with a vector field are affected only
when their rows are narrower than 268 B. Counting the vector alone, that is
a `FLOAT_VECTOR` below about 67 dimensions, `FLOAT16_VECTOR` below about
134, `INT8_VECTOR` below about 268, `BINARY_VECTOR` below about 2,144, and
sparse vectors of short documents. A vector collection of the common
dimensions is not affected: 1 GB is about 350 k rows at dim 768 and 2 M rows
at dim 128.

The 268 B is the byte limit of the collection divided by `maxRows`, so it
moves with the byte limit:

| Byte limit of the collection | Row width below which the row cap applies | `FLOAT_VECTOR` dimensions affected (vector alone) |
|---|---|---|
| 1 GB (`maxSize`, default) | 268 B | below about 67 |
| 2 GB (`diskSegmentMaxSize`, used when every vector index of the collection is DiskANN) | 537 B | below about 134 |
| 4 GB (`maxSize` raised) | 1,074 B | below about 268 |
| 8 GB (`maxSize` raised) | 2,147 B | below about 537 |

Two consequences. A collection indexed with DiskANN only is affected up to
about dim 134 with the default configuration. And a deployment that raises
`maxSize` moves common dimensions under the row cap: with `maxSize` at 8 GB
a dim 128 collection held about 16 M rows per segment and is sealed and
merged at 4 M rows after the upgrade, four times as many segments. Such a
deployment should raise `maxRows` in proportion to `maxSize` (or set it to 0)
when it upgrades; the description of the parameter says so. The effect of
four times as many segments on search was not measured (Appendix B.7
measures 2.3 times).

An earlier revision applied the cap to collections without a vector field
only, by reading the schema. It was changed for two reasons. A schema change
does not change the data: adding a vector field to a collection without one
lifted the cap while the rows stayed narrow (Section 2, "Conversion"). And
the per-segment costs of Section 4.1 depend on the rows of a segment, not on
whether its schema declares a vector.

What the cap costs a vector collection of narrow rows was measured on 24 M
rows with two layouts, sealed on bytes (3 segments, the largest about
11.5 M rows) against about 4 M rows per segment (7 segments); Appendix B.7:

| Index | Index build, total | Search |
|---|---|---|
| `HNSW`, `FLOAT_VECTOR` dim 16 | 493 s → 402 s | QPS 1,663 → 1,542; single-thread p50 2.36 → 2.61 ms |
| `IVF_FLAT`, `FLOAT_VECTOR` dim 16 | 9.7 s → 9.2 s | unchanged |
| `BIN_IVF_FLAT`, `BINARY_VECTOR` dim 512 | 96.5 s → 205.1 s | unchanged |
| `BIN_FLAT`, `BINARY_VECTOR` dim 512 | no build | single-thread p50 83 → 43 ms; QPS unchanged |

`BIN_IVF_FLAT` spends about 29 s per segment whatever its row count, so its
total build time grows with the number of segments. That is the largest
cost found.

Existing segments are not rewritten when the cap starts to apply to a
collection, on upgrade or when the value is lowered: a segment above
`maxRows` rows is full to the planners and no plan is generated to split
it (observed for 30 minutes on the earlier head `1fa020b44e`, by lowering
the cap of one collection). It is cut at
`maxRows` only when a compaction rewrites it for another reason: a manual
compaction, the delete ratio, expiry, clustering, or a schema or storage
version bump. New segments follow the cap from the start.

#### An absolute number of rows, set for the cluster

`maxRows` is an absolute number of rows. It does not follow `maxSize`, and
it is not set per collection. Both were built and measured before this was
settled, on a collection without a vector field holding three segments at
the cap (400 k) to which a BM25 and then a MinHash function field were
added (Section 6.3):

| Rule | After BM25 is added | Largest segment after the MinHash backfill |
|---|---|---|
| absolute `maxRows` for every collection (this design) | 400 k, 400 k, 400 k | 241 MB |
| `maxRows` scaled with the byte limit (rows per GB) | 800 k, 400 k after a manual compaction | 481 MB |
| `maxRows` only for collections without a required vector field | 1.2 M in one segment within 120 s, with no manual compaction | 722 MB |

The scaled cap doubled because a collection whose only vector field is
sparse uses `diskSegmentMaxSize` (2 GB) as its byte limit, an existing rule.
The conditional cap was lifted because the output field of a function cannot
be nullable. Only the absolute cap keeps the segments as they are when a
field is added, which is the property the cap needs across a conversion.

The price of an absolute number is the table above ("Which collections it
affects"): a deployment that has raised `maxSize` must raise `maxRows` in
proportion when it upgrades, or common vector dimensions fall under the
cap. A per-collection property was implemented and removed again: it adds
an interface and a propagation path to the StreamingNode for a value that
one configuration change can set, and a property can be added later without
breaking anything, while removing one cannot.

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

After `maxRows` is raised, in the configuration or by the collection
property, segments that were full under the old cap are merged once more,
the same as when an operator raises `maxSize`. Lowering it does not split
existing segments; they are cut when a compaction rewrites them.

## 5. Compatibility

- proto, etcd layout, binlog format and `DescribeCollection` output are
  unchanged.
- `common.enableCollectionWithoutVector` is a permanent switch (Section 2)
  with no plan to remove it. Rolling upgrade: turning it off before the
  control plane is upgraded is optional; a collection without a vector field
  created during the upgrade window is not compacted and not loadable until
  every role is upgraded, and works afterwards. After a downgrade such a
  collection can still be read and dropped, but is not compacted.
- Clients are described in Section 2 ("Clients"). Restore or
  CDC (Change Data Capture) into an old cluster fails at `CreateCollection`
  with the old error.
- The row cap applies to every collection. A collection whose rows are
  wider than its byte limit divided by `maxRows` (268 B with the default
  `maxSize`, 537 B with `diskSegmentMaxSize`, more when `maxSize` is
  raised; Section 4.2), which with the default configuration is a vector
  collection of the common dimensions, reaches the byte limit first and is
  sealed, compacted and imported as today. A deployment that has raised
  `maxSize` should raise `maxRows` in proportion when it upgrades. An existing collection of narrower rows, with or without a vector
  field (Section 4.2 lists the vector types), changes as follows after the
  upgrade: new segments are sealed and merged at `maxRows` rows; existing
  segments above `maxRows` are not rewritten, and are cut only when a
  compaction rewrites them for another reason. To keep the behaviour of
  today for the cluster, set `dataCoord.segment.maxRows` to 0; there is no
  per-collection setting.
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
  - the row cap is the configured value whatever the schema and whatever
    `maxSize`, and is disabled by 0;
  - no plan is generated to split segments above `maxRows` rows;
  - both mix planners produce no plan for segments holding `maxRows` rows
    and pack smaller segments up to at most `maxRows` rows;
  - the compaction writer cuts at `max_segment_rows`; the row limit of the
    StreamingNode seal; the clustering row limit; the conversion of the
    import byte target.
- End-to-end cases in the repository
  (`tests/python_client/milvus_client/test_milvus_client_collection_without_vector.py`,
  and the updated create case of the Go SDK suite):
  - create without a vector, load with no index, then query, get, count,
    delete and upsert; search returns the unified error;
  - with a scalar `INVERTED` index, partition key and clustering key;
  - `add_field` with a vector is rejected and with a scalar succeeds;
  - bulk import (`test_milvus_client_import.py`, JSON, CSV and parquet): the
    imported rows are returned unchanged after a load with no index, and a
    file that provides no field of the collection fails the job with no row
    visible.
- Covered by Go unit tests instead of an end-to-end case: REST v2 insert
  and search, and the switch turned off.
- Run with local scripts that are not in the repository, because they need
  millions of rows or a server restart (results in 6.1 and Appendix B):
  - bulk import of millions of rows followed by compaction;
  - after 10 M narrow rows are inserted (dynamic field on), compaction
    outputs stay at or below `maxRows` rows and the IDs of full segments do
    not change over the following automatic trigger cycles; an import of
    12 M narrow rows produces at least 3 segments; a low-dimensional vector
    collection is capped as well;
  - every role restarted with the switch off and on again (Appendix B.4).

### 6.1 Results

Run on the implementation head `2cc7740f6e` (Draft PR #54604), local x86
standalone, 2026-11-09 to 11-10. Appendix C has the details and what was
not rerun.

| Test | Result |
|---|---|
| Build | passes |
| Go unit tests, 17 groups of packages touched by the change | all pass. Two cases of `pkg/util/paramtable` fail on an unmodified master as well |
| Functional end-to-end script (create, insert, load without index, query, get, count, delete, upsert, search rejected) | passes |
| Compaction end-to-end script | passes |
| pytest, the 4 new cases | 4 passed |
| Row cap script, default `maxRows` 4 M | 8 of 8 checks pass |
| Row cap script, `maxRows` 400 k, row counts divided by 10 | 7 of 8 checks pass; the import check fails because of the reduced scale (Appendix B.6) |

Row cap script at the default `maxRows`:

| Check | Measured |
|---|---|
| 10 M rows inserted, dynamic field on | 3 segments of 4.00 M, 3.80 M, 2.20 M rows; `count(*)` is 10 M |
| 12 M rows imported from one parquet file | 3 segments of 5.60 M, 3.61 M, 2.80 M rows |
| Manual compaction of the imported collection | 4,000,000, 3.61 M, 2.80 M, 1.60 M rows: the segment above `maxRows` is cut at `maxRows` |
| 200 s of automatic trigger cycles | no segment changes |
| Vector collection, dim 8, 6 M rows | at `2cc7740f6e`: 1 segment of 6 M rows, not capped. With the cap applied to every collection (6.3): 3.80 M and 2.20 M rows |

Upstream CI has not run its build, unit test and e2e pipelines on #54604
yet; they do not start while the PR is a draft.

### 6.2 Conversion

Run on the #54604 head `ff26017cbc` (the three conversion commits), local
x86 standalone, 2026-11-10.

| Test | Result |
|---|---|
| Go unit tests of `internal/util/schemautil` and `internal/proxy/ddl` | pass. Nine cases that asserted the rejections now assert "allowed with the switch on, rejected with it off" |
| Go unit tests of `internal/rootcoord` | pass. A full run of the package used to fail 2 to 8 different `TestDDLCallbacks*` cases each time, without the conversion commits as well. The cause is the mock broadcaster of the tests, which gave two broadcasts in the same millisecond the same time tick, so the second ALTER was skipped as a replay (#55432); with the fix of #55433 the package passes in three runs out of three |
| Python e2e cases in the repository: `test_milvus_client_vector_field_conversion.py` (4 cases), the three changed cases of `test_drop_field_feature.py`, `test_milvus_client_collection_without_vector.py` | pass |
| RESTful `test_drop_field_operations.py`, with a new case that drops the only vector field and then checks that search and hybrid search return the error of a collection without a vector field | 8 passed |
| `test_add_function_field_feature.py::test_add_bm25_function_field_to_collection_without_vector`, against an instance with StorageV3 and both backfill compactions on, as in the e2e helm values | pass |
| C++ unit test `CApiTest.CPlanWithoutIndexMeta` | pass: a search plan on a collection with no index meta returns `FieldNotLoaded` |
| Local scripts on standalone, 28 cases | 27 pass; the one not run adds a BM25 function field on an instance without StorageV3 and is rejected by the existing rule; it is covered by the backfill cases below |
| Backfill of an added BM25 function field, with StorageV3 and both backfill compactions on; 4 cases, each run on a collection without a vector field and on a control with a `FLOAT_VECTOR` field | 8 of 8 pass, the same result on both collections at every step |
| Local scripts on a cluster (mixcoord, proxy, two QueryNodes, datanode, two StreamingNodes; Pulsar), 14 cases | pass: the 12 cases of the first three groups below, and the 2 replica cases |

The local scripts cover, per group:

| Group | Cases | What is checked |
|---|---|---|
| No vector → vector | 5 | released and loaded collections, an empty collection, two added fields with hybrid search, upsert of old rows; null on old rows, the load and search errors of Section 2, search results, compaction |
| Vector → no vector | 3 | released and loaded collections, two vector fields dropped one after the other; index removal, load with no index, the search error |
| Round trips | 4 | vector → none → vector under the same field name, none → vector → none without ever indexing, inserts running during both changes, and a control with a scalar field |
| No index at all, restart | 4 | a loaded collection with no index gains a vector field, is flushed and compacted; three converted collections are served again after a restart |
| Struct array | 4 | a struct field with a vector sub-field added and dropped, only the vector sub-field dropped, and the two shapes at creation |
| Struct array vector as the first vector | 3, each also on a control with a `FLOAT_VECTOR` field; run on a default instance and on one with StorageV3 | a struct field with a vector sub-field added to a loaded collection with no index at all; a vector sub-field added to a struct field that had only scalar sub-fields (element-level index and search over REST), then dropped; a struct vector added to a collection loaded with only a scalar index. Old rows read the struct, or its new sub-field, as null; search before the index is loaded returns "not loaded"; load without an index is rejected; after index and load, search returns only the rows that have the vector. Same result on both collections |
| Index build | 1 | the vector index finishes on segments written before the field existed |
| Row cap | 2 | both directions; rerun after the cap stopped depending on the schema (6.3), numbers in Section 2 |
| Switch off | 1 | create and every form of dropping the last vector are rejected; dropping one of two vector fields is allowed |
| Function field | 2 | dropping a BM25 function field that holds the only vector passes; adding one is covered by the backfill cases below |
| Import after a conversion | 2 | parquet import into a collection that gained a vector field (file with and without the vector column) and into one that lost its last vector field (file without and with the dropped column); each result equals that of a control collection created with the final schema; rows, null values and search results are checked |
| Clustering compaction | 2 | 1 M rows, the schema change sent 1.5 s after the trigger. The log shows the change inside the execution of the plan in both directions (drop: plan 09:29:20.1 to 24.6, change at 21.4; add: plan 09:30:20.1 to 22.9, change at 21.2). The plan completes, the row count is unchanged, and a second clustering compaction after the change completes |
| Two replicas (cluster only) | 2 | both conversions on a collection loaded with two replicas on two QueryNodes: repeated queries and searches return the same result or the same error, and the collection loads with two replicas again |

The row cap cases of this run recorded that adding a vector field lifted the
cap and the capped segments were merged on bytes. That was read as expected
at the time and is the gap closed in 6.3.

The struct array cases found one gap of the collection without a vector
field itself, not of the conversion: RESTful search and hybrid search
resolved a vector field to decode the request body, and on any collection
without a vector field failed with 1801 `can only accept json format
request, error: cannot find a vector field`, while gRPC returns 1100 `has no
vector field, search is not supported`. The RESTful handlers now return the
gRPC error (`30c5723c68`).

With inserts running during a change, the only client error is one rejected
insert after a drop, sent with the schema the client had cached; the control
that drops a scalar field shows the same.

The backfill cases write the same history into both collections: three
sealed segments, 100 deleted rows, 50 rows upserted with new text and 500
rows left in a growing segment, 3,400 live rows. After the function field
is added, a search for a word that every row contains must return exactly
the live rows, and three probes must hold: a deleted row is not returned,
an upserted row is found by its new text and not by its old text, and a row
from an old sealed segment and a row that was growing at the add are each
the top hit for their own word. 200 rows written after the add are
included.

| Case | Collection before the add | Result on both collections |
|---|---|---|
| Loaded | loaded (with no index for the collection without a vector field) | the collection stays loaded; the checks pass without reload and after reload |
| Round trip | the loaded collection above | dropping the function field returns the collection without a vector field to no vector field (search rejected, load with no index); adding it again backfills again and the checks pass |
| Released | never loaded | the checks pass on the first load |
| V2 segments | written while StorageV3 was off, then the instance restarted with it on | the segments are upgraded to V3 and the checks pass without reload and after reload |

`add_function_field` returns after every sealed segment holds the output:
the log shows the schema-bump compaction finishing before RootCoord
completes the change. On segments that are already V3 the 8 adds took 3.1
to 19.5 s; each returned at the next run of the schema-bump policy, every
20 s here.
On V2 segments the add waits for the storage-version upgrade, whose rate
limit (`rateLimitTokens` 3 per `rateLimitInterval` 120 s, for the whole
cluster) set the time: 157.7 s for the collection without a vector field,
whose second segment waited for the next window, against 17.5 s for the
control, whose segments had been upgraded in the earlier window.

### 6.3 Row cap for every collection

Run on the #54604 branch at `31def67692` (the binary was built from the
working tree with exactly this change, before it was committed), local x86
standalone, 2026-11-12, each part on a new instance.

| Test | Result |
|---|---|
| Go unit tests and `go vet` of `internal/datacoord/compaction/...`, `internal/datacoord/bulkimport`, the StreamingNode shard packages, `internal/proxy/ddl`, `internal/rootcoord/...`, `pkg/util/paramtable`, `pkg/common` | pass |
| `maxRows` 400 k: a vector field added to a collection without one and nothing written | three segments of 400 k stay three, before and after a manual compaction; 1.2 M more narrow rows give six segments of 400 k, unchanged over the next 120 s |
| `maxRows` 400 k: both conversion directions with dim 8 vectors | 11 of 11 checks pass, numbers in Section 2 |
| Row cap script, `maxRows` 400 k and default 4 M | 7 of 8 at both scales. The failing check is "the import leaves rows in at least three segments": three segments were allocated and the DataNode wrote the rows into two of them (8.39 M and 3.61 M at the default scale), which is the random pick of Section 4.2; the manual compaction that follows cuts them to 4.00 M, 4.00 M, 3.61 M, 0.39 M |
| StorageV3 instance, `maxRows` 400 k: BM25 backfill script (4 cases with controls) | pass |
| Default configuration: `test_milvus_client_vector_field_conversion.py`, `test_milvus_client_collection_without_vector.py`, and the existing `test_milvus_client_alter.py` and `test_milvus_client_compact.py` | 69 passed, 7 skipped by their own marks |

**Function fields added to capped segments.** A collection without a vector
field, 1.2 M rows of short text (11 B per row stored), `maxRows` 400 k,
StorageV3. A BM25 function field is added, then a MinHash one with 128
hashes (512 B per row). The same script was run under the three candidate
rules of Section 4.2; sizes are all objects of the segment, index files
included.

| Step | Absolute `maxRows`, every collection (this design) | Scaled with the byte limit | Only without a required vector field |
|---|---|---|---|
| before | 400 k × 3, 4.5 MB each | same | same |
| BM25 added (the DDL returns after 5 to 10 s) | 400 k × 3, 13.4 MB each, same segment IDs | same | same |
| 120 s later | 400 k × 3, 29 MB each | 400 k × 3 | **1.2 M × 1**, 87 MB |
| manual compaction | 400 k × 3 | **800 k, 400 k** | 1.2 M × 1 |
| MinHash added (18 s) | 400 k × 3, 234 MB each, same segment IDs | 800 k (468 MB), 400 k (234 MB) | 1.2 M × 1, 702 MB |
| 120 s later, then manual compaction | 400 k × 3, 241 MB each | 800 k (481 MB), 400 k (241 MB) | 1.2 M × 1, 722 MB |

In every run the backfill is in place (same segment IDs, same row counts),
the count is 1.2 M and a BM25 search returns hits. Bytes per row go from 11
to 34 with BM25 and to about 600 with MinHash.

**A backfilled segment above the byte limit.** Same script with `maxSize`
and `diskSegmentMaxSize` both at 128 MB (a collection whose only vector
field is sparse, or whose vector index is `MINHASH_LSH`, uses
`diskSegmentMaxSize`; with only `maxSize` lowered the plans carried 2 GB
and nothing was above the limit). After the MinHash backfill the four
segments hold 300 k rows and about 180 MB each. They are unchanged 120 s
later. A manual compaction splits each into about 220 k rows (133 MB) and
80 k rows (49 MB); the count stays 1.2 M.

**Runs on the earlier head `1fa020b44e`**, which had the same rule and, in
addition, a collection property that is removed in `31def67692`. They were
not repeated; the rows that exercised the property are left out.

| Scenario (`maxRows` 400 k) | Result |
|---|---|
| Several partitions | each partition is capped on its own: 400 k and 200 k per partition, none above the cap |
| 300 k rows upserted and 100 k deleted, then a compaction | no segment above the cap; count 900 k; the upserted and the deleted rows read correctly |
| Clustering compaction on a dim 8 vector collection | completes with 330 k, 330 k, 330 k and 10 k rows; a filter on the clustering key returns the right rows |
| Import of 1.2 M rows with dim 8 vectors | three segments allocated (436 k, 382 k, 382 k); all at or below 400 k after a manual compaction; count correct |
| `BINARY_VECTOR` dim 512, 1 M rows | 400 k, 376 k, 224 k: sealed at the cap; unchanged by a manual compaction |
| Cluster with two QueryNodes and two StreamingNodes: the reproduction, both conversion directions, a two-replica conversion | pass |

Not run: the external (Spark) backfill of a vector field; old and new
versions of a role side by side during a rolling upgrade; recall under the
two segment layouts; sparse vectors; a deployment with `maxSize` raised.

## 7. Alternatives

- **An explicit table type or proto flag.** Every client and tool would have
  to know it. The schema already carries whether a collection has vectors,
  and it changes with the schema when a collection is converted, so it is
  not needed.
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
- **Apply the row cap to collections without a vector field only** (an
  earlier revision of this document and of #54604), so that no vector
  collection changes behaviour. Dropped: the condition is read from the
  schema, and adding a vector field changes the schema but not the rows, so
  the cap was lifted from a collection that was still narrow and its capped
  segments were merged back (Section 2, "Conversion").
- **Apply it to collections without a required (non-nullable) vector
  field.** A plain vector field added later is nullable, so the cap would
  survive that. But the output field of a function cannot be nullable:
  adding a BM25 function field lifted the cap and three capped segments
  became one within 120 s (built and run, Section 6.3).
- **Scale the cap with the byte limit** (a number of rows per GB), so that
  the row width at which the two limits meet stays 268 B whatever `maxSize`
  is and no vector collection of a common dimension is affected on a
  deployment with a large `maxSize`. Dropped: the byte limit of a collection
  is not stable either. A collection whose only vector field is sparse uses
  `diskSegmentMaxSize`, so adding a BM25 function field doubled the cap and
  capped segments were merged (built and run, Section 6.3). A density is
  also harder to state and to check than a number of rows. The absolute cap
  leaves the adjustment for a large `maxSize` to the operator (Section 4.2).
- **A collection property that overrides the cap** (implemented and removed
  again). It gave one collection its own cap or none. Dropped for now: it is
  a public interface and a propagation path to the StreamingNode for a value
  the cluster configuration already sets, and it can be added later.
- **Decide by the data of each segment**: record the number of rows that
  hold a vector in the segment metadata, cap segments of rows without a
  vector, and size the others by bytes only. It affects no vector
  collection and needs no adjustment for `maxSize`. Not done: it adds a
  field to the segment metadata, a count at seal time on the StreamingNode,
  and a rule for segments that mix the two kinds of rows. It is the
  direction to take if the cost on vector collections of narrow rows turns
  out to matter.
- **No row cap.** Index build and load replay are serial per segment and
  grow with the row count (Section 4.1); the cap can be turned off with
  `maxRows=0`. The cap stays.

## 8. Follow-ups

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
- A backfill writes in place and does not split: a segment of a collection
  without a vector field can exceed the byte limit several times over after
  a function field or an externally backfilled vector is added (Section 2),
  and only a manual compaction splits it. Either the backfill should split
  an output above the byte limit, or a compaction policy should pick up
  segments above it. The external backfill of a vector into such a
  collection should be run end to end before it is relied on.
- The row cap on vector collections of narrow rows: recall at a fixed
  search parameter was not compared between the two segment layouts, sparse
  vectors were not measured, and each layout was run once on one machine
  (Appendix B.7).
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
| RootCoord `prepareAlterSchemaAddField` and the function-add path, `schemautil.ValidateSchemaEvolution`; Proxy `addCollectionFieldTask`, `validateAddStructFieldRequest` | a vector field, struct vector sub-field or vector-output function may be added | unchanged: also allowed on a collection without a vector field, under the existing rule that an added vector field is nullable (an earlier revision of #54604 rejected it) |
| Proxy `validateDropField`, `validateDropStructSubField`, drop struct field, `validateDropFunction`; RootCoord `buildSchemaForDropField`, `buildSchemaForDropFunctionField`; `schemautil.validateDroppedEvolutionField` | dropping the last vector field is rejected (eight checks with three messages) | rejected only when `common.enableCollectionWithoutVector` is off |
| segcore `plan_c.cpp` `CreateSearchPlanByExpr` | asserts that the collection has index meta; always true while a loaded collection had a vector index | Conversion: no index meta is reported as "field index is not loaded", the error of a vector field whose index is not loaded |
| QueryCoord `collection_readiness.go` `prepareAutoLoadRequest` (`proxy.enableAutoLoad`) | `ErrIndexNotFound` fails the automatic load | fail only if the schema has a vector field |
| `proxy/task.go:1297`, `:1337` drop struct field / function | `removed >= vectorCount` blocks the drop; always true at 0, so every struct-field and function drop is blocked | `removed > 0 &&`, as RootCoord already does |
| `datacoord/util.go:77` `FilterInIndexedSegments` (now in `datacoord/index/filter.go`) | with no vector the result depends on unrelated details (Section 3.2) | split into `handoffReady` / `compactionInputReady`; the handoff target fields are shared with the staging `PrepareReadiness` through `HandoffIndexTargetFields` |
| `datacoord/index_meta.go:1497` `AllDenseWithDiskIndex` (now in `datacoord/index/meta.go`) | true at 0 dense fields, so a scalar-only collection gets the 2 GB DiskANN target | returns false when there is no vector field at all; a sparse-only collection keeps today's result |
| `datanode/importv2` Parquet / NumPy / CSV / JSON readers | a file that matches no schema column makes the task loop forever: the reader produces no row and never reaches EOF; a vector field used to prevent this | rejected when the reader is constructed |
| StreamingNode `segment_limitation_policy.go`; DataCoord `compaction/trigger.go`, `trigger_v2.go`, `compaction/policy/helpers.go`, `clustering.go`, `bulkimport`; DataNode `compactor/segment_writer.go` | seal, full check, split and allocation on bytes only | every collection gets the additional `maxRows` row cap, from the cluster configuration `dataCoord.segment.maxRows` (Section 4.2); `GetExpectedSegmentSize` is unchanged |
| Search entries, REST handlers | four different errors | one error |
| pymilvus `orm/schema.py` `check_schema` (legacy ORM) | client-side reject | not changed; `MilvusClient` has no such check |
| milvus-sdk-node `milvus/utils/Validate.ts` `checkCollectionFieldsWithOptions`; `milvus/utils/Search.ts` `buildSearchRequest` | client-side reject of create and of search | create check removed; a search on a collection without a vector field is sent to the server; milvus-io/milvus-sdk-node#626 |

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
| Control: `int64 + int32 + FLOAT_VECTOR(dim 8)`, 6 M rows inserted, then a manual compaction | 1 segment of 6 M rows; a vector collection was not capped at this revision. With the cap applied to every collection: 3.80 M and 2.20 M rows (Appendix B.7) |

The collection of the first row has a dynamic field (JSON). Under the
schema-estimate conversion its estimated row size exceeds 268 B, the cap
does not apply, and the 10 M rows are merged into one segment.

An earlier revision of the implementation set the seal row limit to
`maxRows × seal proportion` (480 k rows). The same 10 M-row insert first
sealed into about 20 small segments, which compaction then merged into 4
segments of 2.25–3.0 M rows. That was dropped for the reason given in
Section 4.2.

The final runs on head `2cc7740f6e` (2026-11-10) ran the script at two
scales, once with the default `maxRows` of 4 M and once with `maxRows`
set to 400 k and every row count divided by 10.

| Scenario | `maxRows` 4 M | `maxRows` 400 k |
|---|---|---|
| Insert (10 M / 1 M rows), then flush and a manual compaction | 4.00 M, 3.80 M, 2.20 M rows | 400 k, 400 k, 200 k rows |
| Import from one parquet file (12 M / 1.2 M rows) | 3 segments allocated; 5.60 M, 3.61 M, 2.80 M rows | 3 segments allocated; all 1.2 M rows in one of them, the two empty ones are dropped |
| One manual compaction of the imported collection | 4,000,000, 3.61 M, 2.80 M, 1.60 M rows | 400,000, 400,000, 400,000 rows |
| 200 s of automatic trigger cycles afterwards | unchanged | unchanged |
| Control: vector collection, dim 8 (6 M / 600 k rows), at this revision | 1 segment of 6 M rows | 1 segment of 600 k rows |

In the 400 k import the file (1.2 M rows, 14.4 MB in memory, 12 B per row)
is smaller than one read batch of the import, so the whole file is one
hashed piece and `PickSegment` writes it into one of the three allocated
segments. The byte target after the conversion is 400 k × 12 B = 4.8 MB and
14.4 MB / 4.8 MB gives the three segments seen in the DataCoord log. The
segment count is right and the rows per segment are not, as Section 4.2
states for the import path; the following compaction cuts at `maxRows`.

### B.7 The row cap on narrow rows, with and without a vector field

Local x86 standalone at #54604 `3f82d4f328`, 2026-11-11, 24 M rows per run,
one run per configuration; other instances were running on the machine, so
differences below 10 % are within the noise. Index builds ran one segment at
a time on this instance. Scripts `seg_measure.py`, `run_all.sh`,
`run_more.sh`.

**No vector field**, `int64` primary key, a random `int64` and an `int32`
of 100 distinct values (about 28 B per row). The seal proportion was set to
1 so that a growing segment reaches `maxRows`; with the default 0.12 a
growing segment of these rows is sealed on bytes at about 4.2 M rows
(123 MB), and two such segments cannot be merged under a cap of 6 M.

| `maxRows` | Segment rows | `INVERTED` on the random `int64` | `INVERTED` on the `int32` | Load with indexes |
|---|---|---|---|---|
| 4 M | six of 3.75–3.90 M, one of 1.0 M | 10.6 s | 5.5 s | 1.25 s |
| 6 M | four of 5.75–6.00 M, one of 0.6 M | 13.6 s | 5.1 s | 1.24 s |
| 8 M | three of 7.25–7.75 M, one of 1.5 M | 16.1 s | 5.5 s | 1.44 s |
| 12 M | 11.80 M, 10.95 M, 1.25 M | 21.7 s | 6.1 s | 1.64 s |

Query latency (count by the `int32`, lookup of 20 primary keys) is 1.6 to
2.1 ms at p50 in every row. Reloading after 1 % of the rows were deleted
takes the same time as the load with indexes. The build time of the index
on the high-cardinality field grows steadily with the rows per segment and
has no step; the default stays at 4 M.

**Vector collections of narrow rows.** Layout X is the default
configuration, sealed and compacted on bytes. Layout Y has `maxSize` lowered
(353 and 355 MB) so that a full segment holds about 4 M rows, which is the
layout a row cap of 4 M gives; the row cap itself did not apply to vector
collections at this revision.

| | X | Y |
|---|---|---|
| `BINARY_VECTOR` dim 512, segment rows | 11.58 M, 11.12 M, 1.30 M | six of 3.59–4.04 M, one of 0.94 M |
| `BIN_IVF_FLAT` (nlist 1024, nprobe 16): build | 96.5 s (32.1, 28.6, 30.9 s per segment) | 205.1 s (27.8–29.3 s per segment) |
| single thread, nq 1, top 10: p50 / p99 | 2.87 / 8.07 ms | 2.62 / 4.12 ms |
| 16 threads | 927.5 QPS | 910.2 QPS |
| `BIN_FLAT`: single thread, nq 1, top 10: p50 / p99 | 83.25 / 157.82 ms | 43.02 / 89.84 ms |
| 16 threads | 27.7 QPS | 29.7 QPS |
| `FLOAT_VECTOR` dim 16, segment rows | 11.51 M, 11.11 M, 1.38 M | six of 3.62–4.01 M, one of 0.96 M |
| `HNSW` (M 16, efConstruction 100, ef 64): build | 493.3 s (229.7 and 236.7 s for the two large segments) | 401.6 s (59–69 s per segment of about 4 M) |
| single thread, nq 1, top 10: p50 / p99 | 2.36 / 3.78 ms | 2.61 / 3.88 ms |
| 16 threads | 1,663.3 QPS | 1,541.9 QPS |
| `IVF_FLAT` (nlist 1024, nprobe 16): build | 9.7 s | 9.2 s |
| single thread, nq 1, top 10: p50 / p99 | 3.28 / 8.66 ms | 2.87 / 4.21 ms |
| 16 threads | 885.9 QPS | 868.6 QPS |

Recall was not measured. With the row cap applied to every collection, the
vector collection of dim 8 of Appendix B.6 (6 M rows) is sealed into
segments of 3.80 M and 2.20 M rows, and 1 M rows of `BINARY_VECTOR` dim 512
under a cap of 400 k into 400 k, 376 k and 224 k (Section 6.3).

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
  cases cover (at this revision): the row cap applies to schemas without a
  vector field only and is not affected by a JSON field; both mix planners produce no plan for
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
- **Row cap end to end** (Appendix B.6), two scales. With the default
  `maxRows` of 4 M all 8 checks pass (run of 11-10; a run on 11-09 stopped
  in the eighth check because the data disk crossed the 0.90 hard watermark
  of woodpecker and writes were rejected). With `maxRows` 400 k and all row
  counts divided by 10, 7 of 8 checks pass. The one that fails is "import
  allocates at least rows / `maxRows` non-empty segments": the file is
  smaller than one read batch, which is a property of the reduced scale and
  of `PickSegment` (B.6), and the DataCoord log shows the expected three
  segments allocated.
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
