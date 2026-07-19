# Sealed-Segment Per-Operation Snapshot Pinning

- Status: Implemented on PR #51602
- Date: 2026-07-17
- Related: PR #51602 (operation snapshot pinning), PR #51531 (misc runtime-state migration), PR #51441 (stats skip index cell alignment), PR #51395 (PK state in runtime snapshot), master commit 158b1dc38a (text-index runtime-state migration)

## Motivation

`ChunkedSegmentSealedImpl` publishes all query-visible resources as one immutable
`PublishedSegmentState` (schema, load info, `RuntimeResourceState` with the column map,
indexes, skip-index metrics, readiness bitsets, row count, mmap-field markers, and
variable-field average-size estimates), swapped atomically by load / `Reopen` /
staged-commit publishers. Readers are lock-free: every accessor independently calls
`CapturePublishedState()` and works on whatever snapshot is current *at that call*.

That per-call capture is the residual correctness window:

- An expression caches layout-derived state for its whole lifetime
  (`SegmentExpr::InitSegmentExpr` caches `num_data_chunk_`; `CompareExpr` caches
  `left_num_chunk_` / `right_num_chunk_`; after #51441, `GetChunkSkipDecisions()`
  memoizes per-cell skip decisions from one `GetSkipIndex()` snapshot).
- Later per-batch calls (`chunk_size`, `num_rows_until_chunk`, `get_batch_views`,
  `chunk_data`, `ApplyFieldValidData`, `prefetch_chunks`) each re-capture the
  *current* published state.
- A concurrent `Reopen` (schema change, `fields_to_reload`, `column_groups_to_replace`,
  external-manifest advance) can publish a runtime whose columns have a different
  row-group/cell layout between those two reads. Coordinates computed against snapshot
  A are then applied to snapshot B: misaligned batch windows, wrong skip pruning,
  or out-of-range chunk ids. `Reopen` explicitly does not block readers
  (`reopen_mutex_` is writer-only), so the window is real by design.

#51441 already made *(skip metrics, column, cell layout)* inseparable inside one
snapshot (metrics are owned by the column group). What is still missing is the
guarantee that **one operation uses one snapshot for everything** — chunk counts, row
offsets, skip decisions, data views, validity, prefetch. This document specifies that
guarantee.

## Goal

Within one segcore operation on one sealed segment (a search, a retrieve, or any
expression evaluation spawned by them), every read of published state observes exactly
one `PublishedSegmentState` — the one current when the operation started. Writers stay
lock-free and unblocked; readers stay lock-free.

Non-goals:

- Growing segments. They have no published state; accessors read live mutable state
  (`insert_record_`, `deleted_record_`) and are correct by their own model.
- Deletes. `deleted_record_` is intentionally outside the snapshot: deletes are
  monotonic and timestamp-filtered, and must keep applying to in-flight reads.
- Cross-segment or cross-operation consistency. Each operation pins independently.

## Design

### Carrier: a type-erased slot on `milvus::OpContext`

`OpContext` (milvus-common, `common/OpContext.h`) is already created one-per-operation
per segment at the segcore C API boundary (`segment_c.cpp`: `AsyncSearch`,
`AsyncRetrieve`, `SegmentLoad`, `AsyncReopenSegment`) and threaded through roughly half
of the read accessors. It is the natural carrier for the pin. milvus-common must not
know segcore types, so the slot is type-erased:

```cpp
// milvus-common: common/OpContext.h
struct OpContext {
    ...
    // Pinned read snapshot for the segment this operation runs on.
    // Set once by the segment at operation entry; readers static_pointer_cast
    // it back. `pinned_state_owner` guards against an OpContext ever being
    // reused across segments: a mismatched owner means "do not use the pin".
    std::shared_ptr<const void> pinned_segment_state;
    const void* pinned_state_owner = nullptr;
};
```

This requires a milvus-common PR and a version bump in milvus; the slot is generic
("extensible operation metadata" is OpContext's charter) and adds no dependency.

### Pinning: eager, at the operation's semantic entry

```cpp
// ChunkedSegmentSealedImpl
void PinOpSnapshot(milvus::OpContext* op_ctx) const {
    if (op_ctx == nullptr) return;
    if (op_ctx->pinned_state_owner == this &&
        op_ctx->pinned_segment_state != nullptr) return;
    auto state = CapturePublishedState();          // one atomic load
    op_ctx->pinned_segment_state = state;          // shared_ptr<const void>
    op_ctx->pinned_state_owner = this;
}
```

The query visitor creates the per-segment `OpContext` and calls `PinOpSnapshot()`
before deriving `active_count` or constructing the plan fragment. This is after schema
preparation at the C API / segment entry and before any layout-derived expression
state is created. Once the `QueryContext` exists, it is the exec/query layer's single
source of the operation context: operators and expressions obtain
`query_context->get_op_context()` and pass only the resulting `OpContext*` across the
lower-level segcore accessor boundary.
Offset-driven `RetrieveByOffsets` is a separate sealed-segment read operation and pins
at its own entry.

Ordinary Retrieve has two internal phases: plan execution computes offsets, then the
caller performs output-size estimation and materializes target fields (including
deferred ORDER BY fields). The visitor therefore copies its pin into `RetrieveResult`,
and `SegmentInternalInterface::Retrieve()` reconstructs the fill `OpContext` from that
exact pin. **Re-pinning between these phases is forbidden**: filter coordinates from
snapshot A must never be applied to columns or schema from snapshot B.

Writers never pin. If a writer that shares an op_ctx with a subsequent read path ever
publishes, it must clear the pin (`pinned_segment_state.reset()`) before that later read
selects its operation snapshot.

Eager (not pin-on-first-capture) because it makes the snapshot's birthline auditable —
"the operation sees the world as of Search() entry" — and avoids ordering surprises
between reader and writer calls earlier in the same task.

### Capture: one choke point honors the pin

```cpp
std::shared_ptr<const PublishedSegmentState>
CapturePublishedState(milvus::OpContext* op_ctx = nullptr) const {
    if (op_ctx != nullptr && op_ctx->pinned_state_owner == this &&
        op_ctx->pinned_segment_state != nullptr) {
        return std::static_pointer_cast<const PublishedSegmentState>(
            op_ctx->pinned_segment_state);
    }
    return std::atomic_load(&published_state_);
}
```

`CaptureRuntimeResourceState`, `CaptureSchemaSnapshot`, `CaptureLoadInfoSnapshot`,
`get_column`, `GetSkipIndex` gain the same optional `op_ctx` parameter and forward it.
With no op_ctx (tests, tools, writer internals) behavior is unchanged.

### Plumbing: give the layout-coordinate accessors an op_ctx

Accessors already carrying `OpContext*` (data/validity/prefetch/index pins:
`get_batch_views`, `get_views_by_offsets`, `chunk_data`, `chunk_view`,
`ApplyFieldValidData*`, `prefetch_chunks`, `PinIndex`, `GetTextIndex`, `GetJsonStats`,
`GetNgramIndex*`, `bulk_subscript` overloads) only change internally: their
`CapturePublishedState()` becomes `CapturePublishedState(op_ctx)`.

Accessors that read published state but have no op_ctx today gain
`milvus::OpContext* op_ctx = nullptr` (appended, defaulted — source-compatible):

| Accessor | Used by |
| --- | --- |
| `num_chunk_data`, `num_chunk` | `SegmentExpr::InitSegmentExpr`, `CompareExpr` ctor |
| `chunk_size`, `size_per_chunk` | every `ProcessDataChunks` batch loop and chunk-reader cursor |
| `num_rows_until_chunk`, `get_chunk_by_offset` | scan offsets, `SegmentChunkReader::MoveCursor*` |
| `GetSkipIndex` | `GetChunkSkipDecisions`, `PrefetchRawData` |
| `HasFieldData`, `HasIndex`, `HasJsonIndex`, `IndexHasRawData`, `HasRawData` | exec-path selection |
| `get_schema`, `is_nullable`, `is_field_exist`, `GetFieldDataType` | expression init |
| `GetArrayOffsets`, `get_max_timestamp` | struct-array exprs, ts pruning |
| `get_active_count`, `get_row_count` | query range and chunk-reader cursor bounds |
| `get_field_avg_size`, `is_mmap_field` | Retrieve size guard and field-state probes |
| `find_first_n`, `find_first_n_element` | bitmap-to-offset conversion and PK ordering |

Call-site sweep: `SegmentExpr` (`Expr.h`), the four `PrefetchRawData` overrides,
`SegmentChunkReader`, `CompareExpr`, and the query paths under `query/` that mix
coordinates with data reads. All of these already hold `op_ctx_`; the sweep is
mechanical parameter-passing with no behavior change when unpinned.

### What this buys

- The #51441 memoized skip decisions, the chunk count cached at expression
  construction, every `chunk_size`/offset computation, every data/validity view, and
  the prefetch filter all come from one `PublishedSegmentState`. The reviewer's
  "decisions from A applied to B's layout" scenario becomes unrepresentable.
- Readers get *faster*: one atomic shared_ptr load per operation instead of one per
  accessor call.
- Writers are untouched: publish/Reopen semantics, staged commit, rollback — all
  unchanged. A mid-operation publish simply takes effect for the next operation.

## Completeness: why one pointer is enough

Add-field / drop-field (and every other Reopen flavor) can only affect resources
that live inside `PublishedSegmentState`, because the preceding migrations
deliberately moved them there: the schema itself (`PublishedSegmentState::schema`),
columns (`runtime->fields`), scalar/vector/text/json/ngram indexes
(158b1dc38a), readiness bitsets, PK index and offset maps (#51395), timestamps
and their index, skip metrics (#51441), row count, mmap-field markers,
variable-field average-size estimates, and the load info. Pinning the root pointer
therefore freezes all of them as one version — there is no second carrier to add to
OpContext.

Deliberately outside the snapshot, and correctly so: `deleted_record_` (deletes
are monotonic and timestamp-filtered; they must keep applying to in-flight
reads), growing-segment mutable records (growing has no Reopen), and
non-query-semantic load progress / statistics bookkeeping.

### Write-side proof

The completeness claim is best checked from the write side: what does an
add-field / drop-field Reopen actually mutate? It builds a staged
`PublishedSegmentState` (`DropFieldFromState` / `DropFieldData` operate on a
staged copy, never the live one) and atomically swaps the `shared_ptr`. It
writes no segment member. The members that reads reach directly (bypassing
`Capture*`) are all load-immutable:

| Member | Only write site | Touched by add/drop-field? |
| --- | --- | --- |
| `col_index_meta_` | ctor init-list only (:4501), zero reassignment | No — immutable |
| `is_sorted_by_pk_` | ctor init-list only (:4502), zero reassignment | No — immutable |
| `schema_` | no raw member; schema lives in `PublishedSegmentState` | In the snapshot |

So pinning one pointer captures 100% of what a schema-change Reopen can alter;
everything query-semantic read outside the snapshot is written once and never touched
by Reopen. These immutable members get an immutability comment (so a future edit
that adds a Reopen write to them cannot silently break the pin) but need no
migration.

Two further consequences worth naming:

- `get_schema()` returns `const Schema&` into the published state; today two
  quick republishes can theoretically free the schema under a long-lived
  reference. Resolving it against the pinned snapshot removes that hazard.
- The guarantee holds only for reads that go through `Capture*`. The
  implementation includes the bypass audit above; the four immutable-member
  reads are documented as intentionally live, not migrated.

## Lifetime and memory

The pin holds the old snapshot (columns, cache slots, metas) alive for the operation's
duration — the same ownership model the #51441 `SkipIndex` view and every `PinWrapper`
already use, extended from "one call" to "one operation". Cost is bounded by operation
latency. Old cells remain evictable; re-fetch goes through the old column's translator
against the old files. That requires the existing operational invariant that file GC
grants a grace period covering in-flight operations — pinning does not lengthen the
window beyond the operation lifetimes that PinWrapper already imposes.

## Semantics changes

An operation no longer observes resources published mid-flight (e.g. an interim index
becoming ready between two batches). Today whether it observes them is a race; after
pinning it deterministically does not. This is strictly more predictable and matches
snapshot-isolation intuition.

## Testing

1. **Reopen race regression** (the test the pinning makes writable): load a storage-v2
   segment with the skip-index flag on, start an expression eval, use the #51395
   staged-commit test hooks (`TestStageLoadFieldDataThenPublish`) to republish a
   runtime with a different cell layout mid-scan, assert exact query results and no
   OOB — the eval must complete entirely on its pinned snapshot.
2. **Pin identity**: within one op_ctx, assert every `Capture*` returns the same
   snapshot pointer while a concurrent publisher swaps state.
3. **Unpinned compatibility**: null op_ctx keeps today's behavior (existing suites).
4. **Owner guard**: an op_ctx pinned by segment X, passed to segment Y, must not
   return X's state.
5. **#51594 raw-index transition**: pin while vector output routes to the raw
   column, publish a state that advertises index raw data and removes the column,
   then verify `bulk_subscript` still returns exact vectors from the pinned column.
   The reverse raw-index to raw-column transition is covered by the same invariant:
   `IndexHasRawData`, `get_vector`, and `get_raw_data` all resolve the same pin.

## Alternatives considered

- **Per-expression pinned column handle** (the follow-up sketched in #51441): fixes
  only `SegmentExpr`, needs new view/dispatch APIs on `ChunkedColumnInterface` or
  column-taking overloads of every segment view method, and leaves `CompareExpr`,
  `SegmentChunkReader`, and retrieve incoherent. More churn, less coverage.
- **Pin-on-first-capture (lazy)**: transparent but makes the snapshot's birth depend
  on incidental call order (and `LazyCheckSchema` would pin pre-upgrade state).
  Rejected for auditability.
- **Thread-local pin with RAII guard**: no signature changes, but segcore operations
  hop threads under folly executors; thread identity is not operation identity.
- **Read locks around operations**: rejected outright — blocks Reopen behind slow
  scans and violates the lock-free-reader rule this subsystem is built on.

## Rollout

1. milvus-common: add the two OpContext fields (independent, backward compatible).
2. milvus: bump milvus-common; add `PinOpSnapshot` + op_ctx-aware `Capture*`; pin in
   the per-segment query visitor and offset-driven Retrieve entry.
3. milvus: mechanical op_ctx sweep through the accessor table above and the exec
   call sites; add the tests.

Steps 2–3 can be one PR; the behavior flips on with the `PinOpSnapshot` calls and is
trivially revertible by removing them.

## Implementation status (2026-07-18, gaps closed)

The implementation lives on branch `feat/sealed-op-snapshot-pinning`. The five
spec-review bypasses are fixed, a follow-up sweep closed the remaining read-path
bypasses, and the Reopen-race regression test is in place. One open question
remains a product decision (does not gate correctness):

1. **Is the race reachable in production?** The common Reopen (add/drop field)
   carries existing columns over unchanged (`CloneRuntimeResourceState`:
   `state->fields = current->fields`), so a query reading field F is unaffected
   by schema changes to other fields — zero mismatch. The residual window only
   opens when the *queried* field's column is replaced mid-query via
   `fields_to_reload` (index raw-data transition), `column_groups_to_replace`,
   or v3 manifest advance (compaction) — AND only if those paths are not already
   drained against in-flight queries by the query scheduler. No reproduction has
   been demonstrated; it is a logical-mismatch (wrong rows / OOB assert), NOT a
   use-after-free (the read snapshot is held alive per accessor call).

2. **The first implementation only partially achieved the pin** (spec review) —
   now RESOLVED. The five bypasses below have been fixed so every read path
   routes through the operation's pinned op_ctx:
     a. `RetrieveByOffsets` (SegmentInterface.cpp) — `TryTakeForRetrieve` now
        calls `CapturePublishedState(op_ctx)`, and `FillTargetEntry`'s slow-path
        `local_ctx` copies the pin fields (`pinned_segment_state` +
        `pinned_state_owner`) so its `bulk_subscript` / `is_field_exist` reads
        resolve the pin. The main `Retrieve` passes the visitor's original pin
        through `RetrieveResult` into size estimation and materialization; it
        never re-pins between filter and fill.
     b. `CompareExpr::CanUseBothDataFastPath` — `num_chunk_data` /
        `num_rows_until_chunk` now pass `op_ctx_`.
     c. `MatchExpr::Eval` — `get_schema(op_ctx_)`.
     d. `GISFunctionFilterExpr::EvalForIndexSegment` — the fresh local op_ctx is
        gone; `bulk_subscript` uses `op_ctx_`.
     e. `SegmentChunkReader::GetIndexAndBaseOffset` — gained an `op_ctx` param;
        callers pass the reader's `op_ctx_`.

   A follow-up bypass sweep also routed these read-path accessors through the
   pin: `vector_search`, `get_vector`, `get_emb_list`, `get_raw_data`,
   `CalcDistByIDs`, `pk_range`, `pk_binary_range`, `prefetch_vector`,
   `IsIndexRefineEnabled`, `bulk_subscript_text_impl`, `TryTakeForSearch`, the
   sealed search-result `FillTargetEntry` / `FillPrimaryKeys`, `fill_with_empty`
   (gained an op_ctx param; its `CaptureSchemaSnapshot` was the last live-schema
   read reachable from a pinned `bulk_subscript`), and the exec-path
   constructors `PhyColumnExpr` / `PhyUnaryRangeFilterExpr` plus
   `ReorderConjunctExpr` and `CreateTTLFieldFilterExpression`.

   Intentionally left unpinned (documented): cross-segment / cross-operation
   reduce+export paths (`search_result_export_c.cpp`, `Reduce.cpp`) — their
   op_ctx is shared across segments so the owner guard rejects any pin, matching
   the "each operation pins independently" non-goal; `GetJsonFlatIndexNestedPath`
   (pre-pin exec-path metadata probe, boolean decision only); standalone PK-lookup /
   delete accessors (`Contain` and delete ingestion) that are off the pinned
   search/retrieve read path; and the load-immutable member reads
   (`col_index_meta_`, `is_sorted_by_pk_`, and load/publish/reopen internals)
   which never pin by design.

3. **The useful sealed-state consolidation from #51531 is absorbed** — row count,
   mmap-field markers, and variable-field average-size estimates now live in
   `RuntimeResourceState` and follow the same clone/freeze/publish COW lifecycle as
   columns and indexes. The duplicated live sealed-segment containers were removed.
   Dropping raw field data clears its mmap marker but preserves its average-size
   estimate while the schema still contains the field, because an index with raw data
   may continue to serve Retrieve after raw-column reclamation. The estimate is erased
   only when the field is actually removed from the schema.

Regression test: `test_sealed.cpp` adds `SealedSegmentOpSnapshotPin.*` —
`PinnedScanSurvivesMidScanFieldDrop` (load scalar F, pin, read F, `Reopen` to a
schema that drops F mid-scan, then re-read F through the pin and get exact rows;
the fault-injection witness is `num_chunk_data(F, nullptr)==0` vs
`num_chunk_data(F, &pinned)==1`, and the unpinned RawData read would abort at
`AssertInfo(field_data_ready_bitset[F])` in `chunk_data_impl`),
`PinIdentityAcrossConcurrentPublish` (every `Capture*` on one op_ctx returns the
same snapshot pointer across a concurrent publish), and `OwnerGuardRejectsForeignPin`
(segment X's pin, consulted by segment Y, returns Y's live state).
`PinnedVectorRetrieveSurvivesRawIndexTransition` directly exercises #51594's
no-raw-index to raw-index transition: the live state drops the vector column and
advertises index raw data after the operation pins, while pinned `bulk_subscript`
continues to return the exact vectors from its original column snapshot.

milvus-common with the two OpContext fields is landed and `conanfile.py` is
bumped to `milvus-common/1.0.0-6b16d93`. Note: the package bump also changed
`cachinglayer::LoadingOverheadConfig`'s shape, so `HybridScalarIndexTest.cpp`
was adapted to the new `memory`/`file` dimension members.
