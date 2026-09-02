# MEP: Shared Filter Execution for Hybrid Search

- **Created:** 2026-09-02
- **Author(s):** @zhengbuqian
- **Approver(s):** @czs007
- **Status:** Draft — P0 implemented, untested
- **Component:** QueryNode (Delegator, SearchTask), Segcore
- **Related Issues:** TBD
- **Released:** TBD

## Summary

A hybrid search (`HybridSearch`, internally a `SearchRequest` with N `SubSearchRequest`s) executes each
sub-request as a fully independent search: an independent plan, an independent worker RPC, and an
independent segcore call per segment. When two or more sub-requests carry an **identical filter
predicate**, the same filter bitset is therefore computed once per sub-request per segment, and the
same MVCC/delete mask is applied just as many times. A dense path plus a BM25 path over the same
filtered rows — the ordinary shape of a hybrid search — is exactly this case.

This design makes sub-requests that share a predicate execute the shared portion of the plan **once
per segment**. The shared portion is exactly the source subtree of `VectorSearchNode`
(`FilterBitsNode → MvccNode → [ElementFilterBitsNode]`); the per-branch portion begins at
`VectorSearchNode`. Grouping is decided deterministically at the shard delegator, carried to the
worker as one RPC, and executed in segcore as one prefix call per segment followed by N concurrent
per-branch vector searches against its result.

Sub-request plans are **not** merged. Each branch keeps its own complete `query::Plan`. Only execution
is shared. This keeps `plan.proto` and the proxy untouched and lets the existing per-branch reduce
pipeline be reused verbatim.

## Non-Goals

- **Sharing across sub-requests whose predicates merely share a prefix**, i.e. the shape
  `[0] == [1] == C` and `[2] == (C) && extra`. Exploiting that needs expression-level common subtree
  extraction and delta bitsets; it is deferred (see Delivery Phases).
- **Sharing across separate top-level requests.** That is the cross-request caching problem, already
  served by `ExprResCacheManager`; see Rejected Alternatives for why it does not address this one.
- **Changing the reduce or rank-fusion path.** Each branch continues to reduce to its own top-k and the
  proxy continues to fuse afterwards.
- **The iterative-filter execution path.** It has no `FilterBitsNode`; the filter is applied row-by-row
  after the vector search. Requests on that path fall back to today's behavior.

## Public Interfaces

### Proto changes

`pkg/proto/query_coord.proto`:

```protobuf
message SearchRequest {
    internal.SearchRequest req = 1;
    repeated string dml_channels = 2;
    repeated int64 segmentIDs = 3;
    bool from_shard_leader = 4;
    DataScope scope = 5;
    int32 total_channel_num = 6;
    bool filter_only = 7;
    bool enable_expr_cache = 8;

    // Additional branches that share the filter predicate carried in `req`
    // and must be executed together with it in one per-segment segcore call.
    // Branch 0 always lives in `req` itself, fully populated exactly as it is
    // today. Empty means an ordinary single-branch search.
    repeated internal.SubSearchRequest extra_filter_sharing_reqs = 9;
}
```

No change to `pkg/proto/plan.proto`.

#### Why branch 0 stays in `req`

`internal.SearchRequest` does double duty: it is both the request envelope (`collectionID`,
`mvcc_timestamp`, `guarantee_timestamp`, `timeout_timestamp`, `consistency_level`,
`collection_ttl_timestamps`, `entity_ttl_physical_time`, `output_fields_id`, `username`, `base`) **and**
the payload of one branch (`serialized_expr_plan`, `placeholder_group`, `dsl`, `dsl_type`,
`partitionIDs`, `nq`, `topk`, `metricType`, `ignoreGrowing`, `offset`, `group_by_field_id`, `group_size`,
`field_id`, `analyzer_name`, `search_type`). The two halves are not interchangeable, so a `oneof` between
`req` and the branch list is not possible — it would discard the envelope.

Two shapes were considered for resolving the double duty:

1. **Always-populated branch list** — move every per-branch field out of `req` into a `repeated`
   field that holds exactly one entry for an ordinary search. One source of truth, and grouped mode stops
   being a special case. Rejected: it redirects roughly sixty `req.Req.<per-branch field>` read sites
   across `SearchTask`, `services.go`/`handlers.go`, `optimizers`, and `segcore.NewSearchRequest`, all on
   the regular non-hybrid path, for a purely structural benefit.
2. **Additive: branch 0 in `req`, extras in the repeated field** — chosen. `len(extra) == 0` is not
   "the non-grouped mode", it is the degenerate case of the same rule, so the regular path needs no new
   conditional at all. There is no redundancy between `req` and the list, hence no drift hazard.

The cost of the chosen shape is asymmetry: branch 0 is addressed differently from branches 1..N-1, and
every piece of grouped-aware code must reconstruct the full branch list as `[req] + extra`. That is
accepted deliberately in exchange for zero intrusion into the regular search path.

Note that grouping is entirely internal to `sd.Search`: it still returns one `internalpb.SearchResults`
per sub-request, so `searchChannel` and everything above it (`handlers.go:370,412-413,433-434`, which
read the *top-level* hybrid request's nq/topk anyway) are unaffected by this change.

#### Results

**Results reuse the existing `internalpb.SubSearchResults`** (`pkg/proto/internal.proto:175`), which
already carries a `req_index` field for exactly this purpose. Today the delegator splits sub-requests
before the worker RPC, so workers never populate `SubResults`; in this design a grouped worker response
fills one `SubSearchResults` per branch — **including branch 0** — and the delegator demultiplexes on
`req_index`.

The response is symmetric (all N branches in `SubResults`) even though the request is asymmetric. Each
side is shaped by its own constraint: the request must not disturb existing readers of `req`, while a
grouped response has no existing readers at all, so it can take the clean form.

### New cgo API

`internal/core/src/segcore/segment_c.h`:

The interface is **two-phase**: one call evaluates the shared prefix and hands back an opaque handle, and
one call per branch performs that branch's vector search against it.

```c
/** Opaque owner of one segment's shared prefix output: the post-MVCC bitset
 *  plus the derived QueryContext state the prefix produced. */
typedef void* CSharedFilterBitsetResult;

/**
 * Evaluate the shared prefix (VectorSearchNode's source subtree) of `c_plan`
 * on `c_segment`. Any branch's plan may be passed; they are equivalent by the
 * grouping precondition. No placeholder group is needed.
 */
CFuture*  // Future<CSharedFilterBitsetResult>
AsyncComputeFilterBitset(CTraceContext c_trace,
                       CSegmentInterface c_segment,
                       CSearchPlan c_plan,
                       uint64_t timestamp,
                       int32_t consistency_level,
                       uint64_t collection_ttl,
                       uint64_t entity_ttl_physical_time_us);

void DeleteSharedFilterBitsetResult(CSharedFilterBitsetResult c_bits);

/**
 * Run one branch's VectorSearchNode (and everything above it) against a
 * previously computed shared prefix, skipping FilterBitsNode / MvccNode.
 * Returns exactly what AsyncSearch returns: a single leaked SearchResult*.
 */
CFuture*  // Future<CSearchResult>
AsyncSearchWithBitset(CTraceContext c_trace,
                    CSegmentInterface c_segment,
                    CSearchPlan c_plan,
                    CPlaceholderGroup c_placeholder_group,
                    CSharedFilterBitsetResult c_bits,
                    uint64_t timestamp,
                    int32_t consistency_level,
                    uint64_t collection_ttl,
                    uint64_t entity_ttl_physical_time_us);
```

`AsyncComputeFilterBitset` forwards `enable_expr_cache` unchanged. Whether this bitset comes from
actually evaluating the filter or from `ExprResCacheManager` is orthogonal to sharing it across
branches, so a cached bitset is reused here exactly as it would be in an ordinary search.

Neither call takes `filter_only`. That flag selects a different *output shape* — run the filter and
return only a per-segment matched-row count, discarding the bitset — and it is already served by the
existing `AsyncSearch` path. Two-stage search's stage 1 keeps using it, and on a grouped request it
runs **once for the whole group** rather than once per sub-request, because every branch carries the
same predicate. See Q2.

**Why two phases rather than one `AsyncSearchGrouped` returning N results.** Two independent reasons,
each sufficient on its own:

1. **Branch parallelism without a self-submission deadlock.** Branches must run concurrently (Q6). A
   single grouped call would have to fan its branches out from inside C++, and the only natural target is
   `getSearchCPUExecutor` (`internal/core/src/futures/Executor.cpp`, `CPU_NUM` threads) — the very pool
   the grouped job is already running on. A job that submits to its own bounded pool and then blocks
   deadlocks once every thread holds such a waiter. Avoiding it would mean introducing and sizing a
   second executor. Splitting the call instead puts the fan-out back in Go, where
   `searchSegmentsAttempt` already runs an `errgroup`.
2. **Unchanged result ownership.** `AsyncSearchWithBitset` returns a single leaked `SearchResult*`, exactly
   like `AsyncSearch` today; Go wraps it and releases it through the existing `DeleteSearchResults`. No
   result-array type, no new destructor semantics, and no partial-release question when construction
   fails midway through a batch.

The handle's lifetime is a single Go function scope guarded by one `defer`, covering one segment and one
group. It is a local resource, not the process-wide registry rejected under Rejected Alternatives: no
map, no refcounting, no cross-request sharing, and the `defer` survives panics and cancellation.

Cost: N+1 cgo calls per segment instead of 1, and the read lease plus schema validation are taken N times
rather than once. Both are negligible against one filter evaluation plus one ANN search, and the lease
count is no worse than today, where each of the N sub-requests takes its own.

### New configuration parameter

```yaml
queryNode:
  hybridSearch:
    sharedFilter:
      enabled: false   # default off; flip on per-cluster during rollout
```

`ParamItem` in `pkg/util/paramtable/component_param.go`, `refreshable: true`. The switch is read at the
delegator grouping site, so turning it off restores today's behavior exactly, with no partially-applied
state.

### New metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `milvus_querynode_hybrid_shared_filter_fallback_total` | Counter | node_id, collection_id, reason | A sub-request that could not join a group, by why. `reason` ∈ {`no_matching_peer`, `no_predicate`, `iterative_filter`, `plan_unmarshal_failed`}. |

The existing segcore histograms are the primary effect measurement and need no change:
`internal_core_search_latency_scalar` (`FilterBitsNode.cpp:230,295`) should drop roughly in proportion
to the sharing factor, while `internal_core_search_latency_vector` (`VectorSearchNode.cpp:211`) stays
flat.

## Design Details

### Architecture

The enabling structural fact is that a vector search plan is a **linear chain with exactly one fork
point**:

```
FilterBitsNode(doc_expr) → MvccNode → [ElementFilterBitsNode] → VectorSearchNode → [SearchGroupByNode]
└──────────────── shared prefix ─────────────────────────────┘ └──────── per branch ────────┘
```

The boundary is not a new concept that has to be invented: it is exactly what
`ProtoParser::ExtractFilterOnlyPlan` (`internal/core/src/query/PlanProto.cpp:1609`) already returns —
the `sources()[0]` of `VectorSearchNode`. Two-stage search already uses it to run just the filter and
discard everything else (`ExecPlanNodeVisitor.cpp:394-465`). This design runs the same subtree and
feeds the result back into N vector searches instead of discarding it.

Because the boundary is defined structurally rather than by expression type, element-level hybrid
search comes along for free: when `ElementFilterBitsNode` is present it sits below `VectorSearchNode`
and is therefore inside the shared prefix.

Data flow end to end:

```
proxy         unchanged: N sub-requests, each with its own complete plan
  │
delegator     group sub-requests by predicate bytes; each group of size >= 2
  │           becomes ONE querypb.SearchRequest: branch 0 in `req`, rest in extra_filter_sharing_reqs
  │
  ▼           (1 worker RPC instead of N)
worker        one SearchTask holding N branches = [req] + extra_filter_sharing_reqs
  │
  ▼           per segment: 1 prefix call, then N branch calls run concurrently
segcore       AsyncComputeFilterBitset  ->  shared prefix handle (bitset + state)
              AsyncSearchWithBitset x N, each against that handle
  │
  ▼           N SearchResult per segment
worker        transpose to [branch][segment]; run the existing reduce per branch
  │           emit SubResults[branch] with req_index
  ▼
delegator     demux on req_index; ReduceSearchOnQueryNode per branch (unchanged)
  │
proxy         per-branch reduce + rank fusion (unchanged)
```

### 1. Delegator: grouping

The delegator is the only correct place for this decision:

- **Not the proxy** — it does not know the segment distribution, does not own the MVCC pin, and would
  have to guess whether sharing is even possible.
- **Not the worker scheduler** — that would mean relying on two independent RPCs landing in the same
  scheduling window. It usually would, but "usually" is not a design. The existing `SearchTask.Merge`
  is also a merge along the **NQ axis** (same plan, concatenated placeholder groups, one reduce, then
  sliced back apart). Sharing a filter across different vector fields is an orthogonal axis; overloading
  one mechanism with both would be fragile.
- **The delegator** — it is the first component that sees all sub-requests at once, and
  `PinReadableSegments` (`delegator.go:617`) has already fixed a single segment snapshot and a single
  MVCC timestamp for all of them before the fan-out.

In the `IsAdvanced` branch (`delegator.go:624`):

```go
groups := groupSubReqsBySharedFilter(req.GetReq().GetSubReqs())
// len(group) == 1 -> existing single-branch path, unchanged
// len(group) >= 2 -> grouped path
```

**What sharing actually requires.** The bitset for a segment is a pure function of
`(segment, predicate, mvcc_timestamp, TTL context)`. Nothing else enters into it — in particular
`PartitionIDs` and `IgnoreGrowing` do **not**. They only decide *which* segments are searched. So for any
segment that two branches both search, their bitsets are identical by construction, whatever those two
fields say.

`IgnoreGrowing` nonetheless appears in the grouping key below, and it is worth being precise about why:
a group is packaged as **one RPC carrying one segment list**. That packaging cannot express two branches
disagreeing on the segment set. The constraint comes from the chosen packaging, not from the semantics
of sharing — which is what makes the P1 refinement below possible.

**Grouping key:**

| Field | Why it is in the key | Can it actually differ? |
|---|---|---|
| `vector_anns.predicates` (serialized bytes) | Determines the bitset | Yes — the real discriminator |
| `iterative_filter_execution == false` | The iterative path has no `FilterBitsNode` to share | Yes |
| `IgnoreGrowing` | Changes the growing-segment set, so the group's single segment list would be wrong | **Yes** — settable per sub-request |


`IgnoreGrowing` is settable per sub-request: `task_search.go:623-628` ORs the request-level flag with
`isIgnoreGrowing(subReq.GetSearchParams())`. It is the only field in the table that can genuinely split
a group that would otherwise share a predicate. It stays in the key because it is a **correctness**
constraint, not an optimization one: if one branch wants growing segments and another does not,
executing them under one flag gives one of them the wrong row set. Expected to be rare — it requires a
caller to set it on some sub-requests but not others — but that expectation is **not measured**.

**`PartitionIDs` is deliberately not in the key.** It cannot differ once the predicates match, and it
has no effect on this path anyway:

1. Outside partition-key mode every sub-request gets the same `t.GetPartitionIDs()`
   (`internal/proxy/task_search.go`).
2. In partition-key mode it is derived from the plan's partition-key predicate by
   `tryParsePartitionIDsFromPlan`, so byte-identical predicates yield identical partition IDs.
3. The top-level `t.PartitionIDs` is the **union** across sub-requests
   (`t.partitionIDsSet.Collect()`, `task_search.go:740`), and that union is what
   `PinReadableSegments` pins (`delegator.go:617`). On the worker, `validate()`
   (`internal/querynodev2/segments/validate.go:25`) accepts a `partitionIDs` parameter but never
   references it — segment selection is driven entirely by the `segmentIDs` the delegator already
   computed.

Including it would also have forced a set comparison rather than a slice one, since `getPartitionIDs`
returns a map-backed set's `Collect()` and its order varies between calls. Dropping it removes both the
field and that trap.

> Pre-existing observation, not introduced here: (2) + (3) mean that in partition-key mode every
> sub-request effectively searches the union of the branches' partitions rather than its own narrower
> set. This is correct — the partition-key predicate is in the expression, so segcore filters the rows —
> but the per-sub-request segment-skipping optimization does not currently take effect. Worth a separate
> look; it is out of scope for this MEP.

**P1 refinement — group per data scope.** Since `IgnoreGrowing` only affects growing segments, and the
sealed set is by definition identical across all branches, the sealed side can be grouped
*unconditionally*. `organizeSubTask` already emits sealed (`DataScope_Historical`) and growing
(`DataScope_Streaming`) as separate sub-tasks (`delegator.go:1033-1039`), so pushing the grouping
decision down to scope granularity is structurally natural. Since the sealed side carries nearly all of
the segments and therefore nearly all of the filter cost, this recovers essentially the full benefit for
branches that disagree on `IgnoreGrowing`. Deferred to P1 because it makes the delegator's grouping logic
noticeably more involved, and the case it serves is expected to be rare.

**Fields that may differ freely:** `field_id`, `metricType`, `topk`, `offset`, `group_by_field_id`,
`group_size`, `analyzer_name`, `placeholder_group`, `nq`, `search_type`.

Request-level fields (MVCC timestamp, collection TTL, entity TTL, consistency level, namespace) are
identical across sub-requests by construction. `namespace` deserves an explicit note: it is folded into
the predicate by `MergeExprWithNamespace` during plan parsing, so it is already covered by the byte
comparison.

Byte equality of the serialized predicate is a conservative test — it may miss semantically equivalent
predicates that serialize differently — but it is sound and free. Both sub-requests are produced by the
proxy from the same user-supplied `Dsl` through the same `tryGeneratePlan`, so when the caller passes the
same filter to both, the bytes match.

**Extracting the predicate:** unmarshal `planpb.PlanNode`, take `GetVectorAnns().GetPredicates()`, and
compare its serialized form. `exprutil.ParseExprFromPlan` (already used by `segment_pruner.go`) provides
the accessor. To keep the delegator cheap, hash once per sub-request and compare hashes, then verify
full bytes within a hash bucket.

**Building the grouped request.** The existing flattening loop (`delegator.go:627-660`) already
hand-copies every field of a sub-request into a standalone `internalpb.SearchRequest`; for a group it
runs unchanged for the group's **first** member and the remaining members are attached verbatim as
`extra_filter_sharing_reqs`. `sd.modifySearchRequest` (which builds the per-worker request) needs one
added line to carry the new field through; the inner `req` already round-trips via
`shallowcopy.ShallowCopySearchRequest`.

Group ordering must be stable so that `req_index` in the response maps back to the caller's original
`SubReqs` position. Carry the original index alongside each group member rather than relying on the
group's internal order.

### 2. Worker: `SearchTask` with a branch dimension

`internal/querynodev2/tasks/search_task.go`. The governing principle is that the branch dimension
affects **execution only, never reduce**:

```go
type SearchTask struct {
    ...
    branches []*segcore.SearchRequest  // len == 1 reproduces today's behavior exactly
}
```

`Execute()`:

0. Reconstruct the branch list as `[req] + extra_filter_sharing_reqs`. Branch 0 is `req` itself, so
   `len(extra) == 0` yields a one-element list and every step below degenerates to today's code path.
1. Build one `segcore.SearchRequest` per branch via `collection.NewSearchRequest` — each parses its own
   plan and placeholder group, as today. `NewSearchRequest` takes a `*querypb.SearchRequest`, so the
   extra branches need a small adapter that projects a `SubSearchRequest` plus the shared envelope into
   the shape it expects.
2. One `segments.SearchHistoricalGrouped` / `SearchStreamingGrouped` call. Inside
   `searchSegmentsAttempt` (`internal/querynodev2/segments/search.go:77`), the per-segment
   `s.Search(ctx, req)` becomes `s.SearchGrouped(ctx, reqs)`, returning `[]*SearchResult` of length
   `len(branches)`. `SearchGrouped` computes the shared prefix once and then fans the branches out
   **concurrently** over an `errgroup`, mirroring the segment-level fan-out one level up:

   ```go
   func (s *LocalSegment) SearchGrouped(ctx context.Context, reqs []*segcore.SearchRequest) ([]*segcore.SearchResult, error) {
       bits, err := s.csegment.ComputeFilterBitset(ctx, reqs[0])
       if err != nil { return nil, err }
       defer bits.Release()

       out := make([]*segcore.SearchResult, len(reqs))
       g, gctx := errgroup.WithContext(ctx)
       for i := range reqs {
           i := i
           g.Go(func() error {
               r, err := s.csegment.SearchWithBitset(gctx, reqs[i], bits)
               out[i] = r
               return err
           })
       }
       return out, g.Wait()
   }
   ```

   `len(reqs) == 1` still takes this path; it costs one extra cgo call versus `Search` and keeps a single
   code path. Whether to special-case N==1 back to plain `Search` is a micro-optimization to settle with
   a benchmark, not a design question.
3. Transpose `[segment][branch]` into `[branch][segment]`.
4. **Run the existing reduce pipeline once per branch, unchanged.** `PrepareSearchResultsForExport` →
   `exportSearchResultsAsArrow` → `buildReduceLayout` → `executeGoReduce` →
   `materializeAndAssignResult` all take `(plan, placeholderGroup, results, originNqs, originTopks)`,
   and every branch has its own. This is an added loop, not a rewrite.
5. Emit `resp.SubResults[branch]` with `req_index` set to the sub-request's index in the original
   `SubReqs` list.

`MergeWith` returns `false` unconditionally for a grouped task. The NQ-axis merge and the
filter-sharing group are two different merge semantics and must not interact.

### 3. Segcore: the two phases

**Phase 1 — `SegmentInternalInterface::ComputeFilterBitset(plan, ...)`**

```
1. prefix = ExtractFilterOnlyPlan(plan->plan_node_)
   If nullptr (no filter subtree, or an iterative-filter plan), return a null handle;
   Go falls back to N independent Search() calls.

2. Build a QueryContext, execute PlanFragment(prefix).
   -> RowVector{bitset, valid}

3. Package the RowVector together with the derived QueryContext state (see 3.1)
   into a SharedFilterBitsetResult and hand ownership to Go.
```

**Phase 2 — `SegmentInternalInterface::SearchWithBitset(plan, phg, bitset_result, ...)`**, run once per
branch, concurrently:

```
1. Check the O(1) invariants: bitset_result->segment_id == this segment, and
   bitset_result->active_count == the branch's active_count.

2. Build QueryContext_i with this plan's search_info_ and this branch's placeholder_group_.
   Install the derived state carried by the result (see 3.1).
   set_precomputed_bitset(bitset_result->bitset)   // shared, read-only

3. Execute PlanFragment(rebind(plan->plan_node_)), where rebind replaces
   VectorSearchNode's source with a PrecomputedBitsetNode.

4. Return QueryContext_i->get_search_result() as a single leaked SearchResult*.
```

Phase 2 deliberately does **not** re-verify that the branch's filter subtree matches the one the bitset
came from. Such a check means rendering the expression tree with `ToString()` once per branch per
segment; for the multi-kilobyte predicates this design targets that is pure overhead in the correct
case, and predicate equality is already established by the delegator's byte comparison. The two O(1)
checks that remain catch the mistakes that actually corrupt results — a handle passed to the wrong
segment, or a snapshot that changed between phases.


Concurrency safety across the N phase-2 calls: each has its own `QueryContext` and its own
`SearchResult`; the only shared object is the bitset result, which is read-only after phase 1 (see 3.3).

#### 3.1 Derived query state carry-over

This is the subtlest part of the design and the most likely source of bugs. Evaluating the filter
subtree writes **side-effect state onto the `QueryContext`**, not just the returned bitset. Running it
once means only the producing `QueryContext` receives that state; it must be explicitly propagated to
the other branches.

Known writers today:

| Writer | State written |
|---|---|
| `PhyMvccNode` (`MvccNode.cpp:94`) | `set_all_rows_visible(true)` |
| `PhyElementFilterBitsNode` (`ElementFilterBitsNode.cpp:102-134`) | `set_array_offsets`, `set_active_element_count`, `set_struct_name`, `set_bitset_is_element_level(true)` |

`PhyVectorSearchNode::GetOutput` reads `get_all_rows_visible()` and `bitset_is_element_level()` to pick
between the empty-`BitsetView` fast path and the normal path (`VectorSearchNode.cpp:128-146`). Dropping
either would silently change which rows are searched.

`SharedFilterBitsetResult` therefore carries this state alongside the bitset, with `CaptureFrom` /
`ApplyTo` as the only way it moves. Keeping it on the same struct rather than in a parallel type means
there is exactly one thing to hand between the phases, and adding a new side effect to a filter-subtree
operator forces a visible edit to that struct rather than a silently missing copy.

#### 3.2 New plan node and operator

```cpp
// internal/core/src/plan/PlanNode.h
class PrecomputedBitsetNode : public PlanNode {
    // No sources; carries no data. The bitset lives on the QueryContext so the
    // plan tree stays stateless and shareable across branches.
};

// internal/core/src/exec/operator/PrecomputedBitsetNode.{h,cpp}
class PhyPrecomputedBitsetNode : public Operator {
    RowVectorPtr GetOutput() override {
        if (finished_) return nullptr;
        finished_ = true;
        return query_context_->get_precomputed_bits();
    }
    bool IsFinished() override { return finished_; }
};
```

Register a `dynamic_pointer_cast<const plan::PrecomputedBitsetNode>` branch in the operator factory
(`internal/core/src/exec/Driver.cpp:81-147`). `MvccNode` already demonstrates the source-operator shape
via `is_source_node_ = sources().empty()`.

`rebind` replaces `VectorSearchNode`'s `sources_[0]` with a `PrecomputedBitsetNode`. The replacement point
is exactly the extraction point of `ExtractFilterOnlyPlan`, so the two operations are symmetric by
construction.

#### 3.3 Bitset sharing safety

`PhyVectorSearchNode::GetOutput` (`VectorSearchNode.cpp:120-215`) only **reads** the bitset on the
row-level path: it constructs a `BitsetView` over `col_input->GetRawData()` and hands it to
`vector_search`. The element-level path calls `RowBitsetToElementBitset`, which allocates a new bitmap
rather than mutating the input.

Do not rely on that invariant holding forever. Hand each branch a `shared_ptr<const RowVector>`, and let
the element-level branch build its own derived bitmap as it does today. One extra memcpy in the
element-level path buys immunity to an entire class of future use-after-write bugs.

#### 3.4 Per-branch concerns

Walking `AsyncSearch` (`internal/core/src/segcore/segment_c.cpp:485-530`), these steps are per-call today
and must become **per-branch**, not "branch 0 only":

- `CheckExternalFieldsInLoadedManifest(plan->schema_, segment, plan->access_entries_, ...)` — each plan
  has its own `access_entries_`.
- `FieldAccessible(target_vector_field_id)` — if one branch's vector field is inaccessible, **that
  branch** returns an empty `SearchResult` while the others proceed normally.
- Distance-sign flipping under `!PositivelyRelated(metric_type)` — IP and BM25 differ here; sharing this
  across branches would corrupt scores.

Genuinely shareable:

- `LazyCheckSchema` and `ValidateSegmentSchemaCompatibility` — once per call.
- `read_lease_` is a `std::shared_ptr<segcore::SegmentReadLease>` (`internal/core/src/common/QueryResult.h:262`),
  so all N results can hold the same lease.

**Storage cost attribution.** `op_context.storage_usage` accumulates the prefix's I/O plus each branch's.
Attribute the prefix to branch 0 and each branch's own I/O to itself, and say so in a comment — otherwise
a future reader doing cost attribution will silently get skewed numbers.

**Failure isolation.** First version: any branch throwing fails the entire grouped call. Per-branch
status would drag in error propagation, partial `SubResults`, and delegator-side partial handling for a
benefit that does not justify it.

## Correctness Guarantees

1. **Identical segment set.** All sub-requests of one hybrid search already execute over the same pinned
   snapshot and the same MVCC timestamp (`delegator.go:617`), and `organizeSubTask` derives the
   segment→worker assignment from that same snapshot. Grouping does not change which segments are
   searched.
2. **Identical bitset by construction.** Byte-identical predicate + identical segment + identical MVCC
   timestamp + identical TTL context ⇒ the prefix is a pure function of inputs that are equal. The
   `ToString()` re-check in segcore enforces this at the point of use.
3. **Results are bit-for-bit identical to the unshared path.** Because the candidate set is identical, so
   is each branch's top-k. This makes verification unusually strong: it is an equality assertion, not a
   similarity check. See Test Plan.
4. **Safe degradation.** Every rejection path — predicate mismatch, missing filter subtree, iterative
   filter, config disabled — falls back to the existing per-sub-request execution. There is no
   intermediate state where partial sharing could produce a partially-correct result.

### A pre-existing race, noted but out of scope

`PruneSegments` mutates the shared `sealed []SnapshotItem` in place (`internal/querynodev2/delegator/segment_pruner.go:161-165`,
`sealedSegments[idx] = item`) while running inside each sub-request's goroutine. It is a data race today,
masked only by `queryNode.enableSegmentPrune` defaulting to `false`. Grouping incidentally removes it
*within* a group (one prune per group), but a request containing both a grouped and a singleton group
still races. It is **not addressed by this MEP** (see Q7); fixing it means either moving pruning above the
fan-out or giving each sub-request its own copy of the snapshot.

## Test Plan

**Differential correctness (the primary gate).** Run a corpus of hybrid searches twice against the same
data — once with `sharedFilter.enabled=false`, once with `true` — and assert the serialized
`SearchResults` are **byte-identical**. Cover: 2-way and 3-way; dense+sparse and dense+BM25; identical
predicates and deliberately mismatched predicates (must fall back); element-level; group-by;
`ignore_growing` set on one branch only; empty filter; filter matching zero rows; filter matching all rows
(exercises the `all_rows_visible` fast path and the `SharedPrefixState` propagation).

**Segcore unit tests.** `ComputeFilterBitset` + `SearchWithBitset` with N=1 (must equal `Search`), N=2 identical predicates, N=2
mismatched predicates (fallback), a plan with no filter subtree (fallback), and one branch whose vector
field is inaccessible (that branch empty, others correct).

**Fault injection.** Cancellation mid-prefix; cancellation mid-branch; one branch throwing; segment
released between prefix and branch execution.

**Concurrency.** Run the grouped path under the Go race detector: the N phase-2 calls share one
`CSharedFilterBitsetResult`, so the detector must see no write to it after phase 1 returns. Also cover the
`errgroup` error path — one branch failing while siblings are still in flight must release every result
that did come back, and must release the prefix handle exactly once.

**Performance.** On a workload of dense + BM25 hybrid searches sharing one filter, confirm
`internal_core_search_latency_scalar` drops
roughly by the sharing factor while `internal_core_search_latency_vector` is unchanged, and measure the
resulting end-to-end latency change. Report the scalar/vector ratio *before* the change so the expected
ceiling is known in advance rather than rationalized afterwards. Separately, watch
`getSearchCPUExecutor` queueing: the branch-level `errgroup` nests inside the segment-level one, so
in-flight cgo calls go from about `S` to about `S x N` (see Q6).

## Rollout

1. Land behind `queryNode.hybridSearch.sharedFilter.enabled=false`.
2. Enable on one canary QueryNode; verify the differential-correctness corpus and the group-size
   histogram (expect mode 2 for this workload).
3. Enable cluster-wide; watch `internal_core_search_latency_scalar` and the fallback counter by reason.

## Delivery Phases

| Phase | Scope |
|---|---|
| P0 | 2 branches, row-level, non-iterative, byte-identical predicates — the dense + BM25 pairing that motivates this design |
| P1 | Element-level; 3+ branches; per-scope grouping so `IgnoreGrowing` no longer splits a group. Structurally already supported; mostly test surface |
| P2 | Common-prefix sharing (`(C) && extra`) via expression-level subtree extraction and delta bitsets |

P2 is a materially different problem — it needs a common-subexpression analysis over the predicate tree
and a way to apply the residual conjunct to an already-computed bitset. It should be a separate MEP.

## Design Decisions（设计决策）

Q1–Q6 均已定案，逐条记录决定与理由；Q7 移出本 MEP 范围。

### Q1. 额外分支的 search param 调优 —— 已定案，但极易遗漏

**已决**。之所以仍然记在这里，是因为它是整个设计里最容易漏掉的一环，而且漏掉之后**不报任何错**。

`optimizers.OptimizeSearchParams`（`internal/util/searchutil/optimizers/query_hook.go:32`）在 `sd.search`
派发之前运行。它反序列化 `req.Req.SerializedExprPlan`，把 `topk`、search params、vector type、dim、
`withFilter` 喂给 AutoIndex query hook，改写 `queryInfo`（`topk`、`SearchParams`、refine ratio），最后
**把 plan 重新 marshal 回 `req.Req.SerializedExprPlan`**。它读的每一项都是 per-branch 的。

在选定的 proto 形状下，它对第 0 路继续原样工作，无需改动。**但额外分支如果不补一个循环就什么都拿不到**
—— 它们会带着未经调优的 search params（`ef` 之类直接来自用户请求，没有 AutoIndex 调整）进入 segcore。
不会有任何报错，只是第 0 路和其余各路的召回与延迟悄悄不一致。

要求：`sd.search` 对 `extra_filter_sharing_reqs` 的每一项也各跑一次 `OptimizeSearchParams`，针对该分支
自己的 plan；并且要有测试断言每个分支的 `queryInfo.SearchParams` 都被改写过。

`IsTopkReduce` / `IsRecallEvaluation` 不需要新增 proto 字段：第 0 路照旧写进 `req.Req`，额外分支按位或
合并到同一组字段上。这与既有的合并规则一致 —— `ReduceAdvancedSearchResults`
（`internal/querynodev2/segments/result.go:137`）本来就是跨 sub-result 对 `IsTopkReduce` 做或运算的。

还有一项残留检查：重新 marshal 会重写整个 `PlanNode`，包括 predicate 子消息。predicate 本身不会被改动，
且 Go protobuf 的 marshal 在同一进程内是确定性的，所以各分支的字节理应保持相等 —— 但这正是 segcore 侧
的 `ToString()` 复核要比较**解析后**的前缀而不是原始字节的原因。需要补一个测试，断言 predicate 字节在
调优前后于所有分支上保持一致。

### Q2. 与 two-stage search 的相互作用 —— 已定案：两者正交且协同

**已决：grouped 请求照常走 two-stage，两个参数都透传。**

早期草案把两者定为互斥，理由是「stage 1 就是单独跑一次 filter，和共享前缀重叠」。这个判断是错的 ——
重叠恰恰意味着**协同**，不是冲突。

`filter_only` 和 phase 1 跑的是**同一棵子树**（都是 `ProtoParser::ExtractFilterOnlyPlan`），区别只在输出：
`filter_only` 算完 bitset 只留 `valid_count = active_count - view.count()` 就把它扔掉，phase 1 则把
bitset 留住交出去。所以在分组场景下，two-stage 的 stage 1 本来就该是分组的 —— N 路共享同一个 predicate，
跑一遍就够，而不是今天的跑 N 遍。**分组是在帮 two-stage。**

落地方式：

- **stage 1**：`SearchTask.Execute` 在 `FilterOnly` 时直接走 `executeSingle`（branch 0），无视 extras。
  因为所有分支的 predicate 相同，branch 0 那一趟产出的 per-segment valid counts 就是整组的答案。
  新的 cgo 接口因此**不需要** `filter_only` 参数：那是另一种输出形态，由现有 `AsyncSearch` 路径提供。
- **stage 2**：`AsyncComputeFilterBitset` 透传 `enable_expr_cache`，于是 phase 1 直接从
  `ExprResCacheManager` 读回 stage 1 写入的 bitset，连一次求值都省掉。
- **判定规则**：`ShouldUseTwoStageSearch` 读的 `topk` / `search_type` 都是 per-branch 的，拿 branch 0
  代表整组没有正当性。改成 `shouldUseTwoStageSearchForGroup`：**任一分支够格即整组走 two-stage**。这不是
  宽松，而是正确 —— stage 1 是全组共享的一趟，给已经够格的组多带一个分支，边际成本为零。
- **stage 2 的参数调优**：`twoStageSearch` 内部那次 `OptimizeSearchParams` 同样只改 `req.Req`，
  必须补上 `optimizeSharedFilterBranches`（与 Q1 同源的问题，两处调用点都要覆盖）。

值得记下的一点：`enable_expr_cache` 只控制**整份 FilterBitsNode bitset** 那一层缓存
（`FilterBitsNode.cpp:100,151`）。叶子子表达式缓存（`Expr.h:505,529`、`ExprCacheHelper.h:93`，含
TEXT/PHRASE_MATCH 走的 `GetOrCompute`）只看全局 `ExprResCacheManager::IsEnabled()`，在 phase 1 里一直是
生效的。所以「共享 filter 会绕过 expr cache」从来就不成立，之前只是整份 bitset 那一层没接上而已。

### Q3. cgo 结果的所有权 —— 已定案

**已决：不引入结果数组，用两段式接口，每次调用仍然只返回一个 `SearchResult*`。**

原本的顾虑是 `AsyncSearchGrouped` 要返回 N 个结果，就得新增 `CSearchResultArray` 类型、配套的析构函数，
以及构造中途失败时的部分释放语义。两段式接口（见 Public Interfaces → New cgo API）让这个问题整个消失：
`AsyncSearchWithBitset` 的返回形状与今天的 `AsyncSearch` 逐字一致 —— 单个泄漏出来的 `SearchResult*`，
Go 侧包装后由现有的 `DeleteSearchResults` 释放。所有权模型一个字没变。

新增的唯一跨边界对象是 `CSharedFilterBitsetResult` 句柄，它的生命周期是**一个 Go 函数作用域加一个 defer**，
覆盖一个 segment、一个组。panic 和 cancel 都能正常释放。

### Q4. 失败隔离的粒度 —— 已定案

**已决：任一分支失败即整个 grouped 调用失败。**

这不只是「更简单」，而是与既有语义一致：hybrid search 今天本来就是一路失败即整体失败 ——
`conc.AwaitAll(futures...)` 返回错误后 `sd.Search` 直接 `return nil, err`；即使全部 future 成功，随后
遍历结果时只要有一个 `result.GetStatus().GetErrorCode() != Success` 也会
`return nil, merr.Error(result.GetStatus())`（`delegator.go:624-694`）。分组不改变这个行为。

**但要保留一个区分**：`FieldAccessible` 返回 false **不是失败**。今天它让该路返回一个空的
`SearchResult`（`segment_c.cpp:507-514`），这是一条合法的非错误路径，必须保持 per-branch —— 某一路的
向量场不可访问时，该路返回空结果，其余分支照常执行。所以「异常导致整体失败」和「字段不可访问导致该路
为空」是两回事，不要在实现里合并。

C++ 侧的异常安全：phase 2 是 N 次独立调用，各自返回自己的 `SearchResult*`，Go 侧用 `errgroup` 汇聚。
某一路失败时，已经成功返回的兄弟结果由 Go 侧统一 `DeleteSearchResults` 释放 —— 与今天
`searchSegmentsAttempt` 失败时清理 `validResults` 的做法一致（`segments/search.go:56-62`）。

### Q5. 调度器的开销计量 —— 已定案（并更正先前的表述）

计量位于 querynode 的读任务调度器 `internal/util/searchutil/scheduler/concurrent_safe_scheduler.go`：
`waitingTaskTotalNQ` 由 `updateWaitingTaskCounter` 维护，`GetWaitingTaskTotalNQ()` 读出。消费者只有两处：

1. `QueryNodeReadTaskReadyNQ` 指标（`:421`），以及回传给 proxy 的
   `resp.GetCostAggregation().TotalNQ`（`services.go`），proxy 用它做负载/副本选择。
2. `canMergeNQ` / `MaxGroupNQ`（`queues.go:141-181`、`fifo_policy.go:37`），即 NQ 轴合并策略。

**更正**：本文档早期版本称「少报会让过多工作涌入执行池」，这是错的。执行并发度**不按 NQ 计**，而是由
`conc.NewPool[any](maxReadConcurrency)`（`:32-39`）按**任务数**限流。而且 grouped task 的 `MergeWith`
恒返回 false，`canMergeNQ` 永远看不到它。

于是真实影响只剩一条：**`SearchTask.NQ()` 应返回各分支 nq 之和**，否则回传给 proxy 的负载估计偏小。
`MinNQ()` 同理取各分支最小值。两处都只服务于上面列出的消费者，改动是局部的。

顺带一提，池的压力实际是**下降**的：今天 2 路 = 2 个调度任务 = 2 个池槽位，合并后是 1 个任务、1 个槽位，
且总工作量还少了一次 filter。

### Q6. 分支的并发执行 —— 已定案

**已决：过滤完成后，各路向量检索仍然并发执行。**

这消除了先前识别的唯一延迟回归风险 —— per-segment wall clock 保持在 `F + max(A_i)`，而不会退化成
`F + ΣA_i`，同时 CPU 总量仍然省下一个 `F`：

| | 总 CPU | wall clock（`S >> P`） | wall clock（`S <= P`） |
|---|---|---|---|
| 今天 | `S(2F + A1 + A2)` | `S(2F + A1 + A2)/P` | `F + max(A1, A2)` |
| 合并，分支串行（**未采用**） | `S(F + A1 + A2)` | `S(F + A1 + A2)/P` | `F + A1 + A2` |
| 合并，分支并发（**采用**） | `S(F + A1 + A2)` | `S(F + A1 + A2)/P` | `F + max(A1, A2)` |

**并发必须放在 Go 侧，不能放在单次 cgo 调用内部。** `AsyncSearch` 由 Go 提交到
`getSearchCPUExecutor`（`internal/core/src/futures/Executor.cpp`，`CPU_NUM` 个线程）。若一个 grouped 任务
运行在该 executor 的线程上，又向**同一个** executor 提交 N-1 个分支任务并阻塞等待，则当所有线程都被这类
等待者占满时，排队中的分支任务永远拿不到线程 —— 自提交死锁。规避它要么另开并调参一个 executor，要么把
fan-out 放回 Go。选后者：`LocalSegment.SearchGrouped` 用 `errgroup` 并发调用 `SearchWithBitset`，与上一层
`searchSegmentsAttempt` 对 segment 的 fan-out 是同一套机制（见 Design Details §2）。

注意这**不是**新增的同步点：delegator 本来就在响应之前把所有 sub-request join 了一次（`conc.AwaitAll`，
随后 `ReduceAdvancedSearchResults` 打包进一个 `SearchResults.SubResults` ——
`internal/querynodev2/segments/result.go:137`），shard leader 的响应一直是被最慢的那一路卡住的。

仍需 benchmark 确认的是**总并发度**：segment 层的 `errgroup` 之下又叠了一层分支 `errgroup`，同时在飞的
cgo 调用数从 `S` 变成约 `S × N`。`getSearchCPUExecutor` 的线程数固定为 `CPU_NUM`，因此这是排队深度变化而
非线程爆炸，但仍应在压测中观察队列延迟。

### Q7. `PruneSegments` 的 race 修复 —— 本设计不处理

见 Correctness Guarantees。它是既有 bug（默认 `queryNode.enableSegmentPrune=false`，故未暴露），本设计
只是在组内顺带消除了它。**不在本 MEP 范围内**，另行处理。

## Rejected Alternatives

### Merge sub-requests into a single plan with `repeated VectorANNS`

Superficially the "proper" modeling, but strictly more expensive. `planpb.PlanNode` carries
`output_field_ids`, `dynamic_fields`, `scorers`, `plan_options`, `score_option`, and
`querynode_function_chains`, all of which may differ per sub-request; merging forces a merge policy for
each. The proxy's `tryGeneratePlan` would also have to learn to emit a multi-ANNS plan. In exchange the
execution layer saves nothing — it still has to run one vector search per branch. Keeping N independent
plans and sharing only execution is a strictly smaller interface change.

### Reuse `ExprResCacheManager` with request-scoped admission

`ExprResCacheManager` (`internal/core/src/exec/expression/ExprCache.h`) already caches whole
`FilterBitsNode` bitsets keyed by `{segment_id, FilterBitsNode::ToString()}`, and two-stage search
already uses it to carry a bitset from stage 1 to stage 2. Reusing it here fails on four counts:

1. **Zero cross-request hit rate for the workloads this targets.** When each query carries a distinct
   literal (a different phrase, a different term list), the whole-filter signature is unique per query.
   The entry is written once, read once, and then dead until evicted.
2. **The admission control exists precisely to reject this shape.** `admissionThreshold=2` rejects the
   first `Put` (`ExprCache.cpp:336`) specifically so one-shot expressions do not consume slots and issue
   pointless writes. Making the optimization work means disabling that defense.
3. **Per-entry costs are designed to be amortized over many reads.** Memory mode clones the full
   `TargetBitmap` and applies Roaring compression on write, decompressing on read. Disk mode — the
   default — `pwrite`s the bitset to a per-segment slot file and `pread`s it back. Paying either for a
   single read inside one request is likely slower than recomputing.
4. **No single-flight.** Both `PhyFilterBitsNode::GetOutput` (`FilterBitsNode.cpp:143-220`) and
   `ExprCacheHelper::GetOrCompute` are plain get→compute→put. Sub-requests are dispatched concurrently
   and reach the same segment at the same time, so both would miss, both would compute, and both would
   write. The optimization would not even trigger.

Making it work would additionally require refcounted eviction so entries do not outlive the request. The
lifetime here is fully determined — who reuses the value (the sibling branches), how many times (N-1),
and when it is dead (end of request). A cache expresses "unknown reuse, unknown lifetime"; this is the
wrong primitive for a fully determined one.

The *leaf-level* sub-expression cache is orthogonal and complementary: stable conjuncts that repeat
across requests are cacheable at their own `ToString()` granularity, addressing a different part of the
predicate. This design neither helps nor hinders it.

### A request-scoped shared-bitset registry in segcore

A `scope_id → {segment_id → shared_future<bitset>}` map with an explicit `ReleaseScope` cgo call from Go.
This works and gives single-flight for free, but it is still a process-global map that needs refcounting
and a release path that must be leak-proof across every error, timeout, and cancellation branch — including
the case where one branch fails while another is still running. Merging into one call makes the bitset a
local value with no lifetime question at all.

### Group in the worker scheduler via `SearchTask.MergeWith`

No RPC or proto change, but it depends on two independent RPCs landing in the same scheduling window —
timing-dependent and non-deterministic. It also overloads the NQ-axis merge with an orthogonal
row-sharing semantic.

## Implementation Map (P0)

Where each piece of this design lives, for review navigation.

| Design element | Code |
|---|---|
| Proto field | `pkg/proto/query_coord.proto` — `SearchRequest.extra_filter_sharing_reqs` |
| Grouping, request build, result demux, per-branch param optimization | `internal/querynodev2/delegator/shared_filter.go` |
| Grouped fan-out; two-stage guard; field forwarding | `internal/querynodev2/delegator/delegator.go` |
| Branch expansion, per-branch reduce, `SubResults` assembly | `internal/querynodev2/tasks/search_task.go` — `executeSharedFilter`, `branchTask`, `reduceSegmentResults` |
| Per-segment prefix + concurrent branch fan-out | `internal/querynodev2/segments/segment.go` — `LocalSegment.SearchGrouped` |
| Branch-major segment fan-out | `internal/querynodev2/segments/search.go` — `searchSegmentsGrouped*`, `SearchHistoricalGrouped`, `SearchStreamingGrouped` |
| cgo wrappers | `internal/util/segcore/segment.go`, `responses.go` — `ComputeFilterBitset`, `SearchWithBitset`, `SharedFilterBitsetResult` |
| cgo entry points | `internal/core/src/segcore/segment_c.{h,cpp}` — `AsyncComputeFilterBitset`, `AsyncSearchWithBitset`, `DeleteSharedFilterBitsetResult` |
| Two phases | `internal/core/src/segcore/SegmentInterface.{h,cpp}` — `ComputeFilterBitset`, `SearchWithBitset` |
| Two execution phases | `internal/core/src/query/ExecPlanNodeVisitor.{h,cpp}` — `get_shared_filter_bitset_result`, `SetPrecomputedBitset` |
| Shared bitset payload + derived state | `internal/core/src/query/SharedFilterBitsetResult.{h,cpp}` |
| Precomputed bitset on the context | `internal/core/src/exec/QueryContext.h` — `set_precomputed_bitset` |
| Plan rebinding | `internal/core/src/query/PlanProto.cpp` — `ProtoParser::RebindToPrecomputedBitset` |
| Grouped two-stage search | `internal/querynodev2/delegator/shared_filter.go` — `shouldUseTwoStageSearchForGroup`; `delegator_twostage.go` |
| Source operator | `internal/core/src/plan/PlanNode.h` — `PrecomputedBitsetNode`; `internal/core/src/exec/operator/PrecomputedBitsetNode.{h,cpp}` |
| Config | `queryNode.hybridSearch.sharedFilter.enabled` (default `false`) |
| Metrics | `milvus_querynode_hybrid_shared_filter_fallback_total{reason}` |

Not yet done: the entire Test Plan. No test of any kind has been written or run against this
code beyond compilation, so nothing here is verified behaviorally.

## References

- `internal/proxy/task_search.go:551-702` — per-sub-request plan construction
- `internal/querynodev2/delegator/delegator.go:617-682` — snapshot pin and advanced fan-out
- `internal/querynodev2/tasks/search_task.go:402-440` — `SearchTask.Merge` and why hybrid never merges
- `internal/core/src/query/PlanProto.cpp:702-765` — vector plan chain construction
- `internal/core/src/query/PlanProto.cpp:1609` — `ExtractFilterOnlyPlan`
- `internal/core/src/query/ExecPlanNodeVisitor.cpp:394-465` — `filter_only_` execution mode
- `internal/core/src/exec/operator/VectorSearchNode.cpp:120-215` — bitset consumption
- `internal/core/src/exec/operator/MvccNode.cpp` — MVCC/delete mask and `all_rows_visible`
- `internal/core/src/exec/operator/ElementFilterBitsNode.cpp:102-134` — element-level context state
- `internal/querynodev2/delegator/delegator_twostage.go` — prior art for filter/search stage separation
- `docs/design-docs/design_docs/20260602-expression-result-cache.md` — `ExprResCacheManager`
