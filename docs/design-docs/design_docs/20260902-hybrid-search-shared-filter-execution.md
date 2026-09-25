# MEP: Shared Filter Execution for Hybrid Search

- **Created:** 2026-09-02
- **Author(s):** @zhengbuqian
- **Approver(s):** @czs007
- **Status:** Draft — P0 implementation updated; the latest revision has not been built or tested
- **Component:** QueryNode (Delegator, SearchTask), Segcore
- **Related Issues:** #53110
- **Released:** TBD

## Summary

A hybrid search (`HybridSearch`, internally a `SearchRequest` with N `SubSearchRequest`s) executes each
sub-request as a fully independent search: an independent plan, an independent worker RPC, and an
independent segcore call per segment. When two or more sub-requests carry an **identical filter
predicate**, the same filter bitset is therefore computed once per sub-request per segment, and the
same MVCC/delete mask is applied just as many times. A dense path plus a BM25 path over the same
filtered rows — the ordinary shape of a hybrid search — is exactly this case.

This design makes sub-requests that share a predicate execute the shared portion of the plan **once
per segment and per resulting group**. When the existing `queryNode.grouping.maxNQ` budget is positive,
eligible branches in a same-predicate bucket are split into stable work units whose summed NQ fits it;
an individually oversized or non-positive-NQ branch remains a singleton. With a non-positive budget,
every branch stays on the ordinary singleton path. Each work unit is an independent group.
The shared portion is exactly the source subtree of `VectorSearchNode`
(`FilterBitsNode → MvccNode → [ElementFilterBitsNode]`); the per-branch portion begins at
`VectorSearchNode`. Grouping is decided deterministically at the shard delegator, carried to the
worker as one RPC, and executed in segcore as one prefix call per segment followed by N concurrent
per-branch vector searches against its result.

Sub-request plans are **not** merged. Each branch keeps its own complete `query::Plan`. Only execution
is shared. This keeps `plan.proto` and per-branch plan construction unchanged; the proxy only publishes
the grouping hint. The existing per-branch reduce algorithm is reused.

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

`pkg/proto/internal.proto`:

```protobuf
message SubSearchRequest {
    // ... existing fields 1-15 ...

    // Groups sub-requests of one hybrid search whose filter predicate is
    // identical. Assigned by the proxy from (Dsl, expr_template_values)
    // equality; equal non-zero values mean byte-identical predicates, 0 means
    // the sub-request cannot share a filter (no predicate, or an iterative
    // filter). Read by the delegator as the grouping key.
    int32 filter_sharing_group = 16;
}
```

Written at `internal/proxy/task_search.go` (the `tryGeneratePlan` loop, via
`assignFilterSharingGroups`), read at `internal/querynodev2/delegator/shared_filter.go`
(`sharedFilterKeyOf`). A proxy that does not set it leaves every sub-request at 0, which the delegator
treats as unshareable — the feature is inert, never wrong. The proxy also skips the assignment entirely
when `queryNode.hybridSearch.sharedFilter.enabled` is off, so nothing is computed while the feature is
dark; it already reads `QueryNodeCfg` for storage-usage tracking, so this adds no new dependency. The
resulting config skew is one-directional and benign: with the proxy off and the query nodes on, every
sub-request arrives at 0 and the node simply counts `unshareable` fallbacks.

No change to `pkg/proto/plan.proto`.

#### Mixed versions

Both fields are additive. An old worker ignores `extra_filter_sharing_reqs`, executes branch 0 only and
returns an ordinary single-result response with no `SubResults`. The delegator's demux **validates the
response shape** — the number of `SubResults` must equal the branch count and every `req_index` must
be in range — and returns `ServiceInternal` on a mismatch (`shared-filter worker returned 0 sub-results
for 2 branches`). The request fails; it does not return a subset of branches. There is no fallback to
per-sub-request execution and no version negotiation: the switch defaults to off, and the rule is to
turn it on only once every query node is upgraded — and to turn it **off again before rolling any
query node back**, since a rolled-back worker is the same old worker. The failure is deliberately
explicit: a silent per-branch retry would hide a skew that the operator needs to see.

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
same predicate. See D2.

**Why two phases rather than one `AsyncSearchGrouped` returning N results.** Two independent reasons,
each sufficient on its own:

1. **Branch parallelism without a self-submission deadlock.** Branches must run concurrently (D7). A
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
rather than once. The lease count is no higher than when the N sub-requests run independently. The cgo
and validation overhead remains part of the performance plan; this document does not assume it is
negligible.

### Configuration parameters

```yaml
queryNode:
  grouping:
    maxNQ: 64          # existing scheduler work-unit budget; also caps one shared-filter group
  hybridSearch:
    sharedFilter:
      enabled: false   # default off; flip on per-cluster during rollout
```

Both are `ParamItem`s in `pkg/util/paramtable/component_param.go` with `refreshable: true`. The feature
switch is read at the delegator grouping site, so turning it off restores today's behavior exactly,
with no partially-applied state. That holds because the two-phase segcore path is only entered for a
group of two or more:
`SearchGrouped` with a single request calls plain `Search`, a `filter_only` (two-stage stage 1) task
takes `executeSingle`, and an ordinary search never carries `extra_filter_sharing_reqs`. Turning the
switch off therefore does not merely stop grouping — nothing else on the worker changes.

The grouping site also reads the existing refreshable `queryNode.grouping.maxNQ`. It sums the declared
NQ of branches with the same grouping key and splits them, in input order, into independent chunks no
larger than that budget. Each chunk of two or more branches becomes one grouped task; a chunk left with
one branch takes the ordinary path. A branch whose NQ is non-positive or individually exceeds the
budget is always a singleton, as is every branch when the configured budget is non-positive. The
ordinary request path remains responsible for rejecting an invalid NQ.

This is a per-task work-unit budget, not node-global admission control. It does not set a byte limit on
retained results or bitsets, and it does not limit GPU kernels. Splitting a same-predicate bucket also
means that every resulting chunk evaluates that predicate once per segment; the budget deliberately
trades some repeated filter work for smaller grouped tasks.

### New metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `milvus_querynode_hybrid_shared_filter_fallback_total` | Counter | node_id, collection_id, reason | A sub-request that could not join a group, by why. `reason` ∈ {`unshareable`, `no_matching_peer`, `vector_prune`, `nq_budget`}. `nq_budget` counts only shareable sub-requests that remain singletons because the budget is non-positive, their NQ is non-positive or oversized, or a stable chunk split leaves them without a peer; successful multi-branch chunks are not fallbacks. Released with the collection's other per-collection metrics (`CleanupQueryNodeCollectionMetrics`). |

The existing segcore histograms are the primary effect measurement and need no change. Compare
`internal_core_search_latency_scalar` (`FilterBitsNode.cpp:230,295`) and
`internal_core_search_latency_vector` (`VectorSearchNode.cpp:211`) with the feature off and on; the
performance plan must establish both the saved scalar work and any change in vector-search cost.

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
feeds the result back into N vector searches instead of discarding it — through a stricter sibling,
`ExtractSharedFilterPrefix`, which accepts only the plan shapes phase 2 can rebind (§3).

Because the boundary is defined structurally rather than by expression type, element-level hybrid
search comes along for free: when `ElementFilterBitsNode` is present it sits below `VectorSearchNode`
and is therefore inside the shared prefix.

Data flow end to end:

```
proxy         build the same N complete plans; publish predicate-equality hints
  │
delegator     group by the proxy hint; split each key by the cumulative NQ budget; each chunk of size >= 2
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
// each multi-branch group's summed NQ is <= QueryNodeCfg.MaxGroupNQ
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
| `filter_sharing_group` (the proxy's hint) | Stands for the predicate, which determines the bitset | Yes — the real discriminator |
| `filter_sharing_group != 0` | Folds in "has a predicate" and "is not an iterative filter", both decided by the proxy | Yes |
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
decision down to scope granularity is structurally natural. This can recover sharing on sealed segments
when branches disagree on `IgnoreGrowing`; its value must be measured against the added delegator
complexity. Deferred to P1.

**Fields that may differ freely:** `field_id`, `metricType`, `topk`, `offset`, `group_by_field_id`,
`group_size`, `analyzer_name`, `placeholder_group`, `search_type`. NQ is not part of the filter-sharing
key, but the Hybrid Search API already requires every sub-request to carry the same NQ. The sum across
those legal branches determines where one same-key bucket is split into work units.

One request-scoped decision reads two of those and had to be carved out. With a **vector** clustering
key (itself gated by `common.enableVectorClusteringKey`) and `queryNode.enableSegmentPrune=true`,
`PruneSegments` takes the query vector out of the request's `placeholder_group`, computes centroid
distances with the request's `metricType`, and keeps only the segments near it. That is inherently per
branch: a group could either prune every branch by branch 0's vector — wrong rows for the others; a
BM25 branch next to a dense one would silently lose rows — or skip pruning and search the full
snapshot, trading the pruning ratio for the saved filter. P0 chooses the correctness-preserving
fallback: on such collections sub-requests are **not grouped** (`hasVectorClusteringKey`, fallback
reason `vector_prune`) and each branch retains its own pruning decision.
Scalar-key pruning reads the predicate, which the group shares by construction, and does not stop
grouping. Pruning per branch and searching the union would let these collections share the filter too;
it is a follow-up because `PruneSegments` mutates the snapshot in place.

**The early carve-out does not consult `enableSegmentPrune`.** That flag is `refreshable: true` and is
read again at prune time. If grouping depended on its earlier value, a request could be grouped while
the flag was false and later be pruned with branch 0's vector after it became true. The early grouping
pass therefore rejects a vector clustering key whenever it is effective under the current clustering
configuration, even if segment pruning is currently off.

The clustering-key configuration is refreshable too, so the execution path revalidates the actual key
selected for pruning. The private pruning helper selects that key once; if a grouped request resolves to
a vector key, it returns `errSharedFilterUngroupable` before pruning, and the delegator retries the
branches individually. Otherwise the same selected key is used for the prune itself. This closes the
refresh window without claiming that schema alone is a stable input.

Request-level fields (MVCC timestamp, collection TTL, entity TTL, consistency level, namespace) are
identical across sub-requests by construction. `namespace` deserves an explicit note: it is folded into
the predicate by `MergeExprWithNamespace` during plan parsing, so it is already covered by the byte
comparison.

**Where predicate equality is recognised: the proxy, not the delegator.**

The delegator decides *whether* to share — it owns the segment distribution and the MVCC pin, and it
splits further on `IgnoreGrowing` — but it should not have to *discover* which predicates are equal.
Discovering it there means unmarshalling every sub-request's `planpb.PlanNode`, re-marshalling its
predicate and hashing it, repeated on every shard the request touches. A prior revision measured the
following on a 6.4 KB plan of the shape this design targets (`BenchmarkSharedFilterKeyOf`):

| | ns/op | B/op | allocs/op |
|---|---|---|---|
| read the proxy's hint | 3.2 | 0 | 0 |
| unmarshal + re-marshal + SHA-256 | 175,130 | 38,524 | 790 |

The proxy already parses each sub-request's `(Dsl, expr_template_values)` into a plan
(`task_search.go`, the `tryGeneratePlan` loop), so it can recognise equality without reparsing the plan
on every shard. It publishes the answer as `SubSearchRequest.filter_sharing_group`: equal non-zero
values mean identical predicates, and 0 means the sub-request cannot share a filter at all — no
predicate, or an iterative filter, which applies the predicate after the vector search and leaves no
prefix subtree.

Comparing the *inputs* rather than the serialized predicate is sound for the same reason byte equality
was: same `Dsl`, same template values, same parser, same schema ⇒ same predicate. It is equally
conservative — semantically equivalent filters spelled differently will not group — and the original
inputs are still compared exactly before a group is assigned. The digest below only selects a bucket,
so a digest collision cannot merge unequal inputs. Singletons are numbered too rather than folded into
0, so the query node can still separate "nothing to share" from "nothing matched" in its fallback
metric.

One thing this gives up: with both reasons collapsed into 0, the delegator can no longer report *why* a
sub-request was unshareable. Recovering that distinction would mean unmarshalling the plan, which is the
cost being removed. The proxy-side tests cover the classification instead.

**The assignment avoids a pairwise scan.** `assignFilterSharingGroups` streams fixed-width
length-prefixed components of `(Dsl, template values)` into SHA-256 — template names sorted, values
marshalled deterministically — and uses the fixed 32-byte digest as the bucket key, without retaining
one aggregate encoding of the filter input. A request whose n sub-requests all carry the same `Dsl`
with n distinct template sets therefore builds n fingerprints instead of doing n²/2 template
comparisons. The fingerprint decides nothing by itself: every candidate that finds a bucket is still
compared exactly with that bucket's representative.

**Classifying "iterative filter" mirrors the segcore parser, not the request text.** A sub-request is
marked 0 only when the plan segcore will actually emit has no reusable prefix, so `planUsesIterativeFilter`
returns false in the three cases where an `iterative_filter` in `search_params` does not survive into
the plan: a group-by field is set; `search_params` carries `radius`, since range search is always
emitted as a pre-filter plan; and `QueryInfo.hints` says `disable`, which takes precedence over
`search_params`. All three emit a plan with an extractable prefix, so marking them unshareable would
give up sharing for nothing. The two errors are not symmetric — the other direction, marking a genuine
iterative-filter plan shareable, makes phase 1 throw — so the classification is written against the
parser's rules and tested against them rather than inferred from the parameter's presence.

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
affects **execution only, never the per-branch reduce algorithm**. The task keeps the wire request and
expands it when the grouped path starts:

```go
branchReqs := buildSharedFilterBranches(t.req) // [req] + extra_filter_sharing_reqs
```

`Execute()`:

0. If `len(extra_filter_sharing_reqs) == 0`, use `executeSingle`. Otherwise reconstruct the branch list
   as `[req] + extra_filter_sharing_reqs`; branch 0 is `req` itself.
1. Build one `segcore.SearchRequest` per branch via `collection.NewSearchRequest` — each parses its own
   plan and placeholder group, as today. `NewSearchRequest` takes a `*querypb.SearchRequest`, so the
   extra branches need a small adapter that projects a `SubSearchRequest` plus the shared envelope into
   the shape it expects. The delegator's work-unit budget uses each branch's declared `nq`, rather than
   reparsing vector payloads there. After parsing, the worker therefore compares every
   `SearchRequest.GetNumOfQuery()` with that branch's declared `nq`. A mismatch is an internal
   proxy/managed-function contract violation: it returns `ServiceInternal` before any segment search
   or branch-by-segment result-matrix allocation.
2. One `segments.SearchHistoricalGrouped` / `SearchStreamingGrouped` call. Inside
   `searchSegmentsGrouped` (`internal/querynodev2/segments/search.go`), the per-segment
   `s.Search(ctx, req)` becomes `s.SearchGrouped(ctx, reqs, limiter)`, returning `[]*SearchResult` of
   length `len(branches)`. `SearchGrouped` computes the shared prefix once and then fans the branches
   out **concurrently** over an `errgroup`, mirroring the segment-level fan-out one level up. The
   branch limiter is created **once per task** and handed to every segment, so the bound is on the task
   and not on each segment (D7):

   ```go
   // searchSegmentsGrouped, once per task:
   branchLimiter := semaphore.NewWeighted(int64(hardware.GetCPUNum()))

   func (s *LocalSegment) SearchGrouped(ctx context.Context, reqs []*segcore.SearchRequest,
                                        limiter *semaphore.Weighted) ([]*segcore.SearchResult, error) {
       bits, err := s.csegment.ComputeFilterBitset(ctx, reqs[0])
       if err != nil { return nil, err }
       defer bits.Release()

       out := make([]*segcore.SearchResult, len(reqs))
       err = runBranchesBounded(ctx, len(reqs), limiter, func(gctx context.Context, i int) error {
           // runBranchesBounded acquires before spawning, so waiting branches
           // do not become parked goroutines.
           result, branchErr := s.csegment.SearchWithBitset(gctx, reqs[i], bits)
           out[i] = result
           return branchErr
       })
       return out, err
   }
   ```

   Master and 3.0 also cap the grouped segment `errgroup` at the core count. On 2.6, lazy segment
   loading has separate cache and loader admission: `DiskCache.Do` runs before a task-scoped segment
   search token is acquired, and its pinned callback holds that token only around `SearchGrouped`.
   Cache misses can therefore load concurrently without allowing more than the core count of loaded
   segments to hold a shared bitset and execute the two phases at once.

   The ungrouped path takes neither bound: one branch, no limiter, and the same unbounded segment
   fan-out it has today.

   `len(reqs) == 1` does **not** take this path: `SearchGrouped` special-cases a single request to plain
   `Search`, and `executeSingle` handles every non-grouped task (including two-stage stage 1, which sets
   `filter_only` and has no counterpart in the two-phase cgo calls). The new path is entered only for
   two or more branches, so an ordinary search on an upgraded worker runs exactly the code it ran
   before, and the flag-off run of the differential test is the legacy baseline, not the new path
   compared with itself.

   **Invariant — one pin for the whole call.** The segment pin must span phase 1 through the last
   phase 2, never one pin per cgo call. With a per-call pin the segment can be released between
   `ComputeFilterBitset` and a branch's `SearchWithBitset`, and that branch then runs against a released
   segment. `SearchGrouped` therefore takes the pin at function entry and releases it with `defer`,
   outside the `errgroup`. A refactor that moves the pin inside the loop reintroduces a use-after-release
   that no test in this design would catch.
3. Transpose `[segment][branch]` into `[branch][segment]`.
4. **Run the existing reduce algorithm once per branch.** `PrepareSearchResultsForExport` →
   `exportSearchResultsAsArrow` → `buildReduceLayout` → `executeGoReduce` →
   `materializeAndAssignResult` all take `(plan, placeholderGroup, results, originNqs, originTopks)`,
   and every branch has its own. The grouped path supplies one task-scoped related-data-size value to
   those reductions rather than traversing the same segment metadata once per branch.
5. Emit `resp.SubResults[branch]` with branch-local `req_index`. The delegator retains the group's
   original sub-request indexes and maps each branch-local result back to the caller's `SubReqs` order.

**The grouped envelope is still a full `SearchResults`.** Besides `SubResults` it carries `Status`,
`Base`, `IsAdvanced=true`, `ChannelsMvcc` and a **non-nil** `CostAggregation` — the RPC handler
(`services.go`) assigns through `resp.GetCostAggregation()` unconditionally, so a nil there is a
process-level panic, not a request error. The worker builds the envelope with an empty
`CostAggregation{}` when nothing populated it.

**Branch note (2.6 result lifetime).** With `queryNode.search.enableResultZeroCopy=true`, a reduced
branch's non-empty `SlicedBlob` can still point into C-owned result memory. Grouped reduce keeps each
such temporary branch result registered in `MsgPins`, records which branches actually acquired a pin,
and attaches one cleanup to the final envelope that holds those branch results until the envelope is
consumed. The remote gRPC codec defers the envelope cleanup around marshaling, so both success and a
marshal error release the branch pins; an envelope that is never consumed has the existing `MsgPins`
GC finalizer as a safety net. If a sibling reduce fails before the envelope is built, the task releases
every completed branch pin while unwinding. If no branch actually owns a pin — because that branch used
copy mode or produced an empty blob — there is no envelope pin. For an in-process `LocalWorker` call the
codec does not run, so the wrapper first materializes every top-level and per-branch `SlicedBlob` as
Go-owned `ResultData`, then releases the response pin; the deferred release also runs if decoding a
sub-result fails. This is specific to 2.6's C-backed `MsgPins` result path; master and 3.0 use their
existing result materialization path instead.

**Per-branch attribution.** `SubSearchResults` carries no per-branch cost or flags, so the rule is
stated here rather than left to the reader:

- `IsTopkReduce` / `IsRecallEvaluation` are **OR-ed** across branches into the envelope at the
  delegator (`optimizeSearchParams`), so the proxy's top-level `resultSizeInsufficient &&
  isTopkReduce` re-search check fires if any branch was reduced. Which branch it was is not recoverable,
  and nothing downstream needs it. The two flags are also the query hook's *input* (what the request
  permits) on the same fields, and the hook overwrites them with its output, so the delegator captures
  the input once before branch 0 is optimized and hands that same input to every branch; the OR is
  written back only after all branches ran. The effective segment count the hook is given depends on
  topk and is computed per branch.
  `optimizeSearchParams` also applies Knowhere search defaults keyed by index type, so it resolves
  the index type from each branch's own `field_id` rather than reusing branch 0's.
- `ScannedRemoteBytes` / `ScannedTotalBytes` are **summed** across branches when the worker folds
  the per-branch results into the grouped envelope (`assembleSharedFilterEnvelope`). Each branch's own
  `reduceSegmentResults` attributes its scan to that branch's result via `attributeStorageCost`; the
   envelope assembly adds them up, so the envelope carries the group total under this accounting rule.
   The demux attaches it
  to branch 0 and zeroes the rest, and the top-level sum is unchanged. These feed
  `milvus_proxy_scanned_{remote,total}_mb` and the storage cost returned to the client. Inside segcore
  the filter's own bytes are read in phase 1, under an `OpContext` no branch owns; they are recorded
  on the `SharedFilterBitsetResult` and added to **exactly one** branch's result in phase 2 (an atomic
  claim on the shared result), so the group's total is what a plain search would have carried — once,
  not N times and not zero times.
- `TotalRelatedDataSize` is the size of the segments a request touched, not work done. Today each
  branch reports the full figure and the proxy sums them, so a two-branch hybrid search counts every
  segment twice. It is a property of the task's segment set and of nothing per branch, so a group
  computes it **once per task**, hands that same figure to every branch's reduce, and reports it
  **once** in the envelope. This feeds the `related_data_size` metering hook, so it is a visible change
  in that figure for grouped searches, in the direction of counting each segment once.
- `ServiceTime` is the **whole task's**, measured once by the worker after every branch has been
  reduced. The branches reduce concurrently (bounded by the core count) on their own recorders, so
  their individual readings are neither additive nor a measure of the task; summing them would report
  roughly N× the real duration and hand the load-aside balancer a negative execute speed.
- `ResponseTime` / `ServiceTime` / `TotalNQ` are merged by **maximum** at the delegator too
  (`mergeRequestCost`), so zeros on the non-zero branches are inert.

`MergeWith` returns `false` for a grouped task, and the guard is **symmetric**: `Merge` refuses when
*either* the receiver or the argument carries `extra_filter_sharing_reqs`. The scheduler calls
`taskInQueue.MergeWith(incoming)`, so a one-sided check on the receiver would let an ordinary task
already in the queue absorb an incoming grouped task as a plain NQ merge and drop its extra branches.
The NQ-axis merge and the filter-sharing group are two different merge semantics and must not interact.

### 3. Segcore: the two phases

**Phase 1 — `SegmentInternalInterface::ComputeFilterBitset(plan, ...)`**

```
0. check_search(plan) -- the same admission check as Search, so a predicate
   field that is not loaded surfaces as the retriable FieldNotLoaded here,
   not as whatever assertion the filter would trip over first. (Phase 2
   checks too, but the caller never reaches phase 2 when phase 1 fails.)

1. prefix = ExtractSharedFilterPrefix(plan->plan_node_)
   Strict on shape: the chain above VectorSearchNode must be exactly what
   phase 2 can rebind -- nothing, or one SearchGroupByNode. An
   iterative-filter plan keeps its predicate *above* the vector search
   (IterativeFilterNode -> VectorSearchNode -> MvccNode), so a lenient walk
   would return the bare MvccNode and compute an MVCC-only bitset that
   carries no predicate; this throws instead, before any filter work. The
   proxy never marks such a sub-request shareable, so this is the last line
   of defense, not a path. A plan with no predicate has an MvccNode-only
   prefix and is accepted.

2. Build a QueryContext -- read snapshot pinned once, expression cache
   readable when the request enables it (no sub-expression writes: they would
   be attributed to one branch's plan) -- and execute PlanFragment(prefix).
   -> RowVector{bitset, valid}

   On 2.6 the dedicated prefix helper drives the task to completion, asserts
   that this prefix emits exactly one batch, and returns that RowVector
   directly. It does not concatenate or rebuild the raw and validity bitmaps.

3. Package the RowVector together with the derived QueryContext state (see 3.1)
   and the bytes the filter read (see 2, "Per-branch attribution") into a
   SharedFilterBitsetResult and hand ownership to Go.
```

**Phase 2 — `SegmentInternalInterface::SearchWithBitset(plan, phg, bitset_result, ...)`**, run once per
branch, concurrently:

```
1. Check the O(1) invariants. `bitset_result->segment_id` must name this
   segment; violating that ownership contract throws. The shared bitset must
   be non-null and the visible-row bound (`active_count`) must equal this
   branch's. If either latter check fails, skip steps 2-3 and evaluate this
   branch's own filter instead. Element-level shape and state are carried
   separately (3.1), so `active_count` is not asserted to be the bitmap length.

2. Build QueryContext_i with this plan's search_info_ and this branch's placeholder_group_.
   Install the derived state carried by the result (see 3.1).
   set_precomputed_bitset(bitset_result->bitset)   // shared, read-only

3. Execute PlanFragment(rebound plan), where the rebound tree replaces
   VectorSearchNode's source with a PrecomputedBitsetNode. The rebind accepts
   exactly the shapes ExtractSharedFilterPrefix accepted, and throws on
   anything else. The tree is built once per branch plan and cached on that
   plan, then reused by every segment of the task -- not rebuilt per branch per
   segment (see D7a).

4. Return QueryContext_i->get_search_result() as a single leaked SearchResult*,
   with the filter's phase-1 bytes added on whichever branch claims them first.
```

Phase 2 deliberately does **not** re-verify that the branch's filter subtree matches the one the bitset
came from. Such a check means rendering the expression tree with `ToString()` once per branch per
segment; for the multi-kilobyte predicates this design targets that is pure overhead in the correct
case, and predicate equality is already established by the proxy's grouping hint.

Both phases pass the same MVCC timestamp and TTL context by construction: branch requests inherit them
from one parent request, so the shared result does not copy and re-check those values. The normal
pipeline also applies mutations before advancing tsafe, as described below. A growing row whose insert
timestamp is above the pinned snapshot remains outside both phases; if the visible-row bound nevertheless
changes between them, the check detects it. A missing bitset or a different visible-row bound
**degrades rather than throws**: the branch evaluates its own filter, costing the sharing but never the
query.

A **null bitset is a sentinel, not a failure.** Phase 1 leaves it null when the segment had no active
rows at all, and a branch on that same snapshot short-circuits to an empty result at its own
`active_count == 0` check, which sits *before* reuse is considered — so the sentinel does not reach the
warning on the path it was produced for. Reaching that warning with a null bitset and a non-zero branch
bound means the shared result is unusable, and the per-branch fallback is the safe behavior. The
admission check (`check_search`) runs for every branch either way, so a field that stopped being loaded
still surfaces as the retriable `FieldNotLoaded` and not as a search against a missing column.

**The ordering this rests on.** That phase 1 and phase 2 see the same rows is not a property of segcore
alone; it depends on the pipeline applying a message pack — inserts, and delete forwarding to workers —
*before* advancing tsafe (`delete_node.go`: `ProcessDeleteBatches` at :93, then `UpdateTSafe` at :104),
combined with the delegator waiting for `tsafe >= guarantee` before it pins the MVCC timestamp and fans
out (`waitTSafe`, `delegator.go:637-644`). If tsafe were ever advanced past an unapplied operation, a
branch could observe `active_count + 1` at the same MVCC timestamp.

Worth being explicit about the limit of the guard here: **deletes do not change `active_count`** — it is
derived from insert timestamps — so a delete applied between the phases would produce the same count
with a different mask, which no O(1) check can detect. Only the apply-before-tsafe ordering rules that
out. The same ordering is what makes two-stage search's stage-1 → stage-2 reuse sound, so it is a shared
dependency rather than one this design introduces.


Concurrency safety across the N phase-2 calls: each has its own `QueryContext` and its own
`SearchResult`; the only shared object is the bitset result, which is read-only after phase 1 (see 3.3).

#### 3.1 Derived query state carry-over

This is the subtlest part of the design and the most likely source of bugs. Evaluating the filter
subtree writes **side-effect state onto the `QueryContext`**, not just the returned bitset. Running it
once means only the producing `QueryContext` receives that state; it must be explicitly propagated to
the other branches.

Known writer today:

| Writer | State written |
|---|---|
| `PhyElementFilterBitsNode` (`ElementFilterBitsNode.cpp:102-134`) | `set_array_offsets`, `set_active_element_count`, `set_struct_name`, `set_bitset_is_element_level(true)` |

`PhyVectorSearchNode::GetOutput` reads `bitset_is_element_level()` to pick between the element-level and
the row-level path (`VectorSearchNode.cpp:128-146`). Dropping it would silently change which rows are
searched.

**`all_rows_visible` is the writer that is deliberately *not* on that list**, and the reason has to be
written down, because `PhyMvccNode` does set it (`MvccNode.cpp:94`) and `PhyVectorSearchNode` does read
it, to take an empty-`BitsetView` fast path. It cannot fire inside a group, and every link in the chain
is a precondition of grouping rather than a coincidence: the proxy marks a sub-request shareable only
when it carries a predicate; phase 1 accepts only a plan with an extractable prefix, which is what a
predicate produces; with a predicate the prefix is `FilterBitsNode → MvccNode`, so `MvccNode` has a
source and `is_source_node_` is false; and `MvccNode.cpp` sets `all_rows_visible` only on its
**source-node** sealed fast path. `SharedFilterBitsetResult` therefore does not carry the flag; a
grouped phase-1 run cannot set it. The segcore API also accepts a predicate-less plan whose prefix is a
bare source `MvccNode`; omitting the flag for that direct-API shape only gives up the empty-`BitsetView`
fast path because the returned bitmap already represents the visible rows. It does not change which
rows are searched.

> Branch note (2.6). There, the element-level state is written by `VectorSearchNode` itself, i.e.
> *above* the shared prefix, so each branch writes its own; and `all_rows_visible` is unreachable for
> the same reason as above. `SharedFilterBitsetResult` on 2.6 is consequently the bitset, segment
> identity, visible-row bound and storage-cost accounting only, with no `CaptureFrom` / `ApplyTo` at all.

`SharedFilterBitsetResult` carries the element-level state alongside the bitset, with `CaptureFrom` /
`ApplyTo` as the only way it moves. Keeping it on the same struct rather than in a parallel type means
there is exactly one thing to hand between the phases, and adding a new side effect to a filter-subtree
operator forces a visible edit to that struct rather than a silently missing copy.

**Known trade-off: this copies the coupling rather than removing it.** Filter-subtree operators still
communicate with `PhyVectorSearchNode` / `PhySearchGroupByNode` and the visitor's assertions through
side effects on `QueryContext`, and the only thing keeping `CaptureFrom` complete is the comment on the
struct plus the table above. A new `set_xxx` in a filter operator compiles fine and silently breaks the
grouped path while leaving the ungrouped path correct — the worst shape of bug this design can produce.

Two things would fix it properly, both deferred:

- *Structural (P1).* Make the prefix output self-describing: carry these fields next to the bitset in the
  operator's output and have consumers read them from their input instead of from the context, so a new
  side effect has to go through the type. This changes the operator output shape and is a refactor of its
  own.
- *A test that fails instead of a comment that is read.* `QueryContext` has 12 setters before this
  design adds `set_precomputed_bitset`, and they fall into three classes: the 4 element-level ones that
  are captured, `set_all_rows_visible`, which is deliberately not captured because a group cannot reach
  it, and the 7 that are not filter state at all. Recording which setters fired during phase 1 and
  asserting that set is a subset of {captured} ∪ {the 7} — with `set_all_rows_visible` in neither, so
  that a firing fails the test rather than being silently tolerated — would force a new setter to be
  classified rather than forgotten. Not implemented; the current tests enumerate the known fields by
  hand and share the weakness described above.

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
is exactly the extraction point of `ExtractSharedFilterPrefix`, and the two walk the chain above
`VectorSearchNode` with the same acceptance rule (`VectorSearchNode` itself, optionally under one
`SearchGroupByNode`; anything else throws), so they are symmetric by construction. The lenient
`ExtractFilterOnlyPlan` is not symmetric with `rebind` — it walks past an `IterativeFilterNode` — and
is therefore not used on this path.

The rebound tree is built **once per branch plan and cached on that plan**, behind a thread-safe
once-init, and every segment of the task reuses the same tree. It can be shared because it carries no
data: `PrecomputedBitsetNode` holds no bitset — the bitset reaches the operator through the
`QueryContext` — so the tree is a pure function of that branch's own parsed plan, tied to no segment
and to no sibling. Rebuilding it per branch per segment instead would mean `branches × segments`
rebuilds of a multi-node tree for one task, each drawing ids from the process-wide
`PlanNodeIdGenerator`; the cache removes both the rebuilds and the contention (D7a).

#### 3.3 Bitset sharing safety

`PhyVectorSearchNode::GetOutput` (`VectorSearchNode.cpp:120-215`) only **reads** the bitset on the
row-level path: it constructs a `BitsetView` over `col_input->GetRawData()` and hands it to
`vector_search`. The element-level path calls `RowBitsetToElementBitset`, which allocates a new bitmap
rather than mutating the input.

Do not rely on that invariant holding forever. The current API passes a shared `RowVectorPtr` under a
read-only contract, and the element-level branch builds its own derived bitmap as it does today. If a
future consumer needs to mutate the input, it must first take its own copy rather than weaken that
contract.

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
Attribute each branch's own I/O to itself, and the prefix's to exactly one branch — whichever claims it
first, via an atomic flag on the shared result, since the branches run concurrently and no branch is
privileged. Say so in a comment: otherwise a future reader doing cost attribution silently gets the
prefix N times or zero times.

**Failure isolation.** Any branch throwing fails the entire grouped call. Per-branch status on the wire
would drag in error propagation, partial `SubResults`, and delegator-side partial handling for a benefit
that does not justify it. That is the rule inside the segment and inside the worker. The delegator adds
one more, because partial results are decided above it: a grouped execution that absorbed a worker
failure is re-run one branch at a time, so each branch's partial-result evaluation is its own. See D5.

## Correctness Guarantees

1. **Identical segment set.** All sub-requests of one hybrid search already execute over the same pinned
   snapshot and the same MVCC timestamp (`delegator.go:617`), and `organizeSubTask` derives the
   segment→worker assignment from that same snapshot. Grouping does not change which segments are
   searched.
2. **Identical bitset by construction.** Byte-identical predicate + identical segment + identical MVCC
   timestamp + identical TTL context ⇒ the prefix is a pure function of inputs that are equal.

   What enforces each half is worth being exact about, because they are enforced in different places.
   The *predicate* half is enforced in Go: the proxy marks sub-requests parsed from identical
   `(Dsl, expr_template_values)` with the same `filter_sharing_group`, and the delegator groups on that.
   Segcore does **not** re-check it (see D8). The *snapshot* half — MVCC timestamp and TTL context — is guaranteed
   structurally: `internal.SubSearchRequest` carries no timestamp or TTL fields at all, so every branch
   of one hybrid search inherits `mvcc_timestamp`, `collection_ttl_timestamps` and
   `entity_ttl_physical_time` from the parent `SearchRequest`. `buildSharedFilterBranches` copies them,
   and a unit test pins that.

   Segcore additionally checks segment identity and keeps the filter and branch visible-row bounds
   equal. That row-bound check is not a replacement for the structural snapshot guarantee above;
   master and 3.0 carry element-level shape and state separately.
3. **Required result invariant: bit-for-bit equality with the unshared path.** The design preserves the
   candidate set for every branch, so the verification target is exact equality rather than a similarity
   check. The full differential corpus remains pending; see Test Plan.
4. **Safe degradation.** Every rejection path — predicate mismatch, missing filter subtree, iterative
   filter, vector clustering key, config disabled, or an NQ-budget singleton — falls back to the existing per-sub-request
   execution, and so do the two paths that reject a group only once execution has begun: a BM25 branch
   with no data on the shard (D1) and a worker failure absorbed under partial results (D5). There is no
   intermediate state where partial sharing could produce a partially-correct result.
5. **A grouped request cannot under-report its actual NQ through its declarations.** The delegator
   deliberately budgets from the declared per-branch NQ and does not duplicate placeholder parsing.
   Before allocating grouped results or searching a segment, the worker compares each declaration with
   the NQ parsed by segcore. A mismatch fails with `ServiceInternal`; it is not reclassified as user-input
   validation. This guard checks declared-versus-parsed NQ, not the cumulative budget of an arbitrary
   caller that bypasses delegator grouping.

### A pre-existing race, noted but out of scope

`PruneSegments` mutates the shared `sealed []SnapshotItem` in place (`internal/querynodev2/delegator/segment_pruner.go:161-165`,
`sealedSegments[idx] = item`) while running inside each sub-request's goroutine. It is a data race today,
masked only by `queryNode.enableSegmentPrune` defaulting to `false`. Grouping incidentally removes it
*within* a group (one prune per group), but a request containing both a grouped and a singleton group
still races. It is **not addressed by this MEP** (see D9); fixing it means either moving pruning above the
fan-out or giving each sub-request its own copy of the snapshot.

## Test Plan

**Differential correctness (the primary gate).** Run a corpus of hybrid searches twice against the same
data — once with `sharedFilter.enabled=false`, once with `true` — and assert the serialized
`SearchResults` are **byte-identical**. Cover: 2-way and 3-way; dense+sparse and dense+BM25; identical
predicates and deliberately mismatched predicates (must fall back); element-level; group-by;
`ignore_growing` set on one branch only; empty filter; filter matching zero rows; filter matching all
rows; a segment with no active rows (the null-bitset sentinel). The element-level case is the one that
exercises the `CaptureFrom` / `ApplyTo` propagation, since element-level state is now the only thing
carried. The `all_rows_visible` fast path is deliberately *not* on this list: a group always carries a
predicate, so `MvccNode` is never a source node and the flag cannot be set during phase 1. Assert that
it never is, rather than testing that it propagates.

**Segcore unit tests.** `ComputeFilterBitset` + `SearchWithBitset` with N=1 (must equal `Search`), N=2 identical predicates, N=2
mismatched predicates (fallback), a plan with no filter subtree (fallback), and one branch whose vector
field is inaccessible (that branch empty, others correct).

**NQ work-unit budget.** Added source cases cover exact-fit and stable multi-chunk grouping, interleaved
predicate keys, a single oversized or non-positive branch, a non-positive configured budget, and
near-`MaxInt64` arithmetic. They pin the `nq_budget` fallback count to only the shareable branches left
as singletons. A worker case gives one branch a declared NQ that differs from the parsed placeholder
count and asserts `ServiceInternal` before segment search begins. These cases have not been run in the
latest revision.

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
`getSearchCPUExecutor` queueing: the branch-level `errgroup` nests inside the segment-level one, but
both are bounded per task (D7), so in-flight phase-2 cgo calls stay at about the core count instead of
growing to `S x N`. Verify that under a deliberately wide hybrid search — many branches over many
segments — rather than assuming the bound holds.

## Rollout

1. Land behind `queryNode.hybridSearch.sharedFilter.enabled=false`.
2. Enable on one canary QueryNode; run the differential-correctness corpus and inspect fallback reasons
   for a controlled workload whose shareable and unshareable cases are known.
3. Enable cluster-wide; watch `internal_core_search_latency_scalar` and the fallback counter by reason.

## Delivery Phases

| Phase | Scope |
|---|---|
| P0 | 2 branches, row-level, non-iterative, byte-identical predicates — the dense + BM25 pairing that motivates this design |
| P1 | Element-level; 3+ branches; per-scope grouping so `IgnoreGrowing` no longer splits a group. Structurally already supported; mostly test surface |
| P2 | Common-prefix sharing (`(C) && extra`) via expression-level subtree extraction and delta bitsets |
| P2 | Sharing on collections with vector-clustering-key pruning, by grouping per segment membership (below) |

P2 common-prefix sharing is a materially different problem — it needs a common-subexpression analysis
over the predicate tree and a way to apply the residual conjunct to an already-computed bitset. It
should be a separate MEP.

Grouping under vector-key pruning is smaller but blocked. Today such collections are not grouped at all
(§1), whether or not `enableSegmentPrune` is currently on, because each branch's query vector prunes to
a different segment set `S_i`. The precise way to share anyway is to group by **segment membership**
rather than by branch: prune once per branch, then partition the segments by which branches want them —
`S_A ∩ S_B` runs as a group of `{A, B}` with one
filter evaluation, `S_A \ S_B` as `A` alone, `S_B \ S_A` as `B` alone. A hybrid search has two or three
branches, so the partition count stays small; the cost is one request per partition per worker instead
of one per group. Searching the union with every branch would be simpler but would make each branch
pay for segments its own pruning had excluded. This waits on #53356: pruning currently rewrites the
shared snapshot in place, and per-branch pruning needs it to produce per-branch sets instead.

## Design Decisions

Recorded because each was a real fork in the road.

### D1. Every branch gets the per-branch request rewrites, not just branch 0

`sd.search` rewrites `req.Req` in two places — search-parameter optimization
(the AutoIndex query hook plus Knowhere per-index defaults), and the
managed-function preparation that turns a BM25 branch's VARCHAR placeholder into
an IDF sparse vector — and both now run for every branch. Missing the second one
makes a BM25 branch reach segcore with the client's raw text, which segcore
rejects outright.

The BM25 preparation also produces a *decision*, not just a rewrite: with no
data on the shard yet (`avgdl <= 0`) the branch is skipped and returns empty.
That is a per-branch outcome that a group cannot express, so a group in which
any branch — branch 0 included — would be skipped returns
`errSharedFilterUngroupable` and the delegator re-runs its members as
singletons. The empty BM25 branch comes back empty and its siblings proceed,
exactly as today.

For a grouped request, both rewrites run over the complete `[req] + extras`
branch list **concurrently**, including branch 0, under one `errgroup` bounded
by `min(branch count, core count)`. Branch 0 is no longer a serial head barrier,
and each branch's output is stored by index; the
`IsTopkReduce` / `IsRecallEvaluation` OR-back described in §2 happens after the
group joins, so the input-snapshot semantics are untouched by the concurrency.
This exposes nothing new: IDF construction (`prepareSharedFilterBranchFunctions`)
and search-parameter optimization (`optimizeSearchParams`) already run concurrently
across requests on every query node, and before grouping these same N branches
ran them on N independent goroutines. Serializing them here would instead add
the sum of the branches' hook and IDF latency to the very path this design
exists to shorten.

An ordinary singleton still calls the existing rewrite directly, without a new
goroutine or derived context. In grouped mode a real error cancels sibling work
and takes precedence over the existing per-branch BM25 skip decision; the skip
still causes the same singleton retry after the group joins.

### D2. Two-stage search composes with grouping rather than being excluded by it

Stage 1 is a filter-only pass over the same subtree this design shares, so a
grouped request runs it once for the whole group. A group qualifies if any
branch does; one stage-1 pass then serves every member.

Stage 2 itself is decided **per branch**. Stage-2 semantics tell the query
hook the filter's selectivity is already known (`WithFilterKey=false`) and
suppress `IsTopkReduce`; that is only right for a branch two-stage search was
built for (`topk >= twoStageSearch.minTopk` and
`search_type == PURE_ANN_SEARCH_WITH_FILTER`). A co-grouped branch that would
not have qualified on its own — a BM25 branch next to a dense one — still
shares the group's stage-1 bitset but is optimized with ordinary parameters
(`branchQualifiesForTwoStage`). An ungrouped request reaches stage 2 only by
qualifying, so its behavior is unchanged.

Whether stage 2 then reads the bitset back from the expression result cache or
recomputes it is configuration-dependent and orthogonal to this design:
`queryNode.exprCache.enabled` defaults to `false`, and even when enabled the
default `admissionThreshold=2` rejects a first-seen predicate's stage-1 `Put`
(`ExprCache.cpp`) — which is exactly the per-request-unique-literal workload
this design targets. The once-per-group guarantee holds either way.

### D3. `enable_expr_cache` is forwarded; `filter_only` is not a parameter here

Whether a bitset is evaluated or read from `ExprResCacheManager` is orthogonal
to sharing it, so phase 1 forwards the flag. `filter_only` selects a different
*output shape* — a matched-row count with the bitset discarded — which the
existing `AsyncSearch` path already serves.

### D4. Two cgo phases, not one grouped call returning N results

`AsyncSearchWithBitset` returns a single leaked `SearchResult*` exactly as
`AsyncSearch` does, so result ownership is unchanged. It also keeps the branch
fan-out in Go: fanning out from inside C++ would mean a job on
`getSearchCPUExecutor` submitting to that same bounded pool and blocking on it,
which deadlocks once every thread holds such a waiter.

### D5. A branch failure fails the group, and the delegator restores per-branch failure semantics under partial results

Inside segcore and inside the worker, any branch failing fails the whole grouped
call. This matches existing behavior — a hybrid search already fails wholesale
when one sub-request fails. Per-branch status on the wire would add wire,
error-propagation, partial-`SubResults`, and delegator-side partial-handling
semantics. One distinction must survive at that level:
an inaccessible vector field is **not** a failure, and that branch returns empty
while its siblings proceed.

That equivalence holds exactly as long as a failed worker fails the request,
which is the default: with `queryNode.partialResultRequiredDataRatio == 1` a
worker failure fails the hybrid search precisely as it did before grouping. It
does **not** hold below 1, where the delegator's `executeSubTasks` absorbs a
failed worker and answers from the ones that returned. Absorbing it for a group
would hand the partial set to **every branch of the group** although only the
failing branch had earned it: a branch that would have completed on its own now
silently returns fewer rows because a sibling shared its RPC, and the proxy
cannot tell the two apart. So a grouped request whose execution absorbed at
least one worker failure returns `errSharedFilterUngroupable`, and the group is
re-executed one branch at a time, each branch evaluating partial results on its
own, restoring the pre-grouping failure and partial-result semantics. The cost is a re-execution, paid
only on the failure path and only in partial-result mode. That re-execution also repeats work on
healthy workers whose grouped response already returned. Reusing only the successful coverage while
retrying the missing coverage would have to retain worker target/scope/segment provenance and the
prepared per-branch requests; master and 3.0 would additionally have to preserve two-stage stage-1
incomplete counts. That is a separate recovery design. Directly demultiplexing the common partial
coverage would instead force one branch's missing workers onto every sibling and change the existing
per-branch best-effort semantics. The other alternative, per-branch status on the wire, is rejected for
the reasons above.

### D6. A grouped task reports the sum of its branches' NQ

`SearchTask.NQ()` feeds the scheduler counter the proxy uses for load
estimation, so a group must report what it actually processes rather than only
branch 0. This is accounting after the group has been formed; it does not by
itself constrain that group. The separate `MaxGroupNQ` split in D7 is what
limits one grouped work unit.

The task histograms follow the same rule. `Done` observes
`QueryNodeSearchGroupNQ` as that same sum and `QueryNodeSearchGroupTopK` as the
**maximum** top-K across branches. A group has no single top-K, and the maximum
is what an NQ-merged task already records for the requests it merged, so both
kinds of group stay comparable inside one histogram instead of one of them
reading as a different unit.

### D7. Grouped work has an NQ budget and task-scoped concurrency bounds

Before task construction, the delegator splits each same-key predicate bucket
into stable chunks whose summed declared NQ is at most `MaxGroupNQ`. A
multi-branch chunk becomes one task; a singleton remains on the ordinary path.
The budget is read once for the grouping pass, and the subtraction-first check
avoids overflowing the cumulative `int64` sum. A non-positive budget, a
non-positive branch NQ, or a branch larger than the budget produces a singleton.
Each chunk is independent, so the shared filter is evaluated once per segment
for each chunk rather than once for the original same-predicate bucket.

Sequential branches would trade the saved filter evaluation for the overlap they
used to have, taking per-segment wall clock from `F + max(A_i)` to `F + ΣA_i`.
This adds no synchronization point: the delegator already joined all
sub-requests before responding.

The fan-out is bounded **per task, not per segment**. `searchSegmentsGrouped`
creates one `semaphore.Weighted` sized to the CPU count and passes it to every
segment's `SearchGrouped(ctx, reqs, limiter)`; a segment acquires one unit around
each phase-2 `SearchWithBitset` and releases it after. A task therefore never has
more than the core count of phase-2 searches in flight, however many segments it
holds and however many branches each of them fans out. A per-segment cap does not
bound that: even after NQ chunking, every segment fans out the branches in its
chunk, so `segments × cap` goroutines and queued C++ futures could still pile up
behind one task if the cap were local to each segment. The shared limiter makes
the bound task-wide.

For grouped requests on master and 3.0 the segment-level `errgroup` is limited to
the core count as well, so at most that many segments sit inside their two-phase
window — the window in which a filter bitset is alive — at any moment. On 2.6,
the outer segment goroutines may enter `DiskCache.Do` concurrently; only the
loaded, pinned callback acquires the task-scoped segment search token around
`SearchGrouped`. Lazy loading therefore consumes its own cache/loader admission,
not a CPU-search token, while the same core-count bound still limits live shared
bitsets and two-phase searches. The ungrouped path is untouched: one branch, no
new limiter, the same segment fan-out it has today.

The bound is sized to the C++ search executor's core-based width. Its purpose is
to cap queued futures and simultaneously live bitsets per task; the performance
plan must still measure any scheduling or throughput effect.

There is deliberately no byte-based admission controller for the branch ×
segment result matrix. Within one chunk it scales as
`S × Σ(NQ_b × TopK_b)`: every branch has to be reduced across every segment, so
its per-segment results are held until that branch's reduce consumes them.
`MaxGroupNQ` bounds only `ΣNQ_b` for one grouped task; it does not bound `S`,
per-branch top-K, retained bytes, or GPU-kernel work, and it is not a node-global
limit. The scheduler still sees the task's NQ as the same sum (D6), so its
accounting describes the work unit the delegator formed without being the
mechanism that formed or admitted it. A byte or streaming bound would require a
different reduce contract and remains out of scope.

The same core-count bound applies to the per-branch reduce on the worker and at
the delegator, which likewise run concurrently — before grouping, each
sub-request reduced on its own goroutine, and a serial reduce would have made
grouped latency grow with the sum of the branch reductions. The worker's branch
reduce runs on the `errgroup`'s derived context, so a failing branch cancels its
siblings' reduce rather than leaving them to finish work nobody will read.
A singleton reduces directly inside the existing outer future; multi-branch
reduce keeps the bounded fan-out. Fallback metric increments are accumulated by
reason and applied in batches without changing their per-branch counts. The
temporary `branchTask` is only a reduction view and carries the fields that the
reducer consumes; unused scheduling and segment-lifecycle state is omitted on
master, 3.0 and 2.6.

### D7a. The plan node id generator is atomic, and the rebound plan is built once per branch

Every rebuilt plan node draws an id from `PlanNodeIdGenerator`, a process-wide counter that was a plain
`int`. The race predates this design — every request's plan build goes through the same generator,
concurrently — and phase 2 still draws from it, because each branch's rebound tree is built at run
time. So the counter is made `std::atomic`: the ids only have to be distinct within a plan, and the
atomic removes the data race.

What removes the *volume* of those draws is a separate change. The rebound tree is built **once per
branch plan**, cached on that plan behind a once-init, and reused by every segment of the task, instead
of being rebuilt per branch per segment. That is sound because the tree carries no data:
`PrecomputedBitsetNode` holds no bitset — the bitset reaches the operator through the `QueryContext` —
so the cached tree is a pure function of that branch's own parsed plan, tied to no segment and to no
sibling. Sharing one rebound plan *across branches* would be a different and wrong thing, since each
branch's tree is its own plan with its own `VectorSearchNode`; caching per branch plan is not that, and
it turns `branches × segments` rebuilds into `branches`.

### D8. Phase 2 does not re-verify the filter subtree, and degrades rather than throws

Comparing the subtree's `ToString()` would render a multi-kilobyte expression
once per branch per segment, pure overhead in the correct case, while predicate
equality is already established by the proxy's grouping hint. The O(1)
checks that remain guard the mistakes that would corrupt results.

Predicate identity is established before grouping, and per-branch snapshot
fields are unrepresentable in the proto: every branch receives the same parent
MVCC timestamp and TTL context. `SearchWithBitset` therefore does not duplicate
or compare those snapshot values. The remaining reuse checks require a non-null
bitset and equal filter/branch visible-row bounds; master and 3.0 carry
element-level shape and state separately. If the shared result is unusable, the
branch evaluates its own filter instead, costing the sharing rather than failing
the search.

### D9. The `PruneSegments` data race is out of scope

It mutates the shared `sealed` snapshot from each sub-request's goroutine, is
pre-existing, and is masked by `queryNode.enableSegmentPrune` defaulting to
false. Grouping only narrows it — a request holding both a grouped and an
ungrouped sub-request still races. Tracked as
[#53356](https://github.com/milvus-io/milvus/issues/53356) and fixed separately.

### D10. Grouped requests do not use the 2.6 streaming search task

Branch-specific: 2.6 only. With `queryNode.useStreamComputing=true`,
`SearchSegments` builds a `StreamingSearchTask`, whose `Execute` override reduces
segment results as they stream in rather than after the last one. That override
handles `req` and nothing else — branch 0 — so a grouped request on it would
execute one branch, emit no `SubResults`, and be rejected by the delegator's
response-shape check. Rather than teach the streaming reduce a branch dimension,
a request carrying `extra_filter_sharing_reqs` always takes the regular
`SearchTask`.

The trade is deliberate and narrow: a grouped request gives up the streaming
reduce and keeps the filter sharing; every other request keeps the streaming
reduce. The two optimizations attack different costs — one the reduce tail, the
other repeated filter evaluation — and this design exists for the second. Master
and 3.0 have no streaming search task, so this decision is inert there.

## Rejected Alternatives

### Merge sub-requests into a single plan with `repeated VectorANNS`

This is a larger modeling change. `planpb.PlanNode` carries
`output_field_ids`, `dynamic_fields`, `scorers`, `plan_options`, `score_option`, and
`querynode_function_chains`, all of which may differ per sub-request; merging forces a merge policy for
each. The proxy's `tryGeneratePlan` would also have to learn to emit a multi-ANNS plan. In exchange the
execution layer still has to run one vector search per branch. Keeping N independent
plans and sharing only execution avoids those merge policies.

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
   default — `pwrite`s the bitset to a per-segment slot file and `pread`s it back. Using either for a
   single read inside one request adds work that the request-scoped handle avoids.
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
| Grouping, NQ-budget chunking, request build, result demux, per-branch param optimization | `internal/querynodev2/delegator/shared_filter.go` |
| Grouped fan-out; two-stage guard; field forwarding | `internal/querynodev2/delegator/delegator.go` |
| Branch expansion, declared-vs-parsed NQ guard, per-branch reduce, `SubResults` assembly | `internal/querynodev2/tasks/search_task.go` — `executeSharedFilter`, `branchTask`, `reduceSegmentResults` |
| Per-segment prefix + branch fan-out under the task's limiter | `internal/querynodev2/segments/segment.go` — `LocalSegment.SearchGrouped(ctx, reqs, limiter)` |
| Branch-major segment fan-out; the task-scoped fan-out limiter | `internal/querynodev2/segments/search.go` — `searchSegmentsGrouped*`, `SearchHistoricalGrouped`, `SearchStreamingGrouped` |
| cgo wrappers | `internal/util/segcore/segment.go`, `responses.go` — `ComputeFilterBitset`, `SearchWithBitset`, `SharedFilterBitsetResult` |
| cgo entry points | `internal/core/src/segcore/segment_c.{h,cpp}` — `AsyncComputeFilterBitset`, `AsyncSearchWithBitset`, `DeleteSharedFilterBitsetResult` |
| Two phases | `internal/core/src/segcore/SegmentInterface.{h,cpp}` — `ComputeFilterBitset`, `SearchWithBitset` |
| Two execution phases | `internal/core/src/query/ExecPlanNodeVisitor.{h,cpp}` — `get_shared_filter_bitset_result`, `SetPrecomputedBitset` |
| Shared bitset payload + derived state | `internal/core/src/query/SharedFilterBitsetResult.{h,cpp}` |
| Precomputed bitset on the context | `internal/core/src/exec/QueryContext.h` — `set_precomputed_bitset` |
| Plan rebinding, built once per branch plan and cached on it | `internal/core/src/query/PlanProto.cpp` — `ProtoParser::RebindToPrecomputedBitset` |
| Grouped two-stage search | `internal/querynodev2/delegator/shared_filter.go` — `shouldUseTwoStageSearchForGroup`; `delegator_twostage.go` |
| Source operator | `internal/core/src/plan/PlanNode.h` — `PrecomputedBitsetNode`; `internal/core/src/exec/operator/PrecomputedBitsetNode.{h,cpp}` |
| Config | `queryNode.hybridSearch.sharedFilter.enabled` (default `false`); existing `queryNode.grouping.maxNQ` (default `64`) |
| Metrics | `milvus_querynode_hybrid_shared_filter_fallback_total{reason}` |

Coverage present from earlier revisions includes segcore cases for the two phases, iterative-plan
rejection, unloaded-field admission, storage-cost claim, null-bitset sentinel, and rebound-plan cache;
Go coverage includes proxy grouping and classification, delegator grouping and per-branch re-runs,
worker fan-out and envelope handling, and the 2.6 zero-copy lifetime. The latest revision has received
static inspection only; builds and tests have not been rerun. The differential corpus, broader fault
injection, race-detector run, and performance measurements remain pending.

## References

- `internal/proxy/task_search.go:551-702` — per-sub-request plan construction
- `internal/querynodev2/delegator/delegator.go:617-682` — snapshot pin and advanced fan-out
- `internal/querynodev2/tasks/search_task.go:402-440` — `SearchTask.Merge` and why hybrid never merges
- `internal/core/src/query/PlanProto.cpp:702-765` — vector plan chain construction
- `internal/core/src/query/PlanProto.cpp:1609` — `ExtractFilterOnlyPlan`, and the strict
  `ExtractSharedFilterPrefix` / `RebindToPrecomputedBitset` pair beside it
- `internal/core/src/query/ExecPlanNodeVisitor.cpp:394-465` — `filter_only_` execution mode
- `internal/core/src/exec/operator/VectorSearchNode.cpp:120-215` — bitset consumption
- `internal/core/src/exec/operator/MvccNode.cpp` — MVCC/delete mask and `all_rows_visible`
- `internal/core/src/exec/operator/ElementFilterBitsNode.cpp:102-134` — element-level context state
- `internal/querynodev2/delegator/delegator_twostage.go` — prior art for filter/search stage separation
- `pkg/util/paramtable/component_param.go` — `QueryNodeCfg.MaxGroupNQ` / `queryNode.grouping.maxNQ`
- `docs/design-docs/design_docs/20260602-expression-result-cache.md` — `ExprResCacheManager`
