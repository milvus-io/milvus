# MEP: Search Embedded Aggregation

- **Created:** 2026-04-13
- **Author(s):** @MrPresent-Han
- **Status:** Draft (revised 2026-04-22)
- **Component:** Proxy, QueryNode, Segcore, SDK
- **Related Issues:** TBD
- **Released:** TBD

## Summary

Extend Milvus vector search to support **per-group aggregation metrics**, **multi-field grouping**, and **hierarchical (nested) grouping** in a single request, so that one search call can return "top-K groups → per-group metrics (avg / sum / count / max / min) → per-group top-K hits (optionally sorted) → sub-groups". This is the combined capability that Elasticsearch exposes through nested `terms` + `top_hits` + metric sub-aggregations.

The segcore foundation for multi-field composite-key grouping is already landed (milvus-io/milvus#48970, `feat: support multi-field composite group_by for vector search`). This MEP defines the full feature surface on top of that foundation: user-facing nested API, leaf-level metric aggregations, within-bucket document sort, bucket-level ordering, and a proxy-side reconstruction of arbitrarily deep grouping trees.

## Motivation

### Gap to Elasticsearch

Milvus today supports single-field `group_by_field` + `group_size` for grouping vector search results, but lacks several capabilities that modern vector+analytics workloads expect from Elasticsearch:

| # | Capability | ES | Milvus master | milvus-io/milvus#48970 | Gap |
|---|------------|----|--------------|----------|-----|
| R1 | Single-field grouping | yes | yes | — | covered |
| R2 | Per-group top-K hits | yes (`top_hits.size`) | yes (`group_size`) | — | covered |
| R3 | Multi-field flat (`multi_terms`) | yes | no | segcore only | Go / Proxy / SDK |
| R4 | **Per-group metrics** (avg / sum / count / max / min + stats / cardinality / percentiles) | yes (20+) | **no** | no | **core gap** |
| R5 | Non-vector sort **inside** a group (`top_hits.sort`) | yes | no | no | uncovered |
| R6 | Hierarchical (nested) grouping | yes (nested `terms`) | no | no | uncovered |
| R7 | **Bucket-level ordering** (`terms.order` / `multi_terms.order`) | yes | no | no | uncovered |

R4 is the dominant gap. The most common vector-search analytics requests — "for each document, return the top-2 most similar chunks **and** the average similarity + chunk count of that document" — are impossible to express today without issuing two separate requests and stitching results on the client.

**R5 vs R7 — two independent sort dimensions, often conflated.** ES exposes them through two different parameters, and we follow the same split:

- **R5 (document sort, inside a bucket)** — `top_hits.sort` in ES. Controls the order of the `top_hits.size` hits returned **within** one bucket. Display-only.
- **R7 (bucket sort, across buckets)** — `terms.order` / `multi_terms.order` in ES. Controls the order of the top-K buckets returned in the response, and — crucially — selects **which** buckets land in the top-K. Can sort by `_count`, `_key`, or any sub-metric alias.

### Concrete Use Cases

1. **E-commerce faceted search.** For each brand, return the 3 most relevant products and the average / minimum price of all matched products in that brand.
2. **Document retrieval with signals.** For each document, return the top-2 most similar chunks and the average similarity, max similarity, and total chunk count of all chunks that fell in the retrieval pool.
3. **Category → brand drill-down.** Group top-5 categories by total revenue; inside each category, group top-3 brands by average rating; inside each brand, return the 3 cheapest items.

### Design Goals

1. **ES semantics alignment.** Each level follows ES `multi_terms` (flat tuple keys per level). Hierarchy is expressed via a recursive `sub_group` just like ES nested `terms`.
2. **Zero segcore rewrite for hierarchy.** Segcore keeps a single, flat composite-key contract; nested execution is reconstructed at the proxy. This is the same trade-off ES itself makes when splitting aggregation responsibilities between shards and the coordinator.
3. **Strict layer isolation.** Segcore and the QN/Delegator know nothing about metrics, ordering, or levels. The proxy owns the entire aggregation model.
4. **Reuse the query aggregation framework.** Metric accumulators (`AggregateBase`, type checking) already exist in `internal/agg/`. This MEP extends their use to the search path without reinventing them.
5. **Honest approximation.** Where exact semantics are incompatible with ANN early-stop, document the approximation and expose a tunable knob rather than pretend to be exact. Elasticsearch itself already operates this way at the shard level.

## Public Interfaces

The user-facing API uses a **single recursive class** `GroupBy`, plus a small `TopHits` helper. Hierarchy is expressed by chaining `sub_group`. `metrics` is a `Dict[alias, spec]`; `order` and `top_hits.sort` are explicit lists for deterministic priority.

### New classes (PyMilvus)

```python
class GroupBy:
    fields:    List[str]           # required — composite key for this level
    size:      int                 # required — max buckets returned at this level
    metrics:   Dict[str, dict]     # optional — alias → {"op": "<op>", "field": "<name>|_score|*"}
    order:     List[dict]          # optional — [{"<key>": "desc"}, ...] (priority-ordered)
    top_hits:  TopHits             # optional — if absent, this level returns buckets only
    sub_group: GroupBy             # optional — recursive child level

class TopHits:
    size: int                      # required — how many hits per bucket
    sort: List[dict]               # optional — intra-bucket document sort criteria
```

`metrics` key is the **alias**: it is also what `order` references, and what appears in the response. Supported ops in Phase 1: `avg`, `sum`, `count`, `max`, `min`. The special field `_score` refers to the vector similarity distance. `count` follows SQL/ES semantics: `count("*")` counts **all rows** in the bucket; `count("field")` counts **only rows where `field` is non-null** — giving distinct, useful meaning on nullable fields. For non-nullable fields the two coincide. `avg` / `sum` / `max` / `min` skip nulls.

### New `search()` parameter

```python
client.search(
    ...,
    group_by: GroupBy = None,       # replaces / supersedes group_by_field + group_size
    ...
)
```

When `group_by` is set, `limit` must **not** also be set — the root `GroupBy.size` controls the top-level bucket count and each level has its own `size`, so a user-supplied `limit` has no meaningful role. Sending both returns `merr.WrapErrParameterInvalid` with a message pointing users to `GroupBy.size`.

### Examples

**Flat single-level with metrics and bucket ordering:**

```python
results = client.search(
    data=[query_vector], anns_field="embedding",
    output_fields=["name", "price"],
    group_by=GroupBy(
        fields=["brand", "color"], size=10,
        metrics={
            "avg_price": {"avg":   "price"},
            "doc_count": {"count": "*"},
        },
        order=[{"avg_price": "desc"}, {"_count": "desc"}],
        top_hits=TopHits(size=3, sort=[{"field": "rating", "order": "desc"}]),
    ),
)
```

**Two-level nested:**

```python
group_by=GroupBy(
    fields=["category"], size=5,
    metrics={"total_revenue": {"sum": "price"}},
    order=[{"total_revenue": "desc"}],
    top_hits=TopHits(size=2),                        # level-1 also returns docs
    sub_group=GroupBy(
        fields=["brand"], size=3,
        metrics={"avg_rating": {"avg": "rating"}},
        order=[{"avg_rating": "desc"}],
        top_hits=TopHits(size=3, sort=[{"field": "price", "order": "asc"}]),
    ),
)
```

**Three-level nested with pure-aggregation intermediate levels:** omit `top_hits` on a level to skip document return at that level (bucket key + metrics + sub-group only). See `pymilvus-search-agg-api-design-chn.md` §5.5 for the full example.

### Response shape

```json
{
  "groups": [
    {
      "key":     {"category": "electronics"},
      "metrics": {"total_revenue": 48200.0, "avg_score": 0.87},
      "hits":    [{"score": 0.95, "fields": {...}}, ...],
      "sub_groups": [
        {"key": {"brand": "Samsung"}, "metrics": {...}, "hits": [...], "sub_groups": []}
      ]
    }
  ]
}
```

`hits` is absent when `top_hits` is not configured at that level. `sub_groups` is absent at leaf levels.

### Compatibility aliases

- `group_by_field` + `group_size` (legacy) continue to work unchanged — they are **translated to** `GroupBy(fields=[field], size=topk, top_hits=TopHits(size=group_size))` at the proxy boundary.
- `group_by_field_ids` (proto field 17, `repeated int64`, added by milvus-io/milvus#48970) is used only by the legacy SearchGroupBy reduce path (see §5.1). The new SearchAggregation path does **not** use field 17; it carries composite key values inside `fields_data`.
- **Mutual exclusion**: setting both `group_by_field` and `group_by` in the same request is a `ParamError`.

## Approximation Contract

Approximation is accepted **by design**. ANN early-stop means exact semantics are not a goal at this layer — same trade-off as Elasticsearch distributed `terms` aggregation at shard level: partial collection visibility traded for latency and cost. Users who need exact results on the full post-filter collection should issue an independent scalar `query` aggregation request (see §Future Extensions).

### Exactness by output type

| Output | Exact? | Condition |
|--------|--------|-----------|
| Composite `_key` value of a returned row | ✓ | Always correct for rows that reach the proxy |
| Bucket existence | ✗ | Keys whose rows never entered the ANN retrieval pool are invisible |
| Bucket `_count` | ✗ | Counts rows in the retrieval pool, not the full collection |
| Leaf-level metrics | ✗ | Computed over rows returned to the proxy |
| Parent-level metrics (nested) | ✗✗ | Aggregate over child-bucket rows already subject to early-stop loss; bias compounds per level |
| Top-K bucket selection by `order` | ✗ | Orders over observed (already-approximate) values |
| Top-K hits within a bucket | Best-effort | Bounded by ANN pool × early-stop |

### Bias amplification across nesting levels

Approximation is not a single error source — it **cascades**. A parent-level metric aggregates over child-bucket rows that are themselves subject to early-stop loss. Using such a metric as an `order` key turns "the value is biased" into "the winner selection is biased." This is **accepted behavior, not a bug**. Users observing unacceptable bias can:

1. Raise `group_count_safe_factor` to widen the retrieval pool.
2. Fall back to scalar `query` aggregation for exact results.
3. Restructure the query so only leaf-level metrics drive `order`.

### Mitigation knobs

| Knob | Default | Error source | Effect |
|------|---------|--------------|--------|
| `group_count_safe_factor` | 4 | Tuple coverage — small buckets crowded out by dominant ones | Segcore over-fetches `Πsize × factor` tuples; proxy applies `order` / `size` truncation. ES `shard_size` analog. |
| `metric_safe_factor` | 1 (Phase 2) | Per-bucket sample size — metric accumulator sees only rows that cleared early-stop | Widens segcore per-bucket window. Reserved in proto; segcore implementation deferred. |

Both knobs are orthogonal. Neither produces exact results alone; together they reduce both the likelihood and severity of approximation errors at the cost of retrieval work.

## Design Details

### 5.1 Layer isolation — the foundational principle

| Layer | Responsibility |
|-------|----------------|
| **PyMilvus / Go SDK** | Serialize user's `GroupBy` tree into `SearchRequest.group_by` (proto) |
| **Proxy** | Parse `GroupBy` → build `SearchAggregationContext`; build `SearchAggregationInfo` (flat field-id list) to send down; receive raw shard results; reconstruct hierarchy + metrics + ordering locally |
| **QN / Delegator** | Merge growing + sealed segment results for each shard. **Zero awareness** of metrics, levels, or ordering — only does "multi-column group reduce" |
| **Segcore** | Per-segment composite-key group-by (already done in milvus-io/milvus#48970) |

**Two separate paths never mix at the wire:**

| Path | Trigger | Wire carrier | Reduce logic |
|------|---------|-------------|--------------|
| SearchGroupBy (legacy) | `group_by_field_id > 0` | `SearchResultData.group_by_field_value` (field 8) | Single-field dedup |
| **SearchAggregation (new)** | `agg_info != nil` | `SearchResultData.fields_data` (group-by + metric source fields + output fields) | Multi-field composite-key dedup |

No proto translation between the two. Segcore decides behavior from `QueryInfo.group_by_field_id` vs `QueryInfo.agg_info`.

### 5.2 Segcore — already landed in milvus-io/milvus#48970

Key pieces shipped:

- `SearchInfo.group_by_field_ids_` — `vector<FieldId>` replaces `optional<FieldId>`.
- `CompositeGroupKey` (a small inline-storage vector of `GroupByValueType`) + `CompositeGroupKeyHash` using `folly::bits::hashMix`.
- `MultiFieldDataGetter` — type-erases per-field value getters into `std::function<GroupByValueType(int64_t)>`.
- `CompositeGroupByMap` — enforces `group_capacity` (topk) and per-group `group_size` with a `try_emplace + rollback` idiom.
- `AssembleCompositeGroupByValues` — serializes each group-by field as a separate `FieldData` under `SearchResultData.group_by_field_values`.

For the SearchAggregation path, segcore will additionally:

- Accept `QueryInfo.agg_info` (new `SearchAggregationInfo` message).
- When `agg_info` is set, emit group-by values + metric-source fields into `fields_data` instead of `group_by_field_values`. No new C++ types; the existing composite-key accumulator is reused verbatim.

No changes to the composite-key accumulator are needed for Phase 1 — the per-bucket metric window equals `group_size` implicitly, same as the `top_hits` window. Higher-precision windows are tracked under §5.8 as a Phase-2 knob.

### 5.3 QN / Delegator — unified `SearchGroupByReduce`

`internal/querynodev2/segments/search_reduce.go::SearchGroupByReduce` is **upgraded in place**, not forked. A single struct handles both the single-column (field 8) and multi-column (`fields_data`) modes, unified via a `buildKeyExtractors` helper:

```go
func InitSearchReducer(info *reduce.ResultInfo) SearchReduce {
    if info.GetGroupByFieldId() > 0 || info.GetAggInfo() != nil {
        return &SearchGroupByReduce{}          // same struct, unified implementation
    }
    return &SearchCommonReduce{}
}

func buildKeyExtractors(srds []*schemapb.SearchResultData, info *reduce.ResultInfo) []func(int) string {
    if info.GetAggInfo() != nil {
        // multi-column: read composite from fields_data by group_by_field_ids
        return buildMultiFieldExtractors(srds, info.GetAggInfo().GetGroupByFieldIds())
    }
    return buildSingleFieldExtractors(srds)    // single-column: read field 8
}
```

Output is symmetric:

| Mode | Output |
|------|--------|
| Single-column | `GroupByFieldValue` (field 8) preserved |
| Multi-column | `fields_data` preserved; accepted rows only, rejected rows dropped |

The reduce contract at this layer is the same for both modes: dedup PK, enforce `group_size` per composite key, cap at `topK` distinct keys. Nothing about metrics, nested levels, or ordering is visible here. See `qn-reduce-implementation-plan.md` for implementation detail.

### 5.4 Proxy — `SearchAggregationContext` and `SearchAggregationComputer`

A new Go package `internal/proxy/search_agg/` owns the aggregation model.

#### 5.4.1 Context building (`context_builder.go`)

```go
func BuildSearchAggregationContext(
    groupBy *commonpb.GroupBySpec,
    schema  *schemapb.CollectionSchema,
    nq      int64,
) (*SearchAggregationContext, error)
```

Walks the `GroupBy` tree depth-first and produces:

- `Levels []LevelContext` — one per level, root → leaf, with **`OwnFieldIDs` in user-specified order** (required for `_key` lexicographic comparison).
- `GroupByFieldSlotIdx` — fieldID → slot index in each `SearchResultData`'s composite-key column set.
- `FieldsDataSlotIdx` — fieldID → slot index in `fields_data` for metric-source reads and hit materialization.
- `UserOutputFieldIDs` vs `InternalFieldIDs` — lets the proxy send metric-source / sort-key fields to QN without leaking them into the user-visible response.

Validation at build time: metric source fields exist in schema; `order` keys reference a declared metric alias or the reserved `_count` / `_key`; no duplicate field ID across levels; per-level `size > 0`.

#### 5.4.2 The computer (`computer.go`)

```go
type SearchAggregationComputer struct {
    ctx     *SearchAggregationContext
    results []*internalpb.SearchResults  // raw shard results, read-only
}

// One call returns one result tree per query vector. No internal state.
func (c *SearchAggregationComputer) Compute() ([][]*AggBucketResult, error)
```

**Zero-copy row references.** No row is ever copied; the computer uses `RowRef{ResultIdx, RowIdx}` index pairs that point back into the original `SearchResults`. Per-field reads go through fieldID → slot lookups in the context.

**One-pass top-down recursive descent.** For each level, a single sweep over the rows simultaneously accumulates:

1. group key canonicalization → bucket map insert
2. `Count` increment (used by `order: _count`)
3. Per-metric `AggregateBase.Update` for the alias set at that level
4. Top-K heap push if `TopHits` is configured
5. `[]RowRef` collection (only if the level has a `sub_group`)

After the sweep, finalize each bucket's metrics, apply `order`, truncate to `Size`, and recurse into `sub_group` with the per-bucket `[]RowRef`. The recursion is bounded by the user's `GroupBy` tree depth.

#### 5.4.3 Pipeline integration

`internal/proxy/search_pipeline.go` gets a new variant `searchWithAggPipe` consisting of a single `aggregateOp` that takes raw `[]*internalpb.SearchResults` and produces `*milvuspb.SearchResults` carrying new `SearchResultData.agg_buckets` / `agg_topks` proto fields. No intermediate `searchReduceOp` and no `endOp` / `highlightNode` post-processing — the computer assembles the final response directly. Routing picks this pipeline when `searchTask.aggCtx != nil`.

### 5.5 Per-group metrics (R4)

Metrics are accumulated at the proxy over the rows that reached the proxy from QN. This matches ES's scoping: "a metric sub-aggregation sees the documents that matched the query and fell into this bucket." Approximation characteristics are captured in the Approximation Contract section above.

#### 5.5.1 Reuse vs new code

**Reused from `internal/agg/`:**
- `AggregateBase` interface, `SumAggregate`, `CountAggregate`, `MinAggregate`, `MaxAggregate`.
- Type check rules from `internal/agg/type_check.go`.
- `FieldValue` / `FieldAccessor` for physical field reads.

**New in `search_agg/`:**
- `bucketState` — pairs per-bucket `AggregateBase`s with the top-K heap and recursion rows.
- `_score` synthetic source — reads `SearchResults.Scores[rowIdx]` instead of a segcore field, so metrics like `avg(_score)` / `max(_score)` work end-to-end. Restricted to `avg / max / min / sum` in Phase 1.
- Metric proto (see §6) and response serialization.

#### 5.5.2 Phased priority

| Phase | Metric | Partial state | Notes |
|-------|--------|---------------|-------|
| **1** | `count` / `sum` / `avg` / `max` / `min` | `int64` / `double` / `(sum, count)` / value / value | MVP — exactly the ops with associative merge already in `internal/agg/` |
| 2 | `cardinality` | HyperLogLog sketch | HLL merge |
| 2 | `stats` | `(count, sum, min, max)` | elementwise |
| 3 | `percentiles` | t-digest | digest merge |

#### 5.5.3 Parent-level metrics and approximation

Nested `sub_group` parents carrying metrics are supported. Parent metrics aggregate over all child-level retrieval pools — they are subject to bias amplification (see Approximation Contract). Users requiring exact values on the full post-filter collection should fall back to scalar `query` aggregation.

### 5.6 Bucket ordering (R7) and intra-bucket sort (R5)

#### 5.6.1 R7 — `order` selects the top-K buckets

`order` is an ordered list (priority descending). Keys may be:

- `_count` — bucket size (the computer's `Count` field)
- `_key` — composite grouping value, compared lexicographically over `OwnFieldIDs` in user-specified order
- any metric alias declared in the same level's `metrics`

Multiple criteria give explicit tiebreakers; each has independent `asc`/`desc`.

Default (when `order` is omitted): keep today's behavior for `group_by_field` — buckets ordered by best hit score. This is applied as an implicit `order=[{'_score': 'desc'}]` where `_score` reads the best (highest) hit score in the bucket. Milvus normalizes score direction at the kernel level so that higher = more similar regardless of metric type (L2 / IP / cosine), so a single `desc` default is uniform across metrics. This default is an **internal implicit rule** applied only when the user omits `order`; it is independent of the Q4 restriction on user-supplied explicit `_score` order keys (which still require `avg/max/min(_score)` wrapping).

#### 5.6.2 R5 — `TopHits.sort` orders docs inside a bucket

`TopHits.sort` applies only to the `TopHits.size` docs returned per bucket. It does **not** change which docs entered the metric accumulator or the bucket count — same display-only semantics as ES `top_hits.sort`. Phase 1 allowed sort keys: numeric + varchar + `_score`.

#### 5.6.3 Early-stop impact

For `order` keys other than `_score`, arrival-order early-stop is no longer safe (a small bucket could become the winning bucket later in the iterator). Mitigated via `group_count_safe_factor` — see Approximation Contract.

### 5.7 Nested grouping (R6) — proxy-side reconstruction

#### 5.7.1 Decision: no segcore nesting

A tree-shaped partial-result schema, tree-shaped reduce, per-parent accumulators, and a new plan node type in segcore would be a large engine change, and the value-to-cost ratio is poor: vector-search analytics rarely exceed 2–3 grouping levels, and ES itself caps effective depth via `search.max_buckets` (default 65535 ≈ 4 levels at `size=10`).

#### 5.7.2 Design: flatten at the proxy

The proxy flattens all user levels into a single flat composite-key request for QN/segcore. The field id list sent down is the union of every level's `OwnFieldIDs`, deterministically ordered. Inflated capacity:

```
segcore_topk = Π(level.size for each level) * group_count_safe_factor
               (hard cap: 65535, aligned with ES max_buckets)
```

Default `group_count_safe_factor = 4`. The proxy then reconstructs the tree per `computeLevel`, using the per-bucket `[]RowRef` pool for each parent's child recursion — no re-retrieval, no second query-agg trip.

**Overflow rejection.** If `Π(level.size) × group_count_safe_factor > 65535`, the proxy rejects at parse time with `merr.WrapErrParameterInvalid`, naming the product, factor, and cap. No silent clamp. QN/Delegator additionally enforces its existing per-request memory budget as a defense-in-depth guard against pathological nested requests that somehow bypass proxy validation.

**Max nesting depth.** Recursive `GroupBy.sub_group` is capped at `MAX_NEST_DEPTH = 10`. Requests exceeding this are rejected at parse time with `merr.WrapErrParameterInvalid` — guards against stack overflow and resource exhaustion in the proxy `computeLevel` recursion.

### 5.8 Approximation knobs — proto placement

Knob semantics and trade-offs are documented in the Approximation Contract above. Proto placement: `SearchAggregationInfo.group_count_safe_factor` (int32, default 4). `metric_safe_factor` is reserved in the proto but its segcore implementation is deferred to Phase 2; the Phase-1 release treats it as a no-op.

## 6. Proto additions

```protobuf
// internal.proto — sent from Proxy to QN/Delegator
message SearchAggregationInfo {
    repeated int64 group_by_field_ids       = 1;  // union of all levels' OwnFieldIDs, flat
    repeated int64 metric_field_ids         = 2;  // all metric-source fields, deduped
    int32          group_count_safe_factor  = 3;  // default 4
    int32          metric_safe_factor       = 4;  // default 1, Phase-2
}

// plan.proto — per-node plan carrier
message QueryInfo {
    ...
    SearchAggregationInfo agg_info = <N>;           // non-nil → agg path
}

// milvus.proto — user-facing request
message SearchRequest {
    ...
    common.GroupBySpec group_by = <N>;              // non-nil → agg path
}

// common.proto — recursive user spec
message GroupBySpec {
    repeated string            fields      = 1;
    int64                      size        = 2;
    map<string, MetricAggSpec> metrics     = 3;
    repeated OrderSpec         order       = 4;
    TopHitsSpec                top_hits    = 5;
    GroupBySpec                sub_group   = 6;
}
message MetricAggSpec { string op = 1; string field = 2; }
message OrderSpec     { string key = 1; string dir  = 2; }  // dir: "asc" | "desc"
message TopHitsSpec   { int64  size = 1; repeated SortSpec sort = 2; }
message SortSpec      { string field = 1; string order = 2; }

// schema.proto — response carrier
message SearchResultData {
    ...
    repeated AggBucket agg_buckets = 18;            // top-level buckets for all nq, flattened
    repeated int64     agg_topks   = 19;            // number of top-level buckets per nq
}
message AggBucket {
    repeated BucketKeyEntry key        = 1;
    int64                   count      = 2;
    map<string, double>     metrics    = 3;
    repeated AggHit         hits       = 4;
    repeated AggBucket      sub_groups = 5;
}
```

All proto additions are optional / new-message fields; older binaries that don't understand them see unchanged behavior.

## Compatibility, Deprecation, and Migration Plan

- **No breaking changes.** `group_by_field` + `group_size` continue to work exactly as today. At the proxy boundary they are translated into `GroupBy(fields=[field], size=topk, top_hits=TopHits(size=group_size))`, but the translation reuses the **legacy SearchGroupBy path** (field 8) — no behavior drift.
- **Routing invariant.** Path selection is keyed on the **original request source**, not the normalized form: if the incoming `SearchRequest` carried `group_by_field`, the request routes through the legacy SearchGroupBy path even though the proxy has materialized an internal `GroupBy` struct; only requests that originally carried `group_by` take the new SearchAggregation path. This prevents any chance of a legacy request being misrouted into the new agg pipeline after normalization.
- **Proto additions are additive.** All new fields / messages are optional; mixed-version clusters are safe as long as all segcore / QN / proxy binaries reach the version that understands `QueryInfo.agg_info` before any client starts sending `SearchRequest.group_by`.
- **Mutual exclusion** at the proxy: setting both `group_by_field_id` and `group_by` on the same request returns `merr.WrapErrParameterInvalid`. Prevents silent confusion about which path executed.
- **Feature flag** `proxy.search.embeddedAggregation.enabled` (paramtable), default `off` for the first release. When off, `SearchRequest.group_by` is rejected with a clear error; `group_by_field` + `group_size` still work.
- **Disallowed combinations in Phase 1.** The proxy hard-rejects any request that combines `group_by` with an incompatible feature, returning `merr.WrapErrParameterInvalid` rather than silently ignoring the conflicting parameter. Phase 1 disallows: hybrid search (multi-vector rerank), `highlight`, more than one JSON field in `GroupBy.fields` across all levels, and `limit` set together with `group_by` (use `GroupBy.size` instead). Revisited as these features are extended in later phases.

## Test Plan

1. **Segcore unit tests** (`test_search_group_by.cpp`, `test_group_by_json.cpp`).
   - Multi-field composite key — sealed / growing / cross-segment reduce / single-field via composite path (covered by milvus-io/milvus#48970).
   - `agg_info` present → group-by values emitted into `fields_data`; field 8 / 17 untouched.
   - Nullable field in a composite key — `nullopt == nullopt` semantics.
   - Hash collision on distinct keys → `operator==` separates them.
2. **QN/Delegator unit tests** (`internal/querynodev2/segments/search_reduce_test.go`).
   - Single-column backward compat (field 8 in → field 8 out).
   - Multi-column single field (1 field in `fields_data`).
   - Multi-column multi-field (2–3 fields, composite dedup).
   - `group_size` / `topK` enforcement for both modes.
   - Cross-segment merge — same composite key from different segments merged.
   - `InitSearchReducer` routing on `GroupByFieldId > 0` vs `AggInfo != nil`.
3. **Proxy unit tests** (`internal/proxy/search_agg/*_test.go`).
   - `BuildSearchAggregationContext` field-id resolution (including JSON paths).
   - `SearchAggregationComputer.Compute()` — flat / 2-level / 3-level fixtures with known ground truth.
   - `order` by `_count`, `_key`, metric alias; multi-criterion tiebreaker priority.
   - `TopHits.sort` independence from `order`.
   - `normalizeGroupBy` translating legacy `group_by_field` to `GroupBySpec`.
   - Mutual-exclusion validation.
4. **Integration tests** (`tests/integration/`).
   - Leaf-only metric happy path, cross-node.
   - `group_count_safe_factor` over-fetch recovers small level-1 buckets under skewed distribution.
   - `order` by `_count`, `_key`, metric alias — verify top-K buckets match expected ranking.
   - Feature flag off → new params rejected.
5. **E2E (pytest)** (`tests/python_client/milvus_client_v2/test_milvus_client_search_group_by.py`).
   - End-to-end round-trip with 1-, 2-, 3-level `GroupBy`.
   - Disallowed-combination rejection: hybrid search + `GroupBy`, `highlight` + `GroupBy`, multi-JSON `GroupBy.fields` — each must return `merr.WrapErrParameterInvalid`.

## Rejected Alternatives

### A1 — Run ANN to exhaustion, no early-stop

Dropping early-stop gives ES-exact semantics but collapses search latency by orders of magnitude. Not recoverable by tuning.

### A2 — Two-phase execution (search for hits + query-agg for metrics)

Coordinator fires one search + one query-agg in parallel. Metric precision is perfect, but request latency roughly doubles and the two semantics don't compose naturally — the metric is computed over the entire post-filter collection, not the ANN retrieval pool, which is usually a different number than what users asking for "avg similarity of the chunks I actually retrieved" expect. Rejected for Phase 1; can be offered later as an explicit `metric_source="query"` option.

### A3 — Native segcore nested execution

Implement ES `terms -> terms` natively in segcore: tree-shaped partials, per-parent accumulators, tree-shaped reduce. Most ES-faithful, but a large engine change (new plan node type, new reduce code, new proto schema), and the user value over proxy-side reconstruction is low. Revisit if workloads demand level-1 metrics over deeply nested hierarchies.

### A4 — Flat API only, no nesting at all

Expose only `group_by_fields=[...]` (multi-field flat), let users cope with missing small buckets. Rejected: distribution-skew failure mode (small level-1 buckets silently vanishing) is a correctness surprise users cannot debug, and the hierarchy use case is one of the top-3 user requests.

### A5 — Single `precision` knob instead of two `safe_factor`s

One knob is easier to document but cannot independently control the two orthogonal error sources (tuple coverage vs per-bucket sample size). Rejected.

## Future Extensions

**Scalar query aggregation.** The operators (`count` / `sum` / `avg` / `min` / `max`) and the `GroupBy` class shape are not intrinsically tied to vector search — they reuse the same `internal/agg/` accumulators that the scalar query path already has. A natural follow-up is to adopt the same `GroupBy` API for scalar `query` calls, giving Milvus a unified GROUP BY surface across both retrieval paths. `_score` and `TopHits` would not apply in scalar mode; results would be exact (no ANN early-stop, no `safe_factor`). Out of scope for this MEP: migration from the existing `QueryPlanNode.Aggregate` surface needs its own proposal.

## Open Questions

1. **Per-bucket metric window (`metric_safe_factor`) — Phase 2.** Whether to widen segcore's per-bucket `group_size` window for metric accumulation beyond what `top_hits` needs. Landing this means touching the segcore operator; tentatively deferred until Phase 1 ships and we have data on metric precision complaints.
2. **Metric on JSON dynamic types.** `sum(json_path)` requires a type cast. Follow `internal/agg/type_check.go` rules, or relax for search? **Tentative:** same rules.
3. **`order` referencing an undeclared alias.** Hard error at parse time, or fall through to `_count`? **Tentative:** hard error.
4. **`_score` as an `order` key directly.** ES forbids this and requires `avg/max/min(_score)` wrapping. Follow ES? **Tentative:** follow ES.
5. **`group_count_safe_factor` / `metric_safe_factor` placement in the Python API.** Inside `GroupBy` (design plan's current choice), or on `search()` kwargs? **Tentative:** inside root `GroupBy`.
6. **Cardinality backend.** Vendor HyperLogLog or reuse an internal library? Pending audit of existing HLL code in the codebase.
7. **Hybrid search + `group_by`.** **Resolved — disallowed in Phase 1** (see Compatibility). Hybrid search has its own reducer and a clean composition with the agg pipeline needs a dedicated investigation; deferred to a later phase.

## References

- milvus-io/milvus#48970 — `feat: support multi-field composite group_by for vector search` (segcore composite-key foundation).
- ES `multi_terms`: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-multi-terms-aggregation.html
- ES `terms` approximation semantics: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html (see `shard_size`, `show_term_doc_count_error`)
- Internal design docs (authoritative implementation plans):
  - `~/hc-claude-projects/milvus/search-aggregation/pymilvus-search-agg-api-design-chn.md` — Python SDK API
  - `~/hc-claude-projects/milvus/search-aggregation/segcore-implementation-plan.md` — C++ segcore
  - `~/hc-claude-projects/milvus/search-aggregation/qn-reduce-implementation-plan.md` — QN / Delegator reduce
  - `~/hc-claude-projects/milvus/search-aggregation/proxy-implementation-plan.md` — Go proxy: `SearchAggregationContext`, `SearchAggregationComputer`, pipeline integration
- Milvus query aggregation framework (reused by proxy metrics): `internal/agg/` and `internal/core/src/exec/operator/query-agg/`
- Milvus grouping search user doc: https://milvus.io/docs/grouping-search.md
