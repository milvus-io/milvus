# MEP: Delegator-Side Segment Pruning via PK Predicate Hints

- **Created:** 2026-03-24
- **Author(s):** @xiaofan-luan
- **Status:** Implemented
- **Component:** Proxy, QueryNode (Delegator)
- **Related Issues:** #47804
- **Released:** TBD

## Summary

When a search or query request contains a primary-key (PK) predicate, the shard delegator prunes its sealed-segment dispatch list before fanning out sub-tasks to worker nodes. The delegator evaluates a lightweight PK predicate IR against each segment's `pkoracle.Candidate` — which already holds a bloom filter and min/max PK statistics in memory — and removes segments that are provably non-matching. A proxy-side hint field avoids plan deserialization on the delegator for the common case where no PK predicate exists.

## Motivation

The shard delegator fans out every search/query to all worker nodes that own sealed segments within the requested time range. For collections with many sealed segments this is wasteful when the query carries a PK predicate such as `id == 5`, `id IN [1, 2, 3]`, or `1 < id < 100`: segments whose PK range does not overlap the predicate will return empty results after paying the full cost of a C++ vector-search kernel invocation.

The delegator already maintains a `pkoracle.Candidate` (a `BloomFilterSet`) for every sealed segment it manages. Each candidate holds:

- A **bloom filter** over all PKs in the segment, supporting `BatchPkExist` batch queries.
- **Min/max PK statistics** via `GetMinPk()` / `GetMaxPk()`, computed as the global min/max across all stat blobs (current + historical).

These are sufficient to definitively exclude a segment without touching C++:

- **Point / IN queries** (`pk = X` or `pk IN [list]`): bloom filter plus min/max range can prove a segment cannot contain the target PKs.
- **Range queries** (`pk > X`, `pk < Y`): min/max statistics can prove no overlap with the predicate.

Filtering at the delegator — in Go, before any worker RPC is issued — means that excluded segments never reach C++. The entire cost of the RPC, segment pinning, and the vector-search kernel is avoided for those segments. Workers that end up with an empty segment list are skipped entirely by `organizeSubTask`.

## Non-Goals

- Pruning on non-PK predicates.
- Filtering growing/streaming segments (their statistics are mutable under concurrent inserts).
- Changes to the C++ expression evaluation path.

## Public Interfaces

### Proto Changes

New field `pk_filter int32` on `SearchRequest` and `RetrieveRequest` in `internal.proto`:

```protobuf
message SearchRequest {
    // existing fields ...
    int32 pk_filter = N;
}
message RetrieveRequest {
    // existing fields ...
    int32 pk_filter = N;
}
```

**Constants** (`pkg/common/common.go`):

```go
const (
    PkFilterNotChecked  = int32(0) // proto wire default; set by old proxies (backward compat)
    PkFilterHasPkFilter = int32(1) // proxy confirmed optimisable PK predicate exists
    PkFilterNoPkFilter  = int32(2) // proxy confirmed no optimisable PK predicate
)
```

### New Configuration Parameter

| Key | Default | Hot-reload | Description |
|-----|---------|------------|-------------|
| `queryNode.enableSegmentFilter` | `true` | yes | Enable delegator-side PK predicate pruning |

### New Metrics

All three are `HistogramVec` with labels `{nodeID, collectionID, queryType}`:

| Metric name | Description |
|-------------|-------------|
| `milvus_querynode_segment_filter_total_segment_num` | Sealed segments considered before pruning |
| `milvus_querynode_segment_filter_skipped_segment_num` | Segments pruned (not dispatched) |
| `milvus_querynode_segment_filter_hit_segment_num` | Segments that passed the filter |

## Design Details

### Architecture

```
Proxy
  │  set PkFilter hint on SearchRequest / RetrieveRequest
  │  (PkFilterHasPkFilter=1 or PkFilterNoPkFilter=2)
  ▼
Shard Delegator
  │  1. PinReadableSegments → sealed []SnapshotItem
  │     each SnapshotItem.Segments[i].Candidate = pkoracle.Candidate (BF + min/max)
  │
  │  2. PruneSealedSegmentsByPKFilter(sealed, pkFilter, plan)
  │     ├─ skip if pkFilter == NoPkFilter
  │     ├─ unmarshal plan → BuildPKFilterExpr → PKFilterExpr IR
  │     └─ for each SnapshotItem:
  │         collect entry.Candidate as []PKFilterTarget
  │         CheckPKFilter(IR, targets) → matchedIDs
  │         remove non-matching SegmentEntry in-place
  │
  │  3. organizeSubTask — skips workers with empty segment list
  │
  └─► Worker RPCs with pruned segment lists
```

### 1. Proxy: Setting the PK Filter Hint

`checkSegmentFilter(plan)` in `internal/proxy/segment_filter_helper.go` calls `HasOptimizablePkPredicate` to analyse the expression tree and returns either `PkFilterHasPkFilter` or `PkFilterNoPkFilter`. It is called in both `SearchTask.Execute` and `QueryTask.Execute` and the result is written to `SearchRequest.PkFilter` / `RetrieveRequest.PkFilter` before the request is dispatched.

`HasOptimizablePkPredicate` in `internal/util/exprutil/expr_checker.go` traverses the expression tree:

| Expression | Optimisable? |
|------------|--------------|
| `TermExpr` on PK field | Yes |
| `UnaryRangeExpr` on PK field (=, >, >=, <, <=) | Yes |
| `BinaryRangeExpr` on PK field | Yes |
| `AND(left, right)` | Yes if either side is optimisable |
| `OR(left, right)` | Yes only if **both** sides are optimisable |
| `NOT(inner)` | No — negation cannot narrow the segment set |

**Backward compatibility:** `pk_filter` defaults to `0` (`PkFilterNotChecked`) on the wire. Old proxies that do not set the field cause the delegator to attempt IR compilation anyway — no correctness regression.

### 2. `PKFilterTarget` Interface

Defined in `internal/querynodev2/delegator/pk_filter.go`:

```go
type PKFilterTarget interface {
    ID() int64
    PkCandidateExist() bool
    BatchPkExist(*storage.BatchLocationsCache) []bool
    GetMinPk() *storage.PrimaryKey
    GetMaxPk() *storage.PrimaryKey
}
```

`pkoracle.Candidate` (implemented by `BloomFilterSet`) satisfies this interface. `GetMinPk` / `GetMaxPk` scan `currentStat` plus all `historyStats` to return the global min/max PK; they return `nil` when no statistics are available (segment treated as a match — conservative).

`BloomFilterSet.BatchPkExist` ORs results across `currentStat` and all `historyStats` bloom filters, returning a `[]bool` hit array for the batch.

Each `SegmentEntry` in the distribution carries a `Candidate pkoracle.Candidate` field; this is the data source for `PKFilterTarget`.

### 3. PK Filter IR (`pk_filter.go`)

`BuildPKFilterExpr(plan, pkFilter)` compiles the PK predicate from a `planpb.PlanNode` into a typed IR. It returns `nil` immediately when `pkFilter == PkFilterNoPkFilter`.

```
PKFilterExpr
  ├── pkFilterTermExpr          pk IN [v1, v2, ...]  (or pk = x as single-element list)
  ├── pkFilterUnaryRangeExpr    pk op value          (>, >=, <, <=)
  ├── pkFilterBinaryRangeExpr   lo (</<=) pk (</<=) hi
  ├── pkFilterLogicalExpr       AND / OR
  └── pkFilterUnsupportedExpr   fallback — keeps all segments
```

Non-PK expressions and unsupported node types produce `pkFilterUnsupportedExpr`. `UnaryRangeExpr` with `Equal` is normalised to a single-element `pkFilterTermExpr` so bloom-filter checking applies.

### 4. Evaluating the IR: `CheckPKFilter`

`CheckPKFilter(expr, targets)` returns `(matchedIDs Set[int64], all bool)`:
- `all == true` — keep all targets (returned for `pkFilterUnsupportedExpr` or empty target list).
- `all == false` — `matchedIDs` is the explicit set of segment IDs that passed.

Per-node evaluation:

| Node | Logic |
|------|-------|
| `pkFilterTermExpr` | Build a `BatchLocationsCache` from the value list. For each segment: if `PkCandidateExist`, call `BatchPkExist`; check whether any value falls within `[minPk, maxPk]` and passes the bloom filter. Keep segment if any value matches. |
| `pkFilterUnaryRangeExpr` | Boundary arithmetic on segment `[minPk, maxPk]`. E.g. `pk > X` → keep segment iff `maxPk > X`. |
| `pkFilterBinaryRangeExpr` | Both lower and upper bound arithmetic on `[minPk, maxPk]`. |
| `AND` | Evaluate left; short-circuit if already empty. Intersect with right. |
| `OR` | Evaluate left; short-circuit if already `all`. Union with right. |
| `Unsupported` | Return `{all: true}` — segment kept. |

### 5. Delegator: `PruneSealedSegmentsByPKFilter`

Located in `internal/querynodev2/delegator/segment_pruner.go`. Called in `delegator.Search`, `delegator.Query`, and `delegator.QueryStream` after `PinReadableSegments` returns the sealed snapshot but before `organizeSubTask` routes sub-requests:

```go
if paramtable.Get().QueryNodeCfg.EnableSegmentFilter.GetAsBool() {
    PruneSealedSegmentsByPKFilter(ctx,
        req.GetSerializedExprPlan(),
        req.GetPkFilter(),
        sealed,          // []SnapshotItem, pruned in-place
        collectionID,
        queryType,       // "search" or "query"
    )
}
```

Steps:

1. If `pkFilter == PkFilterNoPkFilter`, return immediately.
2. Unmarshal `serializedExprPlan` into `planpb.PlanNode`; log and return on error.
3. `BuildPKFilterExpr(plan, pkFilter)` → IR; return if `nil`.
4. For each `SnapshotItem`: collect `entry.Candidate` (non-nil only) as `[]PKFilterTarget`, call `CheckPKFilter`, remove non-matching entries in-place.
5. Record the three Prometheus histograms (total / skipped / hit).

`organizeSubTask` is unchanged; it already skips workers whose `Segments` slice is empty.

### 6. Hint Forwarding to Workers

`shallowCopySearchRequest` and `shallowCopyRetrieveRequest` in `delegator.go` copy `PkFilter` into the per-worker sub-request. Workers therefore also receive the hint, which can be used for future worker-side optimisations without any proxy or proto changes.

## Correctness Guarantees

- **No false negatives.** The bloom filter produces false positives only (a segment may be kept when unneeded), never false negatives. The min/max range check is exact. A segment is excluded only when it is proven not to contain a matching PK.
- **Conservative on missing statistics.** If `GetMinPk()` or `GetMaxPk()` returns `nil`, the segment is kept.
- **Growing segments untouched.** `PruneSealedSegmentsByPKFilter` only operates on the sealed `[]SnapshotItem`. Growing segments are always queried.
- **Unsupported expressions kept.** Any expression that cannot be compiled into the IR becomes `pkFilterUnsupportedExpr` → `{all: true}` → all segments kept.
- **Empty TermExpr.** `pk IN []` produces an empty match set, correctly pruning all sealed segments (the query is known to return no results).

## Compatibility and Migration

- **Rolling upgrade safe.** `PkFilter` defaults to `0` (`PkFilterNotChecked`) on the wire. Old proxies cause the delegator to attempt IR compilation on every request — semantically equivalent to the pre-feature behaviour, just with a small extra cost.
- **Feature flag.** `queryNode.enableSegmentFilter` is `true` by default and can be toggled at runtime without restart.
- **No API surface changes.** This is a pure internal optimisation; client-visible search/query APIs are unchanged.

## Test Plan

- **IR unit tests** (`pk_filter_test.go`): all node types (Term, UnaryRange, BinaryRange, AND, OR); edge cases (empty Term, nil plan, unsupported expr); AND/OR short-circuit paths.
- **Pruner tests** (`distribution_test.go`): `PruneSealedSegmentsByPKFilter` removes non-matching entries in-place; no-ops when `pkFilter == NoPkFilter` or `EnableSegmentFilter == false`.
- **pkoracle tests**: `BloomFilterSet.GetMinPk` / `GetMaxPk` returns correct global min/max across multiple stat blobs.
- **Proxy tests** (`task_search_pk_hint_test.go`): `HasOptimizablePkPredicate` for all plan types and logical operators.
- **E2E**: existing search/query integration tests confirm result correctness is preserved.

## Rejected Alternatives

### Worker-Side (Segment-Layer) Filtering

An earlier design applied bloom-filter and min/max checks at the worker, just before the C++ call inside `validateOnHistorical`. This was rejected because:

- The delegator already holds `pkoracle.Candidate` for every sealed segment. Filtering at the delegator eliminates entire worker RPCs rather than just the per-segment C++ cost.
- Duplicating the filter IR and evaluation logic at the segments layer adds coupling and maintenance burden without additional benefit for the common case.
- The delegator dispatch paths (`Search`, `Query`, `QueryStream`) are a single, well-defined choke point; a single call to `PruneSealedSegmentsByPKFilter` covers all three.

### Passing Extracted PK Values via Proto

An earlier design had the proxy extract PK values and send them alongside the serialised plan. Rejected because:

- It duplicates data already present in the serialised plan, increasing message size proportionally to the IN-list length.
- The hint field (`PkFilter`) achieves the avoidance of plan deserialization on the delegator at negligible cost.

### Pruning Growing Segments

Growing segments have bloom filters and statistics, but they are mutable under concurrent inserts. Pruning them would require locking or risk race conditions. Given that growing segments are typically small, the benefit does not justify the complexity.

## References

- PR: https://github.com/milvus-io/milvus/pull/47805
- Issue: https://github.com/milvus-io/milvus/issues/47804
