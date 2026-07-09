# Milvus Cascade Search Design

## Overview

Cascade search adds a built-in two-stage search path to Milvus. A user searches one field to get a large global candidate set, then refines only those candidate rows on a second searchable field and returns a smaller final top-k.

The first version should stay in the existing `search()` API. The top-level search request is the coarse stage. A new `cascade.refine` block inside search params describes the refine stage. Each stage owns its own `limit`, so the large and small k values sit next to the fields they control.

## Motivation

Some workloads already implement a coarse-to-refine flow outside Milvus:

1. Search a dense vector collection for top 10000 ids.
2. Search another collection or field with a smaller top-k and `id in [10000 ids]`.
3. Use the second result as the final ranking.

This works functionally, but the second phase pays a high cost for large scalar `IN` predicates and often duplicates data across collections. For example, a user may store a 1024-d dense vector for coarse recall and an embedding-list field with around 100 128-d vectors per row for refine. The expensive part is not the exact refine math itself; it is routing through a large PK filter and doing row lookup again.

Cascade search moves this flow into Milvus. The coarse stage returns internal candidate addresses, such as segment id and segment offset. The refine stage directly scores those candidate rows on the second field, avoiding `id in [large_k]` as an external scalar filter.

## Goals

- Keep the first public API minimal and close to current PyMilvus search style.
- Support strict global two-phase semantics:
  - phase 1 produces the global coarse top `large_k`
  - phase 2 refines only those globally selected candidates
- Support any two different searchable fields as coarse and refine fields, including dense vector, sparse vector, full-text, and vector-array or embedding-list fields.
- Avoid using PK lookup as the internal refine locator.
- Keep candidate-bearing segments loaded until the cascade request finishes.
- Delay output-field materialization until the final refined top-k is known.

## Non-Goals

- Do not introduce a general search-plan DSL in the first version.
- Do not make segment-local cascade the default semantics.
- Do not expose internal segment ids or segment offsets to users.
- Do not require users to split data across two collections.
- Do not optimize compatibility with older experimental API shapes.

## API Design

The first version extends the existing search API. The top-level search fields describe the coarse stage. `search_params["cascade"]["refine"]` describes the refine stage.

```python
client.search(
    collection_name="docs",

    # Coarse stage.
    data=dense_query,
    anns_field="dense_1024",
    limit=10000,  # large_k for dense_1024
    search_params={
        "metric_type": "COSINE",
        "params": {
            "ef": 128,
        },

        "cascade": {
            "refine": {
                "data": refine_query,
                "anns_field": "emb_list_128",
                "limit": 10,  # small_k for emb_list_128 and final result count
                "metric_type": "COSINE",
                "params": {
                    "mode": "exact",
                },
            },
        },
    },
    output_fields=["title", "url"],
)
```

### API Rules

- Top-level `data`, `anns_field`, `limit`, `metric_type`, and search params define the coarse stage.
- `cascade.refine.data`, `cascade.refine.anns_field`, `cascade.refine.limit`, `metric_type`, and params define the refine stage.
- Top-level `limit` is the coarse candidate count, also called `large_k`.
- `cascade.refine.limit` is the refine result count, also called `small_k`, and is the final number of rows returned.
- `cascade.refine.limit <= top_level.limit`.
- `cascade.refine.anns_field != top_level.anns_field`.
- Both fields must be loaded and searchable.
- If either stage uses an ArrayOfVector or Struct vector sub-field, both stages must use the same candidate scope:
  - row-level to row-level is valid
  - element-level to element-level is valid only when the element identity is the same
  - row-level to element-level is invalid
  - element-level to row-level is invalid
- The first version should not expose a separate `candidate_k` parameter. It would separate the k value from its field and make the API harder to read.

### Supported Field Combinations

Cascade search is not specific to embedding lists. The coarse and refine fields may be any supported searchable field types, as long as the field pair is different and the refine field can score a supplied candidate set.

Examples:

| Coarse Field | Refine Field | Example |
|--------------|--------------|---------|
| Dense vector | Vector-array / embedding-list | Dense recall, MaxSim refine |
| Sparse vector | Dense vector | Sparse lexical recall, dense semantic refine |
| Full-text | Dense vector | Text recall, vector refine |
| Dense vector | Sparse vector | Dense recall, sparse relevance refine |

For refine, the default execution mode should be candidate-restricted exact scoring. If the refine field has an ANN index, Milvus should not implement refine by running ANN with an `id in [...]` filter, because that recreates the original performance problem.

### Row-Level and Element-Level Scope

Cascade search must preserve one candidate identity across both stages.

For row-level search, the candidate identity is a row. This includes normal vector fields, row-level embedding-list search, sparse vector fields, and full-text fields.

For element-level search on Struct array vector sub-fields, the candidate identity is an element inside a row. Element-level cascade is valid only when both stages produce the same element identity, such as two vector sub-fields under the same parent Struct array field. In that case, the internal candidate address should include the parent row offset plus the element index.

Mixed scope is rejected. Milvus should not run a row-level coarse stage followed by an element-level refine stage, or an element-level coarse stage followed by a row-level refine stage. Those requests need an explicit collapse or expansion policy, which is outside cascade v1.

## Feature Compatibility

Cascade v1 composes only two ranking stages:

```text
coarse retrieval -> global coarse reduce -> candidate-restricted refine -> final reduce
```

Features that define the read view or restrict the search universe are inherited. Features that add cursoring, grouping, aggregation, ordering, or another reranking layer are not accepted in v1 unless this document defines their semantics.

### Supported in V1

| Feature | Behavior |
|---------|----------|
| Scalar filter / expr template values | Apply to phase 1. Phase 2 only refines rows or elements that survived phase 1, so the filter remains satisfied. |
| Partition names / partition-key isolation | Apply to phase 1 candidate generation; phase 2 only touches selected candidates from those partitions. |
| Consistency / guarantee timestamp / travel timestamp | One cascade request uses one read snapshot. Phase-2 candidate addresses must validate segment version or snapshot. |
| `ignore_growing` | If true, phase 1 produces no growing-segment candidates. If false, candidate leases must cover growing and sealed candidates. |
| Output fields | Materialize only after final refine top-k is known. |
| Analyzer / BM25 params | Each stage owns the params for its own searchable field. |
| `round_decimal` | Apply to final refine scores. Coarse scores are internal candidate-generation scores. |

### Supported With Explicit Semantics

| Feature | V1 Semantics |
|---------|--------------|
| `offset` | Treat as final-page offset. The top-level coarse `limit` remains `large_k`; the refine stage must produce at least `offset + final_limit` before final slicing. Do not support a separate coarse offset. |
| Range search | Support only on the coarse stage. Range constraints define the coarse candidate pool before global top `large_k`. Refine-stage range constraints are not part of v1. |
| ArrayOfVector / embedding-list search | Support row-level to row-level and element-level to element-level only. Reject mixed row/element scope. |

### Not in V1, Future Support

These features need a clear post-refine or multi-stage semantic model before they are accepted with cascade.

| Feature | Future Semantics |
|---------|------------------|
| Function score / function chains / boost / decay / model rerank | `coarse -> refine -> function rerank`, with separate candidate limits for refine and final output. |
| Order by | `coarse -> refine -> materialize order fields -> order_by -> final slice`. This changes "top by refine score" semantics, so it should be explicit. |
| Search aggregation | `coarse -> refine -> aggregate over refined pool`, with documented approximation over the refined candidate set. |
| Group by / multi-field group by | `coarse -> refine -> group by refined rows`, with over-fetch rules for group coverage. |
| Hybrid search and rank fusion | Requires a general search plan model. It should not be hidden inside `search_params.cascade` v1. |
| Search iterator / search_iter_v2 | Requires a cascade cursor that owns phase state and segment leases across batches. |
| Highlighter | Needs a rule for which stage supplies highlight text and how full-text cascade stages interact with highlight queries. |
| Refine-stage range search | Needs a clear definition for short result sets and whether final `small_k` is a hard count or distance-bounded result count. |

### Not Planned to Support

These combinations conflict with cascade v1 semantics and should be rejected by design.

| Feature / Combination | Reason |
|-----------------------|--------|
| Same coarse and refine field | Cascade requires two different fields. A single field should use normal search. |
| Row-level stage followed by element-level stage | The candidate identity changes from row to element without an explicit expansion policy. |
| Element-level stage followed by row-level stage | The candidate identity changes from element to row without an explicit collapse policy. |
| Element-level stages from different Struct array parents | Element indices do not refer to the same logical element identity. |
| Segment-local cascade as user-visible semantics | It amplifies refine work by segment count and does not mean global coarse top `large_k` followed by refine. |
| Refine implemented as ANN plus `id in [...]` | This recreates the performance problem cascade is meant to remove. |
| Coarse-stage offset | Skipping coarse candidates before refine makes recall semantics hard to reason about. Offset is only a final-result operation. |
| `reduce_stop_for_best` or other early-stop shortcuts before phase 2 | Cascade requires complete global phase-1 reduce before phase 2 can start. |
| Iterative filter hint as an independent cascade option | It changes search/filter execution strategy and conflicts with the candidate-address protocol unless redesigned as part of cascade execution. |

## Query Semantics

Cascade search is a single logical search request with two ranking stages.

For each query vector or query payload:

1. Run the coarse stage on the top-level field.
2. Globally reduce coarse results across all relevant shards, QueryNodes, and segments.
3. Keep only the global coarse top `large_k`.
4. Score those candidate rows on the refine field.
5. Globally reduce by refine score.
6. Return refined top `small_k`.

The refine score is the final ranking score. Coarse score is used only for candidate generation unless explicitly exposed later for debugging or explanation.

Segment-local cascade is not equivalent. If each segment independently produces `large_k` candidates, refine work is amplified by the number of segments and the result no longer means "global coarse top `large_k`, then refine." Therefore v1 should use strict global two-phase cascade.

## Execution Flow

```text
Client
  |
  v
Proxy parses search request with cascade.refine
  |
  v
Phase 1: coarse search on top-level anns_field
  |
  v
QueryNode / shard delegator returns candidates with internal origins
  |
  v
Global coarse reduce keeps large_k candidates
  |
  v
Coordinator groups candidates by owning QN/channel/segment
  |
  v
Phase 2: candidate-restricted refine scoring on refine.anns_field
  |
  v
Global refine reduce keeps small_k final rows
  |
  v
Late materialize output fields for final rows
  |
  v
Return SearchResults
```

The cascade coordinator can live at the layer that already owns the final global reduce for the search request. The important requirement is the phase boundary: phase 2 starts only after phase 1 has produced the true global coarse top `large_k`.

## Candidate Identity

The internal candidate locator should be based on segment row address, not PK lookup.

Recommended internal candidate address:

```text
channel or shard
query node or owner routing info
segment_id
segment_offset
segment_version or snapshot timestamp
optional element_index
optional pk
coarse_score
```

`segment_id + segment_offset` is the hot-path row locator. It lets QueryNode read and score the refine field directly for the candidate row. PK should still be carried for deduplication and final result identity, but it should not be the primary refine locator. Using PK for phase 2 would still require locating the owning segment and row, which is one of the costs this feature is meant to remove.

The segment version or snapshot timestamp prevents a stale offset from silently referring to a different segment state. The QueryNode must validate the candidate address before scoring.

## Segment Lifetime

Current QueryNode search pins searched segments for a single search task. That is not enough for strict global cascade, because phase 1 completes, then a global reduce happens, then phase 2 is sent back to owning QueryNodes.

V1 should add a short-lived cascade candidate lease.

Phase 1 behavior:

1. QueryNode searches candidate-bearing segments.
2. QueryNode creates a `lease_id` for segments that appear in returned phase-1 candidates.
3. QueryNode pins or refs those segment objects under the lease.
4. QueryNode returns candidate addresses plus the lease id.

Coordinator behavior:

1. Globally reduce coarse candidates.
2. Group selected candidates by QueryNode/channel/segment.
3. Send phase-2 refine requests with the corresponding lease id.
4. Release leases that have no surviving candidates after global reduce.
5. Release used leases after phase-2 completion.

QueryNode phase-2 behavior:

1. Validate the lease id.
2. Validate segment id and segment version or snapshot.
3. Score only the requested segment offsets on the refine field.
4. Return refine scores and final row identity metadata.
5. Release lease refs when requested, or when TTL cleanup fires.

The lease must be bounded by:

- maximum `large_k`
- maximum serialized candidate bytes
- request timeout or context cancellation
- short server-side TTL
- eager release on success and on coordinator-side discard

## Data Model and Plan Changes

V1 can represent cascade as an extension to search info rather than a new public API.

Conceptual plan shape:

```protobuf
message CascadeSearchInfo {
  VectorANNS coarse = 1;
  VectorANNS refine = 2;
  int64 coarse_topk = 3;
  int64 refine_topk = 4;
}
```

The exact protobuf layout can reuse existing `VectorANNS` and `QueryInfo` structures where practical. The key is that the plan must preserve two independent search specs:

- coarse field id, metric, params, placeholders, and top-k
- refine field id, metric, params, placeholders, and top-k

The refine stage also needs a candidate-address input rather than a scalar predicate input.

## QueryNode Refine Executor

The refine executor should accept grouped candidate batches:

```text
RefineCandidatesRequest
  collection_id
  replica_id
  channel
  lease_id
  refine_field_id
  refine_metric_type
  refine_params
  placeholder group for refine query data
  repeated CandidateBatch {
    segment_id
    segment_version or snapshot
    repeated nq
    repeated segment_offset
    repeated original_candidate_rank
    repeated coarse_score
  }
```

For each candidate, QueryNode reads the refine field by segment offset and computes the refine score.

For vector-array or embedding-list fields, the row-level refine score follows the existing field metric semantics, such as MaxSim over elements. If the winning element is useful, QueryNode may return an element index in internal results, but this is optional for v1 unless existing result semantics require it.

## Reduce and Materialization

The phase-1 reduce must keep candidate origins, not only PK and score. Existing late materialization already tracks segment offsets in reduced search results. Cascade needs similar source tracking, but with a stable mapping from reduced candidate rows back to owning QueryNode, channel, segment id, and lease id.

Output fields should not be materialized after phase 1. The final materialization should happen only after refine reduce selects `small_k` rows. This keeps the extra work proportional to final results, not `large_k`.

## Error Handling

If phase 1 fails, the request fails normally.

If phase 2 receives an invalid or expired lease, the request should fail with a retryable internal error. Silent fallback to PK lookup is not recommended for v1 because it hides performance regressions and changes the intended execution path.

If a segment is released, compacted, or moved before phase 2, the lease validation should fail unless the lease kept the exact segment object alive. TTL cleanup must release leaked leases after client cancellation, QueryNode errors, or coordinator errors.

If refine scoring fails for one segment, the whole cascade request should fail. Partial refined results would be hard to reason about because final top-k depends on all candidates.

## Validation

Proxy should validate:

- `cascade` has only the supported v1 shape.
- `cascade.refine` is present when cascade search is requested.
- coarse and refine fields exist.
- coarse and refine fields are different.
- both fields are searchable.
- `top_level.limit > 0`.
- `refine.limit > 0`.
- `refine.limit <= top_level.limit`.
- row-level and element-level candidate scopes are not mixed.
- element-level cascade fields share the same element identity.
- query count is compatible between coarse data and refine data.
- refine params request candidate-restricted scoring, not filtered ANN.
- rejected feature combinations listed in the compatibility matrix are absent.

QueryNode should validate:

- lease id exists and belongs to the request context.
- segment id is currently pinned by the lease.
- segment version or snapshot matches.
- all segment offsets are in range.
- refine field is loaded in the target segment.

## Performance Considerations

Cascade shifts the second phase from scalar-filtered ANN or brute-force search over an `IN` predicate to direct candidate-row scoring.

Expected savings:

- no client round trip between stages
- no second collection search required
- no large serialized `id in [...]` expression
- no PK-to-segment lookup for refine candidates
- no output materialization for coarse-only candidates

Expected costs:

- candidate address serialization between phase 1 and phase 2
- segment lease tracking on QueryNode
- second internal fanout after global reduce
- exact refine scoring over `large_k` rows

The API should keep `large_k` bounded by server-side limits. The refined work is still proportional to `large_k` and the refine field shape, for example `large_k * elements_per_row * dim` for embedding-list MaxSim.

## Testing Plan

Unit tests:

- parse and validate `cascade.refine` search params
- reject same coarse/refine field
- reject `refine.limit > top_level.limit`
- reject unsupported refine field types or unloaded fields
- verify final top-k comes from refine limit

Plan tests:

- build a cascade plan with independent coarse and refine field specs
- ensure both placeholder groups are preserved
- ensure candidate-address input is required for refine

QueryNode tests:

- create and release candidate leases
- release unused leases after phase-1 global reduce
- TTL cleanup for canceled requests
- reject expired or mismatched leases
- score candidate offsets directly on the refine field

Integration tests:

- dense vector coarse + dense vector refine
- dense vector coarse + vector-array refine
- row-level embedding-list coarse + row-level vector refine
- element-level Struct vector coarse + element-level Struct vector refine with the same parent Struct array field
- reject row-level coarse + element-level refine
- reject element-level coarse + row-level refine
- reject element-level fields from different Struct array parents
- sparse or full-text coarse + dense refine, if the field type is enabled in the test environment
- verify strict global semantics by comparing with an offline two-call implementation: global coarse top `large_k`, then exact refine over that exact candidate set
- reject iterator, group-by, hybrid, function rerank, aggregation, order_by, highlighter, iterative filter, and reduce shortcut combinations in v1
- verify released or compacted segments do not produce incorrect results under a lease

Performance tests:

- compare cascade search with the current two-call `id in [large_k]` workaround
- measure phase-1 search, global reduce, phase-2 refine, and materialization separately
- test multiple segment counts to confirm refine work is not amplified by number of segments

## Open Questions

- Which component should own the cascade coordinator in the first implementation: Proxy, shard delegator, or a split between them?
- Should coarse scores be optionally returned for explanation or debugging?
- Should v1 expose winning element index for embedding-list refine results?
- What should be the default server-side maximum for `large_k` and candidate bytes?
