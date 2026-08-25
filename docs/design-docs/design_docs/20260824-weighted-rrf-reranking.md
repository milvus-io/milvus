# MEP: Weighted Reciprocal Rank Fusion Reranking

- **Created:** 2026-08-24
- **Author(s):** @AmSmart
- **Status:** Draft
- **Component:** Proxy
- **Related Issues:** #52817
- **Released:** TBD

## Summary

Extend the existing Reciprocal Rank Fusion (RRF) reranker with an optional
`weights` parameter. Each weight applies to the hybrid-search ANN request at
the same position. When weights are supplied, Milvus computes:

```text
score(d) = sum_i(weights[i] / (k + rank_i(d)))
```

Ranks are one-based. When `weights` is omitted, Milvus retains the existing
RRF formula and scores exactly:

```text
score(d) = sum_i(1 / (k + rank_i(d)))
```

## Motivation

Milvus currently provides two server-side fusion choices with different
semantics:

- RRF combines rank positions but gives every retrieval path equal influence.
- The weighted reranker gives paths different influence but combines
  normalized or direction-adjusted retrieval scores.

Dense, sparse, BM25, and other retrieval paths often have different measured
quality, while their raw scores are not necessarily comparable. Users who
want both rank-only fusion and different path importance must currently issue
separate searches and fuse the results in application code. That loses the
single-request execution model and duplicates candidate-fusion logic outside
Milvus.

Weighted RRF fills this gap without introducing score normalization or
metric-specific behavior.

## Public Interfaces

The existing RRF reranker accepts an optional `weights` array in its function
parameters:

```python
ranker = Function(
    name="weighted_rrf",
    input_field_names=[],
    function_type=FunctionType.RERANK,
    params={
        "reranker": "rrf",
        "k": 60,
        "weights": [0.7, 0.3],
    },
)
```

The legacy hybrid-search rank parameters accept the same option:

```json
{
  "strategy": "rrf",
  "params": {
    "k": 60,
    "weights": [0.7, 0.3]
  }
}
```

The weight at index `i` applies to ANN request `i`. The validation contract is:

- `weights` is optional.
- When supplied, it must be a non-empty JSON array of numbers.
- Each value must be in the inclusive range `[0, 1]`.
- The array length must equal the number of ANN search requests.
- Values are not normalized and do not need to sum to one.

The generic reranker parameter fields already carry JSON-encoded arrays, so
this change does not require a protobuf or REST schema change. The Go client
RRF helper adds an optional `WithWeights` convenience method. Convenience APIs
in other SDK repositories can be added independently.

## Design Details

### Parameter conversion and validation

Both public reranker paths converge in the Proxy function-chain builder:

- FunctionScore requests already expose function parameters as key/value
  pairs.
- Legacy rank parameters are converted to the same FunctionSchema shape. The
  conversion will forward `weights` for RRF in addition to the existing `k`
  parameter.

The RRF builder parses `k` as it does today, parses optional weights, and
validates the weight count against the ordered search-metric list. That list is
built in the same order as the hybrid-search sub-requests, preserving the
public positional mapping.

An omitted `weights` parameter remains distinguishable from an explicitly
empty or null value. Omission selects classic RRF. Empty, null, malformed,
out-of-range, or length-mismatched values return a parameter error before the
merge executes.

### Merge execution

The existing MergeOp already stores per-input weights for weighted score
fusion. RRF reuses that configuration field without enabling score
normalization or metric conversion.

For every query chunk, the RRF collector iterates each input list in request
order. Rank resets for each input and is `row index + 1`. The contribution is:

```text
path_weight / (k + rank)
```

When optional weights are absent, `path_weight` is exactly `1`. This preserves
both ordering and the exposed float scores produced by existing RRF requests;
it does not substitute `1 / number_of_paths`.

Documents missing from a path receive no contribution from that path. A
zero-weight path contributes zero while its candidates remain part of the
merged candidate union, consistent with the existing merge operator model.
All-zero weights are permitted and produce deterministic tie ordering by the
existing primary-key tie breaker.

The execution layer also checks that configured weights match the actual input
count and contain only finite values in `[0, 1]`. This is defense in depth for
programmatic MergeOp construction and internal contract violations;
user-facing validation remains in the builder.

### Scoring and ordering

RRF remains metric-agnostic. Original similarity or distance scores are not
read, and score normalization is not applied. Fused scores remain descending:
larger values rank first. Existing primary-key tie breaking, grouping,
rounding, limiting, and output selection remain unchanged.

## Compatibility, Deprecation, and Migration Plan

This change is backward-compatible:

- Existing RRF requests without `weights` produce the same ordering and scores.
- Existing `k` defaults and validation remain unchanged.
- The existing weighted score reranker is unchanged.
- No wire fields, persisted metadata, storage formats, or configuration values
  change.
- Mixed-version clients can send classic RRF as before. Servers predating this
  enhancement silently ignore unknown RRF function parameters, while their
  legacy converter drops RRF weights, so clients must version-gate weighted
  RRF rather than assume an older server will reject it.
- Requests with invalid reranker parameters now return a parameter error even
  when every sub-search returns empty results (previously they succeeded with
  empty results). Validation is now independent of result content.

No migration or deprecation is required. Omitting the parameter restores
classic RRF. Rolling back to an older server also silently restores classic
RRF even if a client continues to send weights.

## Test Plan

Unit tests will cover:

- omitted weights preserve classic RRF scores;
- all-one weights are equivalent to omitted weights;
- unequal weights change exact scores and ordering according to the formula;
- zero and one are accepted weight boundaries;
- weights are not required to sum to one;
- malformed, null, empty, negative, greater-than-one, and length-mismatched
  weights are rejected;
- FunctionScore and legacy rank parameters produce the same MergeOp
  configuration;
- execution-time input-count mismatch returns an internal contract error;
- Go client RRF parameters serialize optional weights correctly.

Query-level tests will verify that hybrid search accepts valid RRF weights and
rejects invalid or mismatched weights through both the public Function API and
the legacy typed RRF helper.

## Rejected Alternatives

### Use the existing weighted score reranker

The weighted reranker combines retrieval scores after normalization or metric
direction handling. That is different from rank-only fusion and can remain
sensitive to score distributions. It does not satisfy the requested semantics.

### Perform weighted RRF in application code

Application-side fusion requires separate result handling, transfers more
candidates to the client, and prevents Milvus from applying the final fusion,
limit, grouping, and requery pipeline in one request.

### Add a separate `weighted_rrf` reranker name

Weighted RRF differs from RRF by one optional coefficient per input. Extending
the existing RRF parameters keeps classic RRF as the default, avoids another
top-level strategy, and matches the existing parameterized `k` design.

### Normalize weights automatically

Scaling every weight by the same positive constant does not change result
ordering, but it does change exposed fused scores. Implicit normalization would
make the requested coefficients less transparent. Milvus therefore uses the
provided values directly.

## References

- Issue #52817: Support per-path weights in RRF reranking
- Issue #52319: FunctionChain roadmap
- Issue #46565: FunctionChain umbrella
- Cormack, Clarke, and Buettcher, "Reciprocal Rank Fusion Outperforms Condorcet
  and Individual Rank Learning Methods," SIGIR 2009
