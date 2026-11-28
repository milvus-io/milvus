# MEP: Maximal Marginal Relevance (MMR) Reranking

- **Created:** 2026-11-22
- **Author(s):** @xiaofan-luan
- **Status:** Draft
- **Component:** Proxy
- **Related Issues:** TBD
- **Released:** TBD

## Summary

Add a Maximal Marginal Relevance (MMR) reranker that diversifies search
results. MMR takes a candidate window produced by an ordinary or hybrid search,
then greedily selects results that are relevant to the query and not redundant
with results that have already been selected:

```text
next = argmax_{c in candidates \ selected}
         lambda * rel(c) - (1 - lambda) * max_{s in selected} sim(c, s)
```

`rel(c)` is the upstream relevance score normalized to `[0, 1]` inside the
window. `sim(c, s)` is the similarity between the stored vectors of two
candidates. MMR is available through both public reranking APIs:

- FunctionScore: `reranker: "mmr"`, either alone or as the last function after
  one other proxy reranker (`rrf`, `weighted`, `decay`, or `model`).
- FunctionChains: a new `mmr` row-selection operator.

## Motivation

Nearest-neighbor search returns the most similar items, and in practice those
items are often near duplicates of each other: several chunks of the same
document, many traces from the same failure, or many posts that say the same
thing. RAG pipelines, agent memory retrieval, and recommendation feeds then
spend their context or screen space on redundant results.

Today users work around this in application code: they request a larger top-k,
return the vector field, and run MMR on the client. That transfers vectors over
the network, duplicates logic in every SDK, and cannot be combined with Milvus
server-side fusion and requery in one request.

`group_by` provides a hard per-key limit (for example one result per user), but
it cannot remove two different keys whose content is nearly identical. MMR
provides semantic diversity and composes with `group_by`-style constraints in a
later phase.

Elasticsearch (diversify retriever), OpenSearch, Qdrant, and Weaviate added
MMR to their query APIs in 2025–2026, and LangChain exposes it in every vector
store wrapper.

## Public Interfaces

### FunctionScore

```python
mmr = Function(
    name="diversify",
    input_field_names=["embedding"],    # vector field used for sim(c, s)
    function_type=FunctionType.RERANK,
    params={
        "reranker": "mmr",
        "lambda": 0.5,                  # optional, default 0.5
        "candidate_size": 100,          # optional, see defaults below
    },
)

# Ordinary search: MMR alone
client.search(..., limit=10, ranker=mmr)

# Hybrid search: RRF fusion, then MMR
client.hybrid_search(..., ranker=FunctionScore([rrf, mmr]), limit=10)
```

### FunctionChains

```python
chain = (FunctionChain()
         .merge(rrf(k=60))
         .mmr(field="embedding", lambda_=0.5, candidate_size=100)
         .limit(10))
```

The SDK helpers shown above are illustrative. The wire format uses the existing
`FunctionSchema` parameters and `FunctionChain` operator representation, so no
protobuf change is required.

### Parameters

| Parameter | Type | Default | Contract |
|---|---|---|---|
| `lambda` | float | `0.5` | Inclusive range `[0, 1]`. `1` is pure relevance (equivalent to no MMR, up to ties); `0` is pure diversity after the first pick. Same direction as Elasticsearch `lambda` and LangChain `lambda_mult`. |
| `candidate_size` | int | `min(maxCandidateSize, max(100, 3 * limit))` | Size of the fixed candidate window: the top `candidate_size` upstream results, always counted from rank 0. Independent of `offset` (see "Paging"). Must satisfy `limit <= candidate_size <= function.rerank.mmr.maxCandidateSize`. |
| `input_field_names` | one vector field | ordinary search: the ANNS field | Required in hybrid search. Any vector type is accepted, including struct-array vector sub-fields; see "Similarity". |
| `similarity` | string | the field's index metric | Optional override; must be a metric valid for the field type (see "Similarity"). |

New configuration:

| Key | Default | Meaning |
|---|---|---|
| `function.rerank.mmr.maxCandidateSize` | `1024` | Upper bound for `candidate_size`. |
| `function.rerank.mmr.maxSimilarityOps` | `1e9` | Upper bound on the estimated vector element operations per request, counted as one similarity per candidate pair of every query's window. Charged per window, not per page, so all pages of a paging session are accepted or rejected alike. Protects embedding-list fields; dense vectors with `W = 1024` and dim up to 2048 fit. |

The existing `proxy.maxResultEntries` check applies to `nq * candidate_size`.

### Result contract

- Results are ordered by MMR selection order.
- Each result keeps its upstream relevance score (raw distance, similarity,
  or fused score). Scores are therefore not guaranteed to be monotonic. This
  matches OpenSearch, which keeps `_score` and reports `mmr_score` separately.
- `offset` and `limit` page through the MMR-ordered window: the result is
  positions `[offset, offset + limit)` of the MMR selection order over the top
  `candidate_size` candidates. Clients page with `offset += limit` as for any
  other search and keep `candidate_size` unchanged. Past the window the page is
  short (`candidate_size - offset` rows) or empty, as in Elasticsearch's
  diversify retriever; there is no error. See "Paging".
- MMR never drops a candidate. Redundant candidates are pushed later in the
  selection order and can still be selected once better alternatives run out.
  A page inside the window is therefore full whenever the upstream search
  produced at least `offset + limit` candidates; fewer are returned only when
  the upstream itself returned fewer (strict filter, small collection, range
  search radius).
- Optionally returning the MMR marginal score as an extra output field is left
  as an open question (see below).

### Unsupported combinations (rejected with a parameter error)

- search iterator and `order_by` (already rejected for any rerank function);
- search `group_by` and search aggregation (planned for a later phase);
- more than one `mmr` function, or `mmr` that is not the last FunctionScore
  function;
- `limit > candidate_size`, or `candidate_size` above the configured maximum
  or the search top-k limit (`quotaAndLimits.limits.topK`);
- an input field whose raw data cannot be retrieved: BM25 function outputs.
  MMR compares stored candidate vectors, and Milvus does not return BM25
  sparse vectors, so full-text search must name a dense vector field as the
  MMR input.

## Design Details

### Similarity

All candidate-to-candidate similarities are computed on the stored vectors of
the field named in `input_field_names`, using **the metric of that field's
index**. A collection can only be loaded when every vector field is indexed, so
the metric is always available. This keeps MMR's notion of "similar" identical
to the one the user chose for the field.

The raw metric value is mapped to a similarity in `[0, 1]`. COSINE, IP and L2
use the functions the weighted reranker uses for `norm_score`
(`getNormalizeFunc`), so the system has one normalization convention; JACCARD
and HAMMING are bounded and use their exact complements:

| Index metric | Similarity used by MMR |
|---|---|
| `COSINE` | `(1 + s) / 2` |
| `IP` | `0.5 + atan(s) / pi` |
| `L2` | `1 - 2 * atan(d) / pi` |
| `JACCARD` | `1 - d` |
| `HAMMING` | `1 - d / dim` |
| `MAX_SIM*` (embedding lists) | Symmetric MaxSim with the element metric, `0.5 * (mean_i max_j m(a_i, b_j) + mean_j max_i m(b_j, a_i))`, then the mapping above |

Float16, BFloat16, and Int8 vectors are converted to float32 before the
computation. Sparse vectors accept `IP` and `COSINE`. BM25 output fields are
not valid MMR inputs (see "Unsupported combinations"). `MHJACCARD`,
`SUBSTRUCTURE` and `SUPERSTRUCTURE` have no usable
candidate-to-candidate similarity and require an explicit `similarity`; a
`similarity` that does not fit the field type is rejected when the chain is
built.

The optional `similarity` parameter overrides the index metric. It is useful
for un-normalized embeddings under `IP`: when norms are large, `atan`
saturates (`IP = 50` and `IP = 80` both map to about `0.99`), every pair looks
equally redundant, and MMR loses discrimination. Setting `similarity: "COSINE"`
avoids this.

MMR always needs the stored vectors of the `W` candidates; they are fetched
internally and are not returned unless requested in `output_fields`.

Rows whose vector is null have similarity `0` to every other row: they are never
considered redundant.

### Relevance normalization

The relevance term and the redundancy term must be on a comparable scale.

- **MMR directly on search scores** (ordinary search with no other reranker):
  the search score is the query-to-candidate value of the same metric used for
  candidate-to-candidate similarity. MMR maps it with the same function from
  the table above, so both terms live on one scale. This matches Qdrant and
  OpenSearch, which use the raw score with the field's own metric.
- **MMR after another reranker** (`rrf`, `weighted`, `decay`, `model`, or any
  FunctionChains merge/map): fused scores have no fixed scale (RRF scores are
  near `1/60`). MMR applies direction-aware min-max normalization inside the
  window, as Weaviate does for hybrid search:

```text
descending scores: rel = (s - min) / (max - min)
ascending scores:  rel = (max - s) / (max - min)
max == min:        rel = 1
```

The score direction is taken from the same `searchScoreInfo` / merge direction
the chain already uses for sorting. Because the window is fixed (see
"Paging"), the min-max bounds are the same for every page.

### Selection algorithm

For each query chunk:

1. Take the first `W = candidate_size` rows of the upstream ranking.
2. Pick the row with the highest `rel` (all redundancy terms are zero).
3. Maintain `maxSim[c]` for every unselected row. After each pick `p`, update
   `maxSim[c] = max(maxSim[c], sim(c, p))` and choose the next row by the MMR
   objective.
4. Stop after `min(offset + limit, W)` picks or when the window is exhausted,
   then skip the first `offset`.

Ties are broken by higher `rel`, then by the existing primary-key tie breaker,
so results are deterministic. Work per query is `O(k * W)` similarity
evaluations with `k = offset + limit`; with dense vectors that is
`O(k * W * dim)`. For `W = 1024`, `k = 100`, `dim = 768` this is about 79M
multiply-adds per query. Pairwise similarities are computed lazily, so no
`W x W` matrix is materialized. The `maxSimilarityOps` budget is charged with
the cost of ordering the whole window (`W * (W - 1) / 2` pairs), an upper
bound for every page, so whether a request is accepted does not depend on
`offset`.

### Paging

MMR follows the Elasticsearch diversify-retriever model: the candidate window
is fixed, MMR orders it once, and `offset`/`limit` slice that order.

- The window is always the top `candidate_size` upstream results counted from
  rank 0. It does not depend on `offset`; the default depends only on `limit`,
  so pages with the same page size use the same window. Clients that need more
  pages set a larger `candidate_size` on the first page and keep it.
- A page is positions `[offset, offset + limit)` of the MMR order. When
  `offset + limit > candidate_size` the page holds the remaining
  `candidate_size - offset` rows; when `offset >= candidate_size` it is empty.
  Clients stop paging at an empty page.
- Because greedy selection is prefix-stable, selecting `offset + limit` rows
  yields exactly the first `offset + limit` positions of the full MMR order, so
  MMR never needs to order more than `offset + limit` rows.
- In hybrid search the window is the top `candidate_size` of the fused
  ranking. Sub-request limits are required by the API and are not changed;
  set them to at least `candidate_size` so that the window is full. They do
  not depend on `offset`, so the fused ranking is the same for every page.

With a fixed window, the candidate set, the min-max bounds, the normalized
relevance, and therefore the MMR order are identical for every page, and later
pages avoid results redundant with earlier ones. As for any offset-based
paging in Milvus, this assumes the data and the approximate top
`candidate_size` do not change between requests; snapshot-consistent paging
remains the role of search iterators.

### Function chain changes

MMR is implemented as a chain `Operator` (like `LimitOp`) because it reorders
and truncates rows. It is not a 1:1 `FunctionExpr`.

The chain's Arrow DataFrame currently supports only scalar and JSON columns
(`ToArrowType`). MMR needs the candidate vectors, so the chain gains an
internal **opaque vector column**:

- Arrow type `Binary`, one value per row holding the field's raw bytes in the
  Milvus wire layout (little-endian float32; sparse content bytes; for
  embedding lists the element vectors' bytes concatenated, so the element count
  is the cell length divided by the element width), plus column metadata
  (`DataType`, `dim`, element type for embedding lists). Import slices the
  `FieldData` payload without copying.
- Import from `FieldData` for every vector type; row pick (`dispatchPickByIndices`)
  copies bytes; pruning drops it like other internal columns.
- It is input-only. The proxy runs rerank chains with liveness pruning, so the
  column is dropped after MMR, its last reader, and never reaches
  `SearchResultData`. Vectors requested in `output_fields` continue to use the
  existing output path.
- `MergeOp` gathers Binary columns (JSON and vector columns) from its inputs,
  so the vector column survives the fusion step of hybrid search.
- Only MMR may take a vector input. Every other reranker and FunctionChains
  operator still rejects vector fields with "unsupported field type".

The MMR operator decodes the bytes per type and computes similarities with
`pkg/util/distance` (float32 inner product and squared L2, AVX2 on x86), the
float16/bfloat16 converters in `pkg/util/typeutil`, and small helpers for
int8, binary Jaccard/Hamming, sparse dot product, and symmetric MaxSim.

In FunctionChains, `mmr` is an L2 operator with one vector input and the
parameters `lambda` (default 0.5), `candidate_size` (default 100), `limit`
(rows to emit, default the whole window), `score_descending` (default true)
and `similarity` (default: the search metric when the request has exactly one
ANN search; required otherwise). The operator orders its input by `$score`,
keeps the top `candidate_size` rows and emits them in MMR order. In ordinary
search the proxy applies the request `limit`/`offset` after the chain unless
the chain has its own `limit` operator.

### Candidate window and vector fetch

**Ordinary search.** Today the reduce phase applies `offset` and truncates to
`limit` before rerank (`rerankOperator.offset = 0`), so rerank only sees the
returned page. When the rerank chain contains MMR:

- QueryNode top-k becomes `W` instead of `limit + offset`;
- reduce keeps `W` rows and does not apply `offset`;
- the pipeline requeries the `W` candidates by primary key to fetch the MMR
  vector field (and any requested output fields), mirroring the existing
  `hybridSearchWithRequeryAndRerankByFieldDataPipe`;
- the chain runs `MMR -> LimitWithOffset(offset, limit)`.

Fetching vectors through requery is used instead of adding the vector field to
the QueryNode search output because requery is the established path for every
vector type, and it transfers only `W` vectors per query instead of `W` per
shard. Collections in namespace-partition mode never requery; there the MMR
vector field is added to the QueryNode output fields like any other rerank
input. The existing `proxy.maxResultEntries` check sees `nq * W`, because the
search top-k is `W`. The "result size insufficient" signal compares the
result with the page MMR can return (`min(limit, W - offset)`), not with `W`.

**Hybrid search.** The window is the fused top-`W`: the MMR operator orders
the upstream reranker's output by score and keeps the first `W` rows.
Sub-request limits are unchanged (see "Paging"). Hybrid search already
requeries all candidates when the rerank function has input fields, so the
vector field joins the existing requery, and sub-requests no longer ask
QueryNode for the MMR vector.

### FunctionScore composition

`buildRerankChain` currently accepts one non-boost proxy function. It will
accept either:

- one non-MMR function (unchanged behavior),
- one `mmr` function alone, or
- one non-MMR function followed by one `mmr` function.

The upstream function builds its usual chain up to the sort. MMR then replaces
the shared `Sort + LimitWithOffset` tail with `MMR -> LimitWithOffset`; the
MMR operator takes the top-`W` window by score itself. Rounding is still
applied last.

For FunctionChains, the validator accepts `mmr` as an L2 operator after merge
or map operators and before the final limit.

### Replication

MMR is a read-path computation in Proxy. It creates no persistent state and no
WAL messages, so it needs no CDC handling.

## Compatibility, Deprecation, and Migration Plan

- Existing requests are unchanged: the new code paths are taken only when a
  request contains an `mmr` function or operator.
- Older servers reject `reranker: "mmr"` with `unsupported reranker` and reject
  the unknown chain operator, so clients get an explicit error rather than
  silently undiversified results.
- No storage, metadata, or protobuf changes. Two new refreshable configuration
  items.

## Test Plan

Unit tests (chain and builder):

- `lambda = 1` reproduces the upstream order; `lambda = 0` selects a maximally
  spread set on a constructed dataset;
- a hand-computed example with duplicates verifies each greedy step;
- prefix stability: selection for `k` equals the first `k` picks for `k + m`;
- offset handling matches `select(offset + limit)[offset:]`;
- paging stability: with a fixed `candidate_size`, pages `offset = 0, limit,
  2 * limit, ...` concatenate to the single-request selection of the same total
  size, for both ordinary and hybrid (`rrf + mmr`) search;
- pages that cross the window end are short, pages past it are empty, and
  `limit > candidate_size` is rejected; the default window does not change with
  `offset`;
- normalization for descending scores, ascending distances, constant scores;
- every vector type, including null vectors, embedding lists, and
  float16/bfloat16 conversion accuracy;
- parameter validation: lambda range, candidate_size bounds, missing or
  non-vector field, ambiguous field in hybrid search, `similarity` not valid for
  the type, MMR not last, two MMR functions, rejected combinations;
- the opaque vector column survives pick and is pruned before output.

Proxy pipeline tests:

- ordinary search with MMR sets QueryNode top-k to `W`, skips reduce offset,
  and requeries the vector field;
- hybrid search with `rrf + mmr` and FunctionChains with `merge -> mmr -> limit`;
- `nq > 1` keeps per-query windows separate.

E2E (Python client): ordinary and hybrid search with MMR on float and sparse
fields, offset paging continuity, and error cases.

## Open Questions

1. Return the MMR marginal score as an optional output field (for example
   `return_mmr_score: true` adding `$mmr_score`), as OpenSearch does with
   `mmr_score`?
2. Phase 2: MMR together with search `group_by` (diversify within the selected
   groups).
3. A separate threshold-based dedup option (for example `max_similarity`:
   drop a candidate whose similarity to a selected result exceeds the
   threshold). Unlike MMR it can return fewer than `limit` results, so it would
   be an explicit opt-in with that behavior documented.

## Rejected Alternatives

### Client-side MMR

Requires returning vectors, transfers `W` vectors per query to the client, and
must be reimplemented in every SDK. It also cannot run between server-side
fusion and requery.

### Keep upstream order and only remove redundant rows

Elasticsearch preserves the inner retriever order for surviving documents.
Re-sorting the selected set by score breaks paging: the set selected for
`offset + limit` is a superset of the previous page's set, and re-sorting it can
move rows across page boundaries. Selection order keeps pages consistent.

### Sliding-window offset (Weaviate)

Weaviate applies MMR independently to the relevance-ranked slice
`[offset, offset + limit)`, so `offset` moves the candidate window and must be
advanced by the window size rather than the page size. Each page costs the
same, but pages are diversified independently (page 2 can repeat content from
page 1), and the paging rule differs from every other Milvus search, which
Weaviate's own documentation warns about. OpenSearch passes `from` through to
the underlying k-NN query unchanged, which yields overlapping sliding windows
when clients advance `from` by the page size, so results can repeat. Milvus
uses a fixed window instead.

### Grow the window with offset (Qdrant)

Qdrant selects `offset + limit` points by MMR and then skips `offset`, with a
window that must be at least `offset + limit`. Growing the window with
`offset` changes the candidate set, the normalization bounds, and in hybrid
search the fused scores between pages, so page `n + 1` no longer continues
page `n`, and the cost of each page grows with depth. A fixed window keeps
every page consistent with a constant cost.

### Add a typed Arrow column per vector type

A typed `FixedSizeList<float32>` column would only cover dense float vectors.
Sparse vectors, binary vectors, and embedding lists need different layouts. An
opaque bytes column with type metadata covers all vector types with one import
and pick path, and MMR is the only consumer.

### Fetch vectors in the QueryNode search output

Possible for some vector types, but it transfers `W` vectors from every shard
and has not been validated end to end for all vector types. Requery fetches
exactly the `W` merged candidates and already supports every vector type.

## References

- Carbonell and Goldstein, "The Use of MMR, Diversity-Based Reranking for
  Reordering Documents and Producing Summaries," SIGIR 1998
- `docs/design-docs/design_docs/20260624-function-chain-api.md`
- `docs/design-docs/design_docs/20260818-hybrid-search-function-chain.md`
- `docs/design-docs/design_docs/20261010-function-chain-row-selection.md`
- Elasticsearch diversify retriever, OpenSearch MMR search, Qdrant MMR query
