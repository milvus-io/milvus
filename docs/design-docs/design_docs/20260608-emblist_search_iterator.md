# MEP: search_iterator() over emb_list (ArrayOfVector), hybrid, and sparse

Tracking discussion: milvus-io/milvus#49906

## Summary (required)

Milvus v2.6 added `emb_list` / `ArrayOfVector` fields with `MAX_SIM` scoring (#42148,
#43726), enabling ColBERT-style late-interaction retrieval where one row (a document or
chunk) carries an array of vectors. `search()` over `emb_list` works; `search_iterator()`
does **not** — nor does it work for hybrid search or stream over sparse vectors. This MEP
extends Milvus's existing stateless **Iterator v2** path (plus a knowhere change and
client-side RRF fusion in the SDKs) to close that gap, so deep chunk-level retrieval runs
in **bounded memory**.

## Motivation (required)

Late-interaction scoring is `MAX_SIM(Q, row) = Σ_i max_{p ∈ row} sim(q_i, p)` — a
sum-of-max over a *list* of query vectors against a *list* of stored vectors. Three gaps
compound to block iteration today:

1. **No iterator for `emb_list`.** `MAX_SIM` is a sum-of-max row score, not a
   single-vector distance, so there is no monotonic radius/`last_bound` cursor of the
   kind the iterator path filters on for ordinary dense vectors.
2. **No iterator for hybrid search.** Reciprocal Rank Fusion needs each modality's
   global rank; a scalar `last_bound` cursor cannot carry it.
3. **No streaming iterator for sparse.** Today's sparse `AnnIterator` materializes all
   distances on the first `Next()` (a full index scan), defeating bounded-memory deep
   retrieval.

Consequence: retrieving beyond the per-search `topK` ceiling forces operators to raise
`topKLimit` / `maxQueryResultWindow` and run a full deep search at once; peak memory
then scales with depth × concurrency. A streaming iterator runs deep retrieval in
bounded memory and removes that ceiling.

## Public Interfaces (optional)

- `search_iterator()` / `searchIterator` accept an `emb_list` query (one query's
  multiple vectors, passed as an `EmbeddingList`) over an `emb_list` field.
- A client-side **hybrid** `search_iterator` / `hybridSearchIterator` that fuses
  dense + emb_list (or sparse) modalities via RRF over the streamed per-modality
  results. No new server proto/RPC.

## Design Details (required)

The design extends the existing **stateless Iterator v2** rather than building a
parallel stateful iterator. Each `next()` is an ordinary `Search` RPC carrying a
`last_bound` cursor; the proxy holds no per-iterator state and query nodes retain
nothing between batches, so server-side working-set memory is flat in the total result
depth `L`. (Cost is super-linear in `L` — each batch rebuilds the iterator and
re-traverses seen results — a known v1 limitation; see *Compatibility* and the v2 note.)

**knowhere**
- *`AnnIterator` for the `emb_list` HNSW index.* The index is an HNSW graph over the
  concatenated paragraph vectors of all rows in a segment plus an offset map (which
  vectors belong to which row). The existing `AnnIterator` returns `m` per-query-vector
  streaming HNSW sub-iterators; the new emblist iterator is a thin **grouping layer**
  over them — it advances the sub-iterators, resolves each touched paragraph to its row,
  computes the **exact** `MAX_SIM` for a row on first sighting (its paragraphs are
  contiguous and few), and emits row-level `(id, max_sim)` in approximately-descending
  order under a soft upper bound. It composes HNSW traversal, not reimplements it.
- *Streaming bounded-WAND iterator for sparse / BM25.* Each `Next(batch)` runs a
  WAND/MaxScore top-`batch` retrieval bounded above by the previous batch's minimum
  `(score, id)` cursor (Zipfian BM25 postings keep WAND pruning effective).

**milvus core / segcore** — `CachedSearchIterator` already wraps a knowhere
`AnnIterator`. Changes: treat `nq` for an emblist query as the number of query
emb_lists; handle row-level `(id, max_sim)` results; lift the iterator path's
rejection of `ArrayOfVector`. The brute-force / growing-segment iterator paths (which
have no emblist `AnnIterator` yet) return a clean, typed `Unsupported` error rather than
an internal assertion. No new CGO entry points, no iterator registry, no new proto.

**milvus proxy** — the Iterator v2 proxy path is already stateless; it only stops
rejecting `ArrayOfVector` for the iterator.

**SDKs** — hybrid iteration is **client-side**: the SDK drives two stateless
single-modality `search_iterator`s and fuses their score-descending streams via RRF
(NRA threshold algorithm), pinning one MVCC snapshot for the whole hybrid iteration.
Because the SDK sees both streams it sees each modality's global rank, which a
server-side scalar cursor cannot. The in-flight (seen-but-not-emitted) map is bounded
by stream skew.

## Compatibility, Deprecation, and Migration Plan (optional)

Purely additive — no change to existing `search()` / `search_iterator()` over ordinary
vectors, no proto/RPC changes, no schema migration. Existing `emb_list` `search()` is
unaffected. **v1 scope** covers the **sealed vector-index path** with
**restart-on-error** failure handling (a failed batch is an ordinary `Search` failure;
the caller retries the batch / restarts from the last `last_bound` — there is no
server-side session to lose). **v2** (deferred): a resumable retained-cursor iterator
making deep retrieval linear-cost, transparent mid-iteration re-seed, and emblist
iteration over brute-force / growing-segment paths (blocked on knowhere
`BruteForce::AnnIterator` gaining emblist support).

## Test Plan (required)

Validated end-to-end on a v2.6.18 cluster against an exact brute-force `MAX_SIM` oracle,
on real pre-embedded Wikipedia data (1024-d, paragraphs grouped into articles):

- **Set recall** (the sub-iterator-grouping concern from #49906): on 150k articles /
  ~828k paragraphs, recall@100 = 1.000, recall@1000 = 0.997, ordering Spearman ρ = 1.000
  (top-100). Held at 10× scale (15k → 150k) — recall@L is bounded by the HNSW index's
  own ef-limited recall, not the grouping. An **adversarial** set built to trigger the
  exact failure mode (high-`MAX_SIM` rows whose chunks are buried behind decoys in every
  sub-iterator): 50/50 surfaced in the exact oracle top-50.
- **No duplicates / score-exactness / deeper L**: verified by full-iteration comparison
  to the oracle.
- **Bounded memory**: iterating 40k hits, client RSS stayed flat (+0.7 MB) and QueryNode
  RSS stayed bounded (no growth with depth).
- **Concurrency**: 32 concurrent deep iterators completed, zero OOMKilled, QueryNode
  memory bounded.
- **Unit/regression**: knowhere UT suite; milvus segcore C++ + proxy Go tests; pymilvus
  full suite (zero regressions); milvus-sdk-node spec — all green.

## Rejected Alternatives (optional)

- **A stateful, server-side iterator with a coordinating delegator / session.** Rejected:
  it would add a per-iterator session (placement, TTL, memory, churn-recovery) and is
  fragile under node churn. The stateless Iterator-v2 model needs none of that — the
  cursor lives in the client — and global hybrid rank is recovered by fusing client-side.
- **Server-side hybrid fusion.** Rejected: a server scalar cursor cannot carry each
  modality's global rank; client-side RRF over the two streams can.

## References (optional)

- Tracking issue: milvus-io/milvus#49906
- emb_list / ArrayOfVector + MAX_SIM: #42148, #43726
- ColBERT / late interaction: Khattab & Zaharia (SIGIR'20); ColBERTv2 (NAACL'22)
- NRA / threshold algorithm: Fagin, Lotem, Naor (2003)
