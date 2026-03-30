# 0022: knowhere-rs HNSW large-topk search/iterator gap

## Summary

When Milvus standalone uses the `knowhere-rs` HNSW implementation through the shim,
large-`topk` sealed-search and iterator flows can return materially fewer results than
requested, even when the collection contains enough vectors.

## Why This Is A knowhere-rs Issue

- The shim forwards `topk` to `knowhere_search` and copies back whatever
  `result->num_results` the Rust FFI returns; there is no shim-side truncation.
- A local shim smoke reproduces the short-result behavior directly against the Rust FFI
  HNSW path, outside Milvus standalone orchestration.
- The authoritative standalone failure is on official Milvus iterator coverage, but the
  signature is consistent with the same underlying HNSW search shortfall.

## Local Reproduction

Shim smoke:

- `internal/core/thirdparty/knowhere-rs-shim/tests/hnsw_large_topk_smoke.cpp`

Observed on 2026-03-27:

- dataset rows: `2000`
- requested `topk`: `1500`
- actual populated results: `99`
- failure signature:
  - `search populated 99 results, expected 1500`

## Remote Authoritative Evidence

Official standalone case:

- `go_client__search_iterator_test__TestSearchIteratorDefault`

Artifacts:

- `/data/work/milvus-rs-integ/artifacts/standalone-subset/go_client/go_client__search_iterator_test__TestSearchIteratorDefault/output.log`

Observed on 2026-03-27:

- expected iterator result count: `2000`
- actual iterator result count: `1000`
- failure surfaced from:
  - `tests/go_client/common/response_checker.go:315`

## Code Evidence

Relevant Rust path:

- `src/faiss/hnsw.rs`

Notable behaviors:

- `create_ann_iterator(...)` uses a search-then-wrap strategy instead of a distinct
  exhaustive iterator backend.
- `search(...)` only returns as many results as the HNSW search path actually produces.
- `search_single(...)` fills output from the candidate set returned by the graph search;
  if the candidate set is short, the API result is short too.

## Current Mitigation Direction

Do not change `knowhere-rs` in this Milvus integration track. For stage2 standalone
replaceability, the shim may need to preserve raw vectors and provide an exact local
fallback for large-`topk` sealed search / iterator semantics when the Rust HNSW result
set is short.

## Follow-up

- Reproduce directly in `knowhere-rs` with a minimal Rust HNSW large-`topk` test.
- Determine whether the root cause is graph traversal coverage, candidate-pool sizing,
  or another HNSW search-path limitation.
- Keep the Milvus shim workaround scoped to correctness/compatibility, not performance
  claims.
