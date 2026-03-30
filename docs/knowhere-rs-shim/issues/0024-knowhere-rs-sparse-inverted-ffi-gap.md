# 0024: SparseInverted/SparseWand FFI sparse-lane gap in knowhere-rs

## Status

Closed on `2026-03-30`.

This issue originally tracked a real `knowhere-rs` blocker in the sparse
standalone lane:

- `SparseInverted` existed in the C ABI enum but did not have a usable FFI
  constructor path
- sparse search routing in the C ABI assumed `query_dim == index_dim`
- sparse bitset filtering used `doc_id` directly instead of the stored row
  ordinal

Those gaps were enough to keep Milvus standalone sparse build/search lanes red
even after the higher-level shim wiring was added.

## What Was Fixed

The `knowhere-rs` sparse FFI now has the missing capability and the missing
runtime semantics:

- `SparseInverted` is wired through the FFI create/type/capability/search/save
  and iterator paths
- sparse multi-query search now chunks by the query-side sparse dimension passed
  through the C ABI instead of forcing the index-side dimension
- sparse bitset filtering now maps `doc_id -> row ordinal` before checking the
  bitset

These fixes were implemented on branch `fix/sparse-inverted-ffi` in
`knowhere-rs` and then validated both on the remote authority machine and
through Milvus standalone integration.

## Closure Evidence

### knowhere-rs authority evidence

- local targeted regressions passed:
  - `cargo test --lib test_ffi_sparse_inverted_search_accepts_query_dim_larger_than_index_dim -- --nocapture`
  - `cargo test --lib test_search_with_bitset_sparse_inverted_accepts_query_dim_larger_than_index_dim -- --nocapture`
  - `cargo test --lib sparse_inverted -- --nocapture`
  - `cargo test --lib sparse_wand -- --nocapture`
- remote authority passed:
  - `bash scripts/remote/test.sh --command "cargo test --lib sparse_wand -- --nocapture"`
    - `run_id=20260330T004200Z_45499`
  - `bash scripts/remote/test.sh --command "cargo test --lib sparse_inverted -- --nocapture"`
    - `run_id=20260330T004221Z_45730`

### Milvus integration evidence

- rebuilt Milvus-side `libknowhere_rs.so` on `hannsdb-x86`:
  - `/data/work/milvus-rs-integ/knowhere-rs-target/release/libknowhere_rs.so`
  - rebuilt at `2026-03-30 08:44:47 +08:00`
- reran the previously failing grouped sparse go lane:
  - `TestHybridSearchSparseVector`
  - `TestLoadCollectionSparse`
  - `TestSearchEmptySparseCollection`
  - `TestSearchOutputSparse`
  - `TestSearchSparseVector`
  - `TestSearchWithEmptySparseVector`
  - `TestSearchFromEmptySparseVector`
  - `TestSearchSparseVectorPagination`
  - `TestSearchSparseVectorNotSupported`
  - `TestRangeSearchSparseVector`
  - result: `PASS`
- reran the dedicated sparse-only standalone manifest:
  - artifact: `/data/work/milvus-rs-integ/artifacts/standalone-sparse-only-20260330-1/summary.json`
  - result: `14 / 14 PASS`
  - suite totals:
    - `go_client`: `13 / 13 PASS`
    - `restful_client_v2`: `1 / 1 PASS`

## Impact

- the previous sparse-only full-manifest blocker is resolved
- Milvus standalone replaceability moved from `128 / 142 PASS` to
  `142 / 142 PASS`
- this issue should no longer be used as the active blocker for sparse
  standalone coverage
