# Standalone Replaceability Matrix

## Status

This file is the unified tracker for the `knowhere-rs` standalone replaceability
effort.

It is not the final verdict yet. It is the current authoritative snapshot based
on remote standalone runs and remote artifact summaries.

Snapshot date: `2026-03-30`

## Scope

Full standalone subset from
[standalone_test_subset.json](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/scripts/knowhere-rs-shim/standalone_test_subset.json):

| Suite | Full subset cases |
| --- | ---: |
| `go_client` | 70 |
| `python_client` | 35 |
| `restful_client` | 9 |
| `restful_client_v2` | 28 |
| `total` | 142 |

Current active no-sparse target from remote manifest
`/data/work/milvus-rs-integ/artifacts/standalone_test_subset_nosparse_20260328-3.json`:

| Suite | Active no-sparse cases |
| --- | ---: |
| `go_client` | 57 |
| `python_client` | 35 |
| `restful_client` | 9 |
| `restful_client_v2` | 27 |
| `total` | 128 |

## Current Progress

| Suite | Full subset | Active target | Authoritative pass | Authoritative fail | Status | Evidence | Current blocker |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| `go_client` | 70 | 57 | 70 | 0 | `complete` incl. sparse-only lane | `/data/work/milvus-rs-integ/artifacts/go-client-subset-nosparse-20260328-5/summary.json`; `/data/work/milvus-rs-integ/artifacts/standalone-sparse-only-20260330-1/summary.json` | none |
| `python_client` | 35 | 35 | 35 | 0 | `complete` for current no-sparse lane | `/data/work/milvus-rs-integ/artifacts/python-client-nosparse-sweep-20260329-3/summary.json`; `/data/work/milvus-rs-integ/artifacts/python-client-nosparse-sweep-20260329-4/summary.json` is invalid harness noise (`/usr/bin/python3: No module named pytest`), not a product regression | no current no-sparse blocker |
| `restful_client` | 9 | 9 | 9 | 0 | `complete` for current no-sparse lane | `/data/work/milvus-rs-integ/artifacts/restful-probe-20260328-1/summary.json` and `/data/work/milvus-rs-integ/artifacts/restful-nosparse-sweep-20260328-3/summary.json` | sparse-inclusive lane is not relevant for this suite |
| `restful_client_v2` | 28 | 27 | 28 | 0 | `complete` incl. sparse-only lane | `/data/work/milvus-rs-integ/artifacts/restful-v2-probe-20260328-1/summary.json`, `/data/work/milvus-rs-integ/artifacts/restful-binary-metric-probe-20260328-1/summary.json`, `/data/work/milvus-rs-integ/artifacts/restful-range-search-probe-20260328-1/summary.json`, `/data/work/milvus-rs-integ/artifacts/restful-nosparse-sweep-20260328-3/summary.json`, and `/data/work/milvus-rs-integ/artifacts/standalone-sparse-only-20260330-1/summary.json` | none |

## Totals

Current authoritative snapshot:

- Active no-sparse cases exercised: `128 / 128`
- Active no-sparse passes so far: `128 / 128`
- Full manifest cases exercised: `142 / 142`
- Full manifest passes so far: `142 / 142`
- Full manifest remaining open: `0`

## Current Known Gaps

### `knowhere-rs` gaps

- [0002-abi-gap.md](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/docs/knowhere-rs-shim/issues/0002-abi-gap.md)
  - `HNSW` FFI still does not expose the full upstream config surface such as `M`
- [0021-knowhere-rs-hnsw-half-panic.md](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/docs/knowhere-rs-shim/issues/0021-knowhere-rs-hnsw-half-panic.md)
  - `Float16/BFloat16 + HNSW` can still panic in the Rust-owned path
- [0022-knowhere-rs-hnsw-large-topk-gap.md](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/docs/knowhere-rs-shim/issues/0022-knowhere-rs-hnsw-large-topk-gap.md)
  - large `topk` and iterator semantics still need shim-side raw-data fallback

## Latest Authoritative Milestones

- Remote no-sparse baseline is now fully green:
  - `go_client`: `57 / 57 PASS`
  - `python_client`: `35 / 35 PASS`
  - `restful_client`: `9 / 9 PASS`
  - `restful_client_v2`: `27 / 27 PASS`
- Remote `python_client` authoritative baseline is `/data/work/milvus-rs-integ/artifacts/python-client-nosparse-sweep-20260329-3/summary.json`
- `/data/work/milvus-rs-integ/artifacts/python-client-nosparse-sweep-20260329-4/summary.json` is invalid and should be ignored for verdicts because every case failed before execution with `/usr/bin/python3: No module named pytest`
- Sparse-only authoritative rerun is now fully green:
  - `/data/work/milvus-rs-integ/artifacts/standalone-sparse-only-20260330-1/summary.json` records `14 / 14 PASS`
  - `go_client`: `13 / 13 PASS`
  - `restful_client_v2`: `1 / 1 PASS`
- The previously failing sparse families are now covered by passing Milvus integration evidence:
  - grouped sparse go search/hybrid/load/range lane passed after rebuilding `libknowhere_rs.so` with the sparse FFI fixes
  - `restful_client_v2` `TestSearchVector::test_search_vector_with_sparse_float_vector_datatype` passed inside the sparse-only manifest rerun

## Next Checkpoint

1. Treat `142 / 142 PASS` as the new standalone replaceability baseline
2. Close [0024-knowhere-rs-sparse-inverted-ffi-gap.md](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/docs/knowhere-rs-shim/issues/0024-knowhere-rs-sparse-inverted-ffi-gap.md) as a resolved blocker with authority and Milvus integration evidence
3. Keep the sparse-only manifest artifact `/data/work/milvus-rs-integ/artifacts/standalone-sparse-only-20260330-1/summary.json` as the canonical sparse closure proof
