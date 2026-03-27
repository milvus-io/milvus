# Standalone Test Subset

## Purpose

This document defines the Milvus-original standalone test subset used for the `knowhere-rs` replaceability effort.

The subset is not a generic standalone regression suite.
It is the set of original Milvus standalone-capable cases whose core assertion materially depends on Knowhere-shaped vector index or vector search behavior.

The machine-readable source of truth is:

- `scripts/knowhere-rs-shim/standalone_test_subset.json`

## Selection Rules

A case is included only if:

- it runs against standalone instead of `tests/integration`
- it comes from an original Milvus test suite
- its core assertion depends on vector index build, vector search, range search, search iterator, index load/persistence, or growing/brute-force fallback behavior

A case is excluded if it is primarily about:

- auth / rbac / user / role
- database / alias / partition management without vector-index dependence
- HTTP routing
- geometry
- full-text / embedding / reranker services
- cluster / k8s / distributed / MiniCluster orchestration
- pure query, get, or delete semantics that do not materially exercise Knowhere

## Included Suites

The current subset draws cases from:

- `tests/go_client`
- `tests/python_client`
- `tests/restful_client`
- `tests/restful_client_v2`

Current inventory size:

- `go_client`: 7 file groups / 70 cases
- `python_client`: 7 file groups / 35 cases
- `restful_client`: 2 file groups / 9 cases
- `restful_client_v2`: 4 file groups / 28 cases
- total: 20 file groups / 142 cases

Excluded standalone-capable suites or files remain outside the subset until they can be shown to materially depend on Knowhere rather than generic server behavior.

## Included File Groups

### `go_client`

- [collection_test.go](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/go_client/testcases/collection_test.go)
  - selected for inline HNSW collection creation and immediate vector search
- [groupby_search_test.go](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/go_client/testcases/groupby_search_test.go)
  - selected for float, binary, growing, iterator, range, and hybrid group-by search
- [hybrid_search_test.go](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/go_client/testcases/hybrid_search_test.go)
  - selected for multivector, sparse, pagination, and group-by hybrid search
- [index_test.go](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/go_client/testcases/index_test.go)
  - selected for dedicated vector-index build, describe, and parameter-validation coverage
- [load_release_test.go](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/go_client/testcases/load_release_test.go)
  - selected only where load/release behavior depends on vector-index presence
- [search_iterator_test.go](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/go_client/testcases/search_iterator_test.go)
  - selected for iterator and growing search behavior
- [search_test.go](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/go_client/testcases/search_test.go)
  - selected for the main standalone vector-search surface

### `python_client`

- [test_hnsw.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/python_client/testcases/indexes/test_hnsw.py)
- [test_hnsw_sq.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/python_client/testcases/indexes/test_hnsw_sq.py)
- [test_diskann.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/python_client/testcases/indexes/test_diskann.py)
- [test_ivf_rabitq.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/python_client/testcases/indexes/test_ivf_rabitq.py)
  - dedicated vector-index family coverage
- [test_index.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/python_client/testcases/test_index.py)
  - only vector-index creation, search, load, and validation cases
- [test_e2e.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/python_client/testcases/test_e2e.py)
  - default standalone vector e2e case
- [test_high_level_api.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/python_client/testcases/test_high_level_api.py)
  - only high-level API cases whose result still depends on vector index/search semantics

### `restful_client`

- [test_restful_sdk_mix_use_scenario.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/restful_client/testcases/test_restful_sdk_mix_use_scenario.py)
  - only index, load, and vector-search mix-use cases
- [test_vector_operations.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/restful_client/testcases/test_vector_operations.py)
  - only vector-search cases with actual ANN assertions

### `restful_client_v2`

- [test_collection_operations.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/restful_client_v2/testcases/test_collection_operations.py)
  - only collection creation and load cases that embed vector-index semantics
- [test_index_operation.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/restful_client_v2/testcases/test_index_operation.py)
  - dedicated vector-index coverage
- [test_restful_sdk_mix_use_scenario.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/restful_client_v2/testcases/test_restful_sdk_mix_use_scenario.py)
  - cross-surface index/load/search checks
- [test_vector_operations.py](/Users/ryan/.config/superpowers/worktrees/milvus/knowhere-rs-shim-stage1/tests/restful_client_v2/testcases/test_vector_operations.py)
  - vector, sparse, binary, range, hybrid, advanced, and ignore-growing search coverage

## Explicit Exclusions

The current subset intentionally excludes:

- all of `tests/integration`
- `tests/go_client` files focused on scalar-only query, database management, text, geometry, or non-vector admin flows
- `tests/python_client` files focused on bulk insert, connection, utility, full-text, resource groups, and non-vector management paths
- `tests/python_client/async_milvus_client/*` for now
  - reason: these wrap the same product surface but add no distinct Knowhere semantics until the synchronous subset is stable
- `tests/restful_client(_v2)` cases whose primary assertion is insert, delete, query-only, auth, or collection metadata unrelated to vector index/search behavior

## Coverage Intent

The subset is intentionally broad across vector behavior, not only `HNSW`.

It includes official standalone coverage for:

- float vector search
- binary vector search
- sparse vector search
- hybrid search
- search iterator
- range search
- group-by search
- load/release behavior that depends on vector indexes
- multiple index families requested by original tests

This is the correct scope for answering whether `knowhere-rs` can replace Knowhere for the standalone product surface that actually uses it.

## Status

This inventory is the current authoritative subset for stage 2.

Future changes should:

- add or remove cases only with a written reason
- keep the JSON manifest synchronized
- preserve the rule that only true `knowhere-rs` capability gaps become `knowhere-rs` issues
