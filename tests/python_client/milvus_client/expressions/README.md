# Expression Filtering Tests

This directory contains focused Milvus client expression filtering tests. The suite combines long-standing scalar/JSON baseline tests with newer coverage for regression issues, filtering matrix gaps, special scalar types, StructArray predicates, and index consistency.

For the detailed coverage table, see [coverage_matrix.md](coverage_matrix.md). Keep that matrix updated when adding or removing filtering coverage in this directory.

## Test Modules

| Module | Role | Main Coverage |
|---|---|---|
| `test_milvus_client_scalar_filtering.py` | Baseline scalar/array/JSON correctness | Scalar and ARRAY expression generation, Python eval oracle, scalar index consistency, LIKE escaping, INT64 overflow and 3VL corner cases |
| `test_milvus_client_json_filtering.py` | Baseline JSON UNKNOWN semantics | JSON missing/null/type-mismatch behavior, growing/sealed segment coverage, JSON path/flat index behavior, query/search consistency |
| `test_filter_regressions.py` | Issue-driven regression and boolean rewrite coverage | JSON mixed-type OR/IN regressions, expression order permutations, equivalence rewrites, fanout, segment-mode probes |
| `test_filtering_expression_matrix.py` | Compact deterministic expression matrix | Focused scalar, ARRAY, JSON, NULL, NOT, arithmetic, composition, and negative-message cases with explicit expected IDs |
| `test_filtering_index_consistency.py` | Focused index consistency and index negative cases | Shared plain/indexed fields, materialized scalar and JSON path indexes, boundary cases, meaningful index errors |
| `test_filtering_special_types.py` | Documented special filtering types | TIMESTAMPTZ, analyzer VARCHAR text/phrase match, GEOMETRY and RTREE consistency |
| `test_filter_expression_issue_mining.py` | GitHub expressions issue mining | Empty template params, bitwise controls, NULL literal rejection, INT64 overflow generalization |
| `test_filtering_additional_l2.py` | Additional L2 gaps | StructArray full filtering matrix, StructArray sub-field indexes, RANDOM_SAMPLE |
| `filtering_case_matrix.py` | Shared deterministic case data | Reusable row builders and case matrices for the focused filtering modules |
| `expression_test_utils.py` | Shared test helpers | Query ID helpers, minimal vector index helpers, segment-mode and index-readiness utilities |

## Baseline Versus Added Coverage

The two original baseline modules are intentionally kept:

- `test_milvus_client_scalar_filtering.py` is broad and oracle-driven. It uses deterministic boundary values plus generated expressions to validate scalar, ARRAY, JSON, and scalar-index behavior.
- `test_milvus_client_json_filtering.py` is the canonical suite for JSON UNKNOWN behavior. It uses fixed expected IDs across raw, indexed, growing, sealed, query, and search paths.

The newer modules do not replace those baselines. They add reviewable, deterministic nodes for coverage that was missing or too implicit:

- issue regressions and generalized mining cases;
- meaningful negative-path error messages;
- special scalar types such as TIMESTAMPTZ, text-enabled VARCHAR, and GEOMETRY;
- materialized index assertions for HNSW vector, scalar, JSON path, and StructArray sub-field indexes;
- StructArray filtering, MATCH-family predicates, NULL/empty semantics, and query/search/delete API behavior;
- compact mixed-expression stress cases without a cartesian explosion of collections.

Some basic scalar, ARRAY, and JSON expressions overlap semantically with the baseline modules. Those focused cases are kept as small matrix anchors with explicit IDs, while the baseline modules remain the broad oracle-based tests.

## Coverage Dimensions

The current suite covers:

- scalar types: `INT8`, `INT16`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BOOL`, `VARCHAR`;
- compound types: `ARRAY`, `JSON`, `ARRAY<STRUCT>`;
- special types: `TIMESTAMPTZ`, analyzer-enabled `VARCHAR`, `GEOMETRY`;
- operators: comparison, `IN`, `NOT IN`, `LIKE`, arithmetic, bitwise `&`/`|`/`^`, logical `AND`/`OR`/`NOT`, `IS NULL`, `IS NOT NULL`;
- functions: `array_contains*`, `array_length`, `json_contains*`, text/phrase match, geometry functions, StructArray `element_filter` and MATCH family, `RANDOM_SAMPLE`;
- indexes: no-index plus materialized `INVERTED`, `BITMAP`, `TRIE`, `STL_SORT`, `NGRAM`, `RTREE`, JSON path cast indexes, StructArray sub-field scalar indexes, and issue-specific `HNSW` fixtures;
- segment modes: sealed, growing, and mixed where relevant;
- API paths: query and delete for general filtering correctness, plus issue-specific search and hybrid-search regressions and controls.

Focused fixtures that claim real index coverage insert and flush 3000 rows, require `state=Finished` with `indexed_rows == total_rows == 3000` and zero pending rows, and assert index name, field, and type. Plain/indexed twin fields reuse the same logical values so the index is the only semantic difference. The mixed-segment fixture separately proves 3000 sealed indexed rows plus visible growing rows before checking exact query results.

See [coverage_matrix.md](coverage_matrix.md) for maintained coverage-family mappings to representative pytest nodes.

## Running

From `tests/python_client`, using an activated Python 3.12 virtual environment:

```bash
python -m pytest milvus_client/expressions --collect-only -q
```

Run the baseline scalar and JSON suites:

```bash
python -m pytest \
  milvus_client/expressions/test_milvus_client_scalar_filtering.py \
  milvus_client/expressions/test_milvus_client_json_filtering.py
```

Run the focused added suites:

```bash
python -m pytest -n 4 --dist loadgroup \
  milvus_client/expressions/test_filter_regressions.py \
  milvus_client/expressions/test_filtering_expression_matrix.py \
  milvus_client/expressions/test_filtering_index_consistency.py \
  milvus_client/expressions/test_filtering_special_types.py \
  milvus_client/expressions/test_filter_expression_issue_mining.py \
  milvus_client/expressions/test_filtering_additional_l2.py
```

Read-only focused suites use class-scoped shared collections where their schema and index requirements match. Segment lifecycle, destructive operations, and schema-isolation cases remain independent. When running with xdist, pass `--dist loadgroup` so tests with the same `xdist_group` stay on one worker and reuse their prepared collection.
Use pytest-tagging's `--tags L0`, `--tags L1`, or `--tags L2` when validating a single level.

For remote validation, pass the standard Python client options, for example:

```bash
python -m pytest -n 4 --dist loadgroup milvus_client/expressions/test_filtering_additional_l2.py \
  --host "${MILVUS_HOST}" --port "${MILVUS_PORT:-19530}" -q -s --tb=short --disable-warnings
```

## Maintenance Rules

- Add broad generated scalar/ARRAY expression behavior to `test_milvus_client_scalar_filtering.py`.
- Add JSON missing/null/type-mismatch UNKNOWN behavior to `test_milvus_client_json_filtering.py`.
- Add issue-specific regressions to `test_filter_regressions.py` or `test_filter_expression_issue_mining.py`.
- Add compact deterministic matrix gaps to `test_filtering_expression_matrix.py`.
- Add index consistency or index negative cases to `test_filtering_index_consistency.py`.
- Add TIMESTAMPTZ, text, and GEOMETRY cases to `test_filtering_special_types.py`.
- Add StructArray and RANDOM_SAMPLE gaps to `test_filtering_additional_l2.py`.
- Update [coverage_matrix.md](coverage_matrix.md) when changing meaningful coverage.
