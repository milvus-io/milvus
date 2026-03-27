## Summary

- After fixing the shim `expected<T>` default-construction contract, remote `internal/core` compilation advanced into `query` and `segcore`.
- The next blocking compatibility gaps are:
  - missing `knowhere/comp/materialized_view.h`
  - missing `knowhere::meta::BM25_K1`
  - missing `knowhere::meta::BM25_B`
  - missing `knowhere::meta::BM25_AVGDL`

## Impact

- Blocks `internal/core/src/query/PlanProto.cpp` on the materialized-view search metadata include.
- Blocks `internal/core/src/segcore/IndexConfigGenerator.cpp` and `internal/core/src/query/SearchBruteForce.cpp` on sparse/BM25 parameter propagation.
- Prevents the remote Milvus core build from moving past the late-stage query and segcore compile surface.

## Evidence

- Remote build error:
  - `fatal error: knowhere/comp/materialized_view.h: No such file or directory`
- Remote build errors in `IndexConfigGenerator.cpp`:
  - `error: ‘BM25_K1’ is not a member of ‘knowhere::meta’`
  - `error: ‘BM25_B’ is not a member of ‘knowhere::meta’`
  - `error: ‘BM25_AVGDL’ is not a member of ‘knowhere::meta’`
- Representative usage sites:
  - `internal/core/src/query/PlanProto.cpp`
  - `internal/core/src/query/SearchBruteForce.cpp`
  - `internal/core/src/segcore/IndexConfigGenerator.cpp`

## Resolution Direction

- Keep the fix inside the shim only.
- Mirror the minimal upstream knowhere public surface needed by Milvus:
  - `MaterializedViewSearchInfo`
  - `to_json(...)`
  - `from_json(...)`
  - BM25-related `knowhere::meta` keys
- Do not expand runtime scope beyond metadata transport for stage-1.
