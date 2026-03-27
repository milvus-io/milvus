## Summary

- After filling the materialized-view and BM25 metadata surface, remote `internal/core` compilation advanced into `query/SearchBruteForce.cpp`.
- The next blocking gap is the missing `knowhere/comp/brute_force.h` surface expected by Milvus query execution.

## Impact

- Blocks `internal/core/src/query/SearchBruteForce.cpp`.
- Prevents the query target from compiling, which is closer to the stage-1 standalone search path than the earlier pure metadata blockers.

## Evidence

- Remote build error:
  - `fatal error: knowhere/comp/brute_force.h: No such file or directory`
- Current Milvus call sites require the following public surface:
  - `BruteForce::SearchWithBuf<T>(...)`
  - `BruteForce::RangeSearch<T>(...)`
  - `BruteForce::SearchSparseWithBuf(...)`
  - `BruteForce::AnnIterator<T>(...)`
- `SearchBruteForce.cpp` also assumes sparse helper aliases such as `knowhere::sparse::label_t`.

## Resolution Direction

- Keep the fix inside the shim.
- Add a minimal header-only `BruteForce` facade that matches the required signatures and returns `Status::not_implemented` / error `expected` values.
- Add any missing sparse aliases needed purely to satisfy compile-time contracts.
- Do not add real brute-force kernels in stage-1 unless a later runtime validation proves they are required.
