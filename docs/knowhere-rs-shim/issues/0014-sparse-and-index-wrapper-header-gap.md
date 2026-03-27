## Summary

- After the earlier shim header gaps were closed, remote `internal/core` compilation advanced into `common`, `query`, `storage`, and `index`.
- The next compatibility-surface failures are two more knowhere headers Milvus includes broadly:
  - `knowhere/sparse_utils.h`
  - `knowhere/index/index_node.h`

## Impact

- Blocks `milvus_common`, `milvus_query`, `milvus_storage`, and `milvus_index` from compiling.
- Confirms the stage-1 integration gap is still in the compatibility shim header surface, not in `knowhere-rs` itself.

## Evidence

- Remote build fails with:
  - `fatal error: knowhere/sparse_utils.h: No such file or directory`
  - `fatal error: knowhere/index/index_node.h: No such file or directory`
- Inclusion sites include:
  - `internal/core/src/common/Utils.h`
  - `internal/core/src/common/Chunk.h`
  - `internal/core/src/common/QueryResult.h`
  - `internal/core/src/query/SearchBruteForce.cpp`
  - `internal/core/src/query/SubSearchResult.h`
- Milvus uses sparse-row compile-time APIs such as:
  - `knowhere::sparse::SparseRow<T>::element_size()`
  - `data()`
  - `size()`
  - `data_byte_size()`
  - `dim()`
  - element access via `operator[]`

## Resolution Direction

- Add the minimum sparse-row model needed for compilation to the shim.
- Add an `index/index_node.h` wrapper that forwards to the existing top-level shim `index_node.h`.
- Keep the sparse implementation minimal and header-only:
  - enough for Milvus common/query/storage code to compile
  - no attempt to implement full sparse search semantics in stage 1
