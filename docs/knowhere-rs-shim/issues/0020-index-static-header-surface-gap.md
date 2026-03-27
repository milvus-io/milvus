## Summary

- After filling the brute-force and dataset tensor-begin-id gaps, remote `internal/core` compilation advanced through `milvus_query` and much of `milvus_exec`.
- The next blocking compatibility gap is the missing `knowhere/index/index_static.h` header expected by segcore load/index helpers.

## Impact

- Blocks `internal/core/src/segcore/load_index_c.cpp`.
- Stops the remote core build late in the pipeline even though `milvus_query` is already compiling through the brute-force path.

## Evidence

- Remote build error:
  - `fatal error: knowhere/index/index_static.h: No such file or directory`
- Representative include site:
  - `internal/core/src/segcore/load_index_c.cpp:32`

## Resolution Direction

- Keep the fix inside the shim.
- Mirror the minimal `index_static` public surface Milvus expects, rather than introducing new runtime behavior.
- Re-run the remote incremental build after the shim header is added to identify the next compatibility gap.
