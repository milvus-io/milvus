## Summary

- After the brute-force facade landed, remote query compilation advanced further into `SearchBruteForce.cpp`.
- The next blocking gap is that the shim `knowhere::DataSet` does not expose the `tensor begin id` metadata accessors expected by Milvus and upstream knowhere.

## Impact

- Blocks `internal/core/src/query/SearchBruteForce.cpp`.
- Prevents brute-force query preparation from tagging base datasets with embedding-list starting offsets.

## Evidence

- Remote build error:
  - `error: ... knowhere::DataSet ... has no member named 'SetTensorBeginId'`
- Upstream/reference knowhere also carries the matching metadata key:
  - `knowhere::meta::INPUT_BEG_ID`
- Representative usage sites:
  - `internal/core/src/query/SearchBruteForce.cpp`
  - `knowhere` brute-force helpers read the same field through `GetTensorBeginId()`

## Resolution Direction

- Keep the fix inside the shim.
- Add the minimal `DataSet::{SetTensorBeginId, GetTensorBeginId}` pair and the `meta::INPUT_BEG_ID` key.
- Preserve existing stage-1 behavior: metadata transport only, no new algorithmic work.
