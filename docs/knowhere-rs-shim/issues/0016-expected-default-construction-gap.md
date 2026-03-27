## Summary

- Remote `internal/core` compilation advanced past the first two shim API-surface rounds and then stopped in `IndexFactory.cpp`.
- The next blocker is that the shim's `knowhere::expected<T>` does not match knowhere's default-construction behavior for value types such as `knowhere::Resource`.

## Impact

- Blocks `internal/core/src/index/IndexFactory.cpp` in both `VecIndexLoadResource(...)` and `ScalarIndexLoadResource(...)`.
- Prevents the remote incremental build from advancing into the next compatibility gap, even though the shim's own Linux compile smoke and HNSW roundtrip smoke already pass.

## Evidence

- Remote build error:
  - `error: no matching function for call to 'knowhere::expected<knowhere::Resource>::expected()'`
- A local compile-surface RED reproduces the same contract gap with:
  - `knowhere::expected<knowhere::Resource> resource_expected;`
- Current usage site in Milvus:
  - `knowhere::expected<knowhere::Resource> resource;`
  - then later reassigned from `IndexStaticFaced<...>::EstimateLoadResource(...)`

## Resolution Direction

- Keep the fix inside the shim.
- Align `knowhere::expected<T>` more closely with upstream knowhere by allowing value-default construction through the templated value constructor path, rather than special-casing `Resource`.
- Preserve stage-1 scope: no `knowhere-rs` changes and no Milvus core logic changes.
