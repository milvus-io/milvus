# 0006 HNSW Train Lifecycle

## Summary

- `knowhere-rs` HNSW FFI requires an explicit `knowhere_train_index(...)` call before `knowhere_add_index(...)`
- This applies even though HNSW training is logically lightweight and only marks the index as trained
- A shim that maps Milvus `Build(...)` to `create + add` without an explicit train call will fail with `CError::Internal`

## Evidence

- Direct `ctypes` repro against `libknowhere_rs` returned:
  - `create -> non-null`
  - `add -> 3` when called before training
- Direct `ctypes` repro returned:
  - `train -> 0`
  - `add -> 0`
  - `search -> non-null`
  when training is called first with the same HNSW config and vectors
- Rust-side HNSW implementation in `src/faiss/hnsw.rs` rejects `add(...)` when `trained == false`

## Stage 1 Policy

- The Milvus shim must treat `Train` as a real lifecycle step and forward it to `knowhere_train_index(...)`
- This issue does not require a `knowhere-rs` source change
