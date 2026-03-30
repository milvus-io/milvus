# 0021: knowhere-rs HNSW half-vector panic

## Summary

When Milvus standalone builds `HNSW` indexes for `Float16Vector` or `BFloat16Vector`
through the `knowhere-rs` shim, the process can abort inside `knowhere-rs` with a
non-unwinding Rust panic.

## Why This Is A knowhere-rs Issue

- The crash originates inside `knowhere-rs`, not in Milvus core or the shim glue.
- The authoritative remote log shows Rust panics from `src/faiss/hnsw.rs`.
- The panic occurs during C FFI execution, so it aborts the hosting Milvus process.

## Remote Evidence

Authoritative log:

- `/data/work/milvus-rs-integ/milvus-var/logs/standalone-stage1.log`

Observed signatures on 2026-03-27:

- `thread '<unnamed>' ... panicked at src/faiss/hnsw.rs:2985:59`
- `thread '<unnamed>' ... panicked at src/faiss/hnsw.rs:3136:53`
- `panic in a function that cannot unwind`
- `thread caused non-unwinding panic. aborting.`
- `SIGABRT: abort`
- `signal arrived during cgo execution`

## Current Mitigation

Do not route `Float16Vector` or `BFloat16Vector` `HNSW` build/search traffic into
the Rust HNSW implementation from the shim. Keep stage2 moving by handling these
types in the shim compatibility layer instead.

## Follow-up

- Reproduce directly in `knowhere-rs` with a minimal half-vector HNSW harness.
- Confirm whether the root cause is unsupported dtype, bad size assumptions, or
  invalid reinterpretation of half-vector buffers.
