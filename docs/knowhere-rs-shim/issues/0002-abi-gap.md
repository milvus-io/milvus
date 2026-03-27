# 0002 ABI Gap

## Known Surface Mismatch

- Milvus expects Knowhere C++ headers and object model
- `knowhere-rs` currently exposes a Rust-owned C ABI
- The shim must cover the minimum C++ compatibility surface for:
  - `Version`
  - `DataSet`
  - `IndexNode`
  - `IndexFactory`
  - `BinarySet`
  - config helpers / constants

## Newly Confirmed Gaps

- HNSW `M` is not exposed in `CIndexConfig`
  - Current Rust FFI `CIndexType::Hnsw` only accepts `dim`, `metric_type`, `ef_construction`, and `ef_search`
  - Rust HNSW currently defaults `m` to `32` when not provided
  - Milvus HNSW build params normally include `M`, so stage 1 shim must either accept the Rust default or document explicit parameter downgrade
- HNSW per-search `ef` is not exposed as a first-class C ABI argument
  - `knowhere_search` / `knowhere_search_with_bitset` accept `top_k` but not a search config object
  - Rust HNSW search currently runs through `SearchRequest` built inside the FFI layer, so the shim cannot faithfully propagate Milvus search-time `ef` without extra ABI support
- No public C header exists for the full index ABI
  - `knowhere-rs` ships Rust exports in `src/ffi.rs`, but the repository does not provide a stable installed C header for the index functions
  - Stage 1 shim must define its own bridge declarations against the existing exported symbol set

## Stage 1 Policy

- Record every newly discovered ABI mismatch here before widening the adapter surface
- Prefer a thin translation layer over changes to `knowhere-rs`
